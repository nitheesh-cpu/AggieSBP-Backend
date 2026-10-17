"""External AI clients for professor-review summarization.

TAMU AI Chat generates one structured summary per professor. TypeSafe System
One optionally checks that each generated claim is supported by its cited
reviews. Both integrations are optional so the local pipeline remains usable
without credentials.
"""

from __future__ import annotations

import json
import logging
import os
import re
import threading
import time
from dataclasses import dataclass, field
from types import SimpleNamespace
from collections import defaultdict
from typing import Any, Dict, List, Mapping, Optional

import requests
from pydantic import ValidationError

from pipelines.professors.hierarchical_summarization.professor_request import (
    SYSTEM_PROMPT as PROFESSOR_SYSTEM_PROMPT,
)
from pipelines.professors.hierarchical_summarization.response_schema import (
    ProfessorSummaryOutput,
    professor_summary_response_format,
)

logger = logging.getLogger(__name__)

from pipelines.professors.hierarchical_summarization.config import (
    TYPESAFE_RISK_THRESHOLD,
    TYPESAFE_SUPPORT_THRESHOLD,
    TYPESAFE_TOPIC_THRESHOLD,
)
from pipelines.professors.schemas import ProcessedReview, SummaryClaim

try:
    from typesafe_sdk import Choice, Noul, Score, TypeSafeClient
except ImportError:  # pragma: no cover - dependency may be absent in local setups
    Choice = None  # type: ignore[assignment]
    Noul = None  # type: ignore[assignment]
    Score = None  # type: ignore[assignment]
    TypeSafeClient = None  # type: ignore[assignment]


class TamuAIError(RuntimeError):
    """Base error for TAMU AI Chat requests."""


class TamuAIQuotaExceeded(TamuAIError):
    """The TAMU allowance or configured per-run budget was exhausted."""


class TamuAIInvalidJSON(TamuAIError):
    """TAMU returned text that could not be parsed as a complete JSON object."""


class TamuAITruncated(TamuAIInvalidJSON):
    """TAMU stopped at its output-token limit before finishing the JSON."""


class TamuAITransientError(TamuAIError):
    """A temporary server failure eligible for a bounded retry."""


@dataclass(frozen=True)
class ClusterJudgment:
    """TypeSafe decisions consumed by deterministic pipeline code."""

    topic: str
    topic_confidence: float
    sentiment: str
    sentiment_confidence: float
    topic_probabilities: Mapping[str, float] = field(default_factory=dict)
    disagreement_probability: float = 0.0
    importance: float = 0.0
    importance_confidence: float = 0.0

    def relevant_topics(self, threshold: float) -> List[str]:
        """Return meaningful topics while always retaining the primary topic."""
        topics = [
            topic
            for topic, probability in self.topic_probabilities.items()
            if probability >= threshold
        ]
        if self.topic not in topics:
            topics.insert(0, self.topic)
        return topics


@dataclass(frozen=True)
class ClaimVerification:
    """Independent Jev probabilities used by deterministic acceptance policy."""

    support_probability: float
    overgeneralization_probability: float
    disagreement_erasure_probability: float
    topic: str
    topic_confidence: float
    sentiment: str
    sentiment_confidence: float
    importance: float
    importance_confidence: float


class TamuAIChatClient:
    """Small OpenAI-compatible client for TAMU AI Chat."""

    def __init__(
        self,
        *,
        api_key: str,
        base_url: str,
        model: str,
        timeout_seconds: float = 90.0,
        max_input_chars: int = 45_000,
        max_output_tokens: int = 2_400,
        max_requests: int = 0,
        session: Optional[requests.Session] = None,
        max_retries: int = 2,
        response_format: str = "json_schema",
    ) -> None:
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.timeout_seconds = timeout_seconds
        self.max_input_chars = max_input_chars
        self.max_output_tokens = max_output_tokens
        self.max_requests = max_requests
        self.requests_made = 0
        self.max_retries = max(0, max_retries)
        self._budget_lock = threading.Lock()
        self._thread_sessions = threading.local()
        self._injected_session = session
        self._quota_exhausted = False
        self.retries_made = 0
        self.json_retries = 0
        if response_format not in {"json_schema", "json_object"}:
            raise ValueError(
                "TAMU_AI_RESPONSE_FORMAT must be json_schema or json_object"
            )
        self.response_format = response_format

    @classmethod
    def from_env(cls) -> Optional["TamuAIChatClient"]:
        api_key = os.getenv("TAMU_AI_API_KEY", "").strip()
        base_url = os.getenv(
            "TAMU_AI_BASE_URL", "https://chat-api.tamu.ai/openai"
        ).strip()
        model = os.getenv("TAMU_AI_MODEL", "").strip()
        if not (api_key and base_url and model):
            return None
        return cls(
            api_key=api_key,
            base_url=base_url,
            model=model,
            timeout_seconds=float(os.getenv("TAMU_AI_TIMEOUT_SECONDS", "90")),
            max_input_chars=int(os.getenv("TAMU_AI_MAX_INPUT_CHARS", "45000")),
            max_output_tokens=int(os.getenv("TAMU_AI_MAX_OUTPUT_TOKENS", "2400")),
            max_requests=int(os.getenv("TAMU_AI_MAX_REQUESTS_PER_RUN", "0")),
            max_retries=int(os.getenv("TAMU_AI_MAX_RETRIES", "2")),
            response_format=os.getenv("TAMU_AI_RESPONSE_FORMAT", "json_schema"),
        )

    @property
    def endpoint(self) -> str:
        if self.base_url.endswith("/chat/completions"):
            return self.base_url
        return f"{self.base_url}/chat/completions"

    def _consume_request_budget(self) -> None:
        with self._budget_lock:
            if self._quota_exhausted:
                raise TamuAIQuotaExceeded("TAMU AI allowance or rate limit was reached")
            if self.max_requests and self.requests_made >= self.max_requests:
                raise TamuAIQuotaExceeded(
                    "Configured TAMU_AI_MAX_REQUESTS_PER_RUN limit reached"
                )
            self.requests_made += 1

    def _request_session(self) -> requests.Session:
        if self._injected_session is not None:
            return self._injected_session
        if not hasattr(self._thread_sessions, "session"):
            self._thread_sessions.session = requests.Session()
        return self._thread_sessions.session

    def take_thread_requests(self) -> int:
        """Requests this worker thread sent since the last call, then reset.

        Workers overlap, so global counter deltas cannot be attributed to one
        professor; each request part runs on a single thread.
        """
        count = getattr(self._thread_sessions, "requests", 0)
        self._thread_sessions.requests = 0
        return count

    def close_thread_session(self) -> None:
        """Release worker-owned connections when a course's requests finish."""
        session = getattr(self._thread_sessions, "session", None)
        if session is not None:
            session.close()
            del self._thread_sessions.session

    @staticmethod
    def _extract_json(text: str) -> Dict[str, Any]:
        cleaned = text.strip()
        if cleaned.startswith("```"):
            cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned, flags=re.IGNORECASE)
            cleaned = re.sub(r"\s*```$", "", cleaned)
        try:
            value = json.loads(cleaned)
        except json.JSONDecodeError:
            start = cleaned.find("{")
            end = cleaned.rfind("}")
            if start < 0 or end <= start:
                raise TamuAIInvalidJSON("TAMU AI Chat returned no JSON object")
            try:
                value = json.loads(cleaned[start : end + 1])
            except json.JSONDecodeError as exc:
                raise TamuAIInvalidJSON(
                    "TAMU AI Chat returned invalid or truncated JSON"
                ) from exc
        if not isinstance(value, dict):
            raise TamuAIInvalidJSON("TAMU AI Chat response must be a JSON object")
        return value

    @staticmethod
    def _message_text(response_body: Mapping[str, Any]) -> str:
        try:
            content = response_body["choices"][0]["message"]["content"]
        except (KeyError, IndexError, TypeError) as exc:
            raise TamuAIError("TAMU AI Chat response has an unexpected shape") from exc
        if isinstance(content, str):
            return content
        if isinstance(content, list):
            return "".join(
                item.get("text", "")
                for item in content
                if isinstance(item, dict) and item.get("type") in {None, "text"}
            )
        raise TamuAIError("TAMU AI Chat response contains no text")

    @staticmethod
    def _raise_provider_error(body: Mapping[str, Any]) -> None:
        """TAMU can return provider errors with HTTP 200, including in SSE."""
        error = body.get("error")
        if not error:
            return
        if isinstance(error, Mapping):
            kind = str(error.get("type", "")).lower()
            code = str(error.get("code", "")).lower()
            message = str(error.get("message", ""))
        else:
            kind, code, message = "", "", str(error)
        message_lower = message.lower()
        if (
            kind
            in {
                "budget_exceeded",
                "insufficient_quota",
                "quota_exceeded",
                "rate_limit_exceeded",
            }
            or code
            in {
                "budget_exceeded",
                "insufficient_quota",
                "quota_exceeded",
                "rate_limit_exceeded",
                "429",
            }
            or "budget has been exceeded" in message_lower
            or "budget exceeded" in message_lower
            or "quota exceeded" in message_lower
        ):
            raise TamuAIQuotaExceeded(
                "TAMU API budget/quota or rate limit exhausted; resume after the allowance is available"
            )
        if code in {"500", "502", "503", "504"} or kind in {
            "server_error",
            "internal_server_error",
            "service_unavailable",
        }:
            raise TamuAITransientError("TAMU returned a provider server-error envelope")
        # Do not dump provider messages: they may echo request data.
        raise TamuAIError("TAMU returned a provider error envelope (not a completion)")

    @staticmethod
    def _response_body(response: requests.Response) -> Mapping[str, Any]:
        """Parse either regular OpenAI JSON or TAMU's SSE fallback response."""
        try:
            body = response.json()
            if isinstance(body, Mapping):
                TamuAIChatClient._raise_provider_error(body)
                return body
        except ValueError:
            pass

        content_parts: List[str] = []
        finish_reason = None
        for raw_line in response.text.splitlines():
            line = raw_line.strip()
            if not line.startswith("data:"):
                continue
            payload = line[5:].strip()
            if not payload or payload == "[DONE]":
                continue
            try:
                chunk = json.loads(payload)
                if isinstance(chunk, Mapping):
                    TamuAIChatClient._raise_provider_error(chunk)
                if (
                    isinstance(chunk, dict)
                    and chunk.get("choices") == []
                    and "usage" in chunk
                ):
                    continue
                choice = chunk.get("choices", [])[0]
            except (json.JSONDecodeError, AttributeError, IndexError, TypeError):
                raise TamuAIInvalidJSON("TAMU returned a malformed stream event")
            if not isinstance(choice, dict):
                raise TamuAIInvalidJSON("TAMU returned an invalid stream choice")
            delta = choice.get("delta", {})
            finish_reason = choice.get("finish_reason") or finish_reason
            message = choice.get("message", {})
            text = delta.get("content") or message.get("content")
            if isinstance(text, str):
                content_parts.append(text)

        if not content_parts:
            raise TamuAIError("TAMU AI Chat returned a non-JSON response")
        return {
            "choices": [
                {
                    "message": {"content": "".join(content_parts)},
                    "finish_reason": finish_reason,
                }
            ]
        }

    def _post(
        self, messages: List[Dict[str, str]], max_tokens: Optional[int] = None
    ) -> Dict[str, Any]:
        for attempt in range(self.max_retries + 1):
            try:
                return self._post_once(messages, max_tokens)
            except TamuAIQuotaExceeded:
                with self._budget_lock:
                    self._quota_exhausted = True
                raise
            except (
                requests.Timeout,
                requests.ConnectionError,
                TamuAITransientError,
            ) as exc:
                if attempt >= self.max_retries:
                    raise TamuAIError(
                        f"TAMU request failed after {attempt + 1} attempts: {exc}"
                    ) from exc
                with self._budget_lock:
                    self.retries_made += 1
                logger.debug("TAMU temporary failure; retrying: %s", exc)
                time.sleep(min(2**attempt, 8))
        raise AssertionError("unreachable")

    def _post_once(
        self, messages: List[Dict[str, str]], max_tokens: Optional[int] = None
    ) -> Dict[str, Any]:
        self._consume_request_budget()
        self._thread_sessions.requests = (
            getattr(self._thread_sessions, "requests", 0) + 1
        )
        response = self._request_session().post(
            self.endpoint,
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            params={"bypass_filter": "false"},
            json={
                "model": self.model,
                "messages": messages,
                "temperature": 0.1,
                "max_tokens": max_tokens or self.max_output_tokens,
                "stream": False,
                "response_format": (
                    professor_summary_response_format()
                    if self.response_format == "json_schema"
                    else {"type": "json_object"}
                ),
            },
            timeout=self.timeout_seconds,
        )
        # Inspect error envelopes before relying on HTTP status. TAMU's gateway
        # has returned budget_exceeded with status 200 and error.code="400".
        try:
            envelope = response.json()
        except ValueError:
            envelope = None
        if isinstance(envelope, Mapping):
            self._raise_provider_error(envelope)
        if response.status_code == 429:
            with self._budget_lock:
                self._quota_exhausted = True
            raise TamuAIQuotaExceeded(
                "TAMU AI Chat daily allowance or rate limit was reached"
            )
        if response.status_code in {500, 502, 503, 504}:
            raise TamuAITransientError(f"HTTP {response.status_code}")
        try:
            response.raise_for_status()
        except requests.RequestException as exc:
            raise TamuAIError(
                f"TAMU AI Chat request failed with status {response.status_code}"
            ) from exc
        body = self._response_body(response)
        choices = body.get("choices")
        if (
            not isinstance(choices, list)
            or not choices
            or not isinstance(choices[0], dict)
        ):
            raise TamuAIError("TAMU response contains no valid choice")
        choice = choices[0]
        message = choice.get("message")
        if isinstance(message, dict) and message.get("refusal"):
            raise TamuAIError("TAMU declined the generation request")
        if choice.get("finish_reason") == "content_filter":
            raise TamuAIError("TAMU response was blocked by its content filter")
        if choice.get("finish_reason") == "length":
            raise TamuAITruncated("TAMU output reached its token limit")
        return self._extract_json(self._message_text(body))

    def generate_professor_summary(
        self, user_text: str, *, max_tokens: Optional[int] = None
    ) -> ProfessorSummaryOutput:
        """One structured request for a professor; one strict retry on bad JSON.

        Truncation is not retried here: repeating the same prompt would hit the
        same limit, so the caller decides whether to split by course group.
        """
        messages = [
            {"role": "system", "content": PROFESSOR_SYSTEM_PROMPT},
            {"role": "user", "content": user_text},
        ]
        for attempt in range(2):
            try:
                result = self._post(messages, max_tokens=max_tokens)
                try:
                    return ProfessorSummaryOutput.model_validate(result)
                except ValidationError as exc:
                    # Never log review text or entire provider responses.
                    raise TamuAIInvalidJSON(
                        "TAMU summary failed schema validation"
                    ) from exc
            except TamuAITruncated:
                raise
            except TamuAIInvalidJSON as exc:
                if attempt:
                    raise
                logger.debug("TAMU JSON recovery: %s", exc)
                with self._budget_lock:
                    self.json_retries += 1
                messages = [
                    messages[0],
                    {
                        "role": "user",
                        "content": user_text
                        + "\nReturn one complete JSON object only. Keep every "
                        "claim under 30 words.",
                    },
                ]
        raise AssertionError("unreachable")


class TypeSafeReviewJudge:
    """Typed topic, sentiment, and evidence judgments for review summaries."""

    TOPIC_CRITERIA = {
        "teaching": "Lecture quality, explanations, clarity, or learning experience.",
        "exams": "Tests, quizzes, midterms, finals, or assessment content.",
        "grading": "Grades, curves, points, fairness, or grading strictness.",
        "workload": "Homework, assignments, projects, time, or amount of work.",
        "personality": "Behavior, helpfulness, approachability, or demeanor.",
        "policies": "Attendance, deadlines, late work, rules, or requirements.",
        "other": "None of the other topics is the primary subject.",
    }
    SENTIMENT_CRITERIA = {
        "positive": "The reviews are predominantly favorable about this topic.",
        "mixed": "The reviews contain meaningful favorable and unfavorable views.",
        "negative": "The reviews are predominantly unfavorable about this topic.",
        "unclear": "There is not enough information to determine sentiment.",
    }

    IMPORTANCE_CRITERIA = [
        "Trivial detail that would not help a student choose or prepare for the course.",
        "Minor detail with limited usefulness.",
        "Useful information, but not a major recurring consideration.",
        "Important recurring consideration that could affect a student's experience.",
        "Essential consideration that could materially affect course selection or success.",
    ]

    def __init__(
        self,
        *,
        support_threshold: float = 0.72,
        topic_threshold: float = 0.62,
        risk_threshold: float = 0.55,
    ) -> None:
        self.support_threshold = support_threshold
        self.topic_threshold = topic_threshold
        self.risk_threshold = risk_threshold
        # Conservative UTF-8 payload budgets, not claimed tokenizer counts.
        self.max_state_bytes = max(
            2000, int(os.getenv("TYPESAFE_MAX_STATE_BYTES", "24000"))
        )
        self.max_request_bytes = max(
            self.max_state_bytes + 2000,
            int(os.getenv("TYPESAFE_MAX_REQUEST_BYTES", "48000")),
        )

    @staticmethod
    def _state_size(state):
        return len(json.dumps(state, ensure_ascii=False).encode("utf-8"))

    def _evaluate(self, state, questions):
        """Scope each question group to its cluster; bound and adapt question batches.

        Never truncate review evidence to make verification fit. Missing answers
        remain missing so the pipeline's source-text fallback can handle them.
        """
        merged = SimpleNamespace(choices={}, nouls={}, scores={})
        groups = defaultdict(dict)
        for key, question in questions.items():
            cluster_id = (
                key.rsplit("_", 2)[-2]
                if key.startswith("claim_")
                else key.rsplit("_", 1)[-1]
            )
            claim_index = (
                int(key.rsplit("_", 1)[-1]) if key.startswith("claim_") else None
            )
            # Keep small claims batched together; isolate only oversized state.
            if (
                claim_index is not None
                and self._state_size(
                    {"clusters": {cluster_id: state["clusters"][cluster_id]}}
                )
                <= self.max_state_bytes
            ):
                claim_index = None
            groups[(cluster_id, claim_index)][key] = question
        with TypeSafeClient() as client:

            def send(scoped, items):
                if not items:
                    return
                # SDK questions expose instructions/criteria; fake test questions
                # use kwargs. Include both in the request-size estimate.
                serialized = {
                    key: getattr(
                        value,
                        "kwargs",
                        {
                            "instructions": getattr(value, "instructions", None),
                            "criteria": getattr(value, "criteria", None),
                        },
                    )
                    for key, value in items
                }
                if (
                    self._state_size(scoped) + self._state_size(serialized)
                    > self.max_request_bytes
                    and len(items) > 1
                ):
                    midpoint = len(items) // 2
                    send(scoped, items[:midpoint])
                    send(scoped, items[midpoint:])
                    return
                if (
                    self._state_size(scoped) + self._state_size(serialized)
                    > self.max_request_bytes
                ):
                    logger.warning(
                        "TypeSafe single judgment exceeds request budget; leaving it unverified"
                    )
                    return
                try:
                    response = client.system_one(state=scoped, questions=dict(items))
                except Exception as exc:
                    if "max_tokens_exceeded" not in str(exc):
                        raise
                    if len(items) > 1:
                        midpoint = len(items) // 2
                        send(scoped, items[:midpoint])
                        send(scoped, items[midpoint:])
                    else:
                        logger.warning(
                            "TypeSafe context exceeds model limit for one judgment; leaving it unverified"
                        )
                    return
                for name in ("choices", "nouls", "scores"):
                    getattr(merged, name).update(getattr(response, name, {}))

            for (cluster_id, claim_index), group in groups.items():
                cluster_state = dict(state["clusters"][cluster_id])
                if claim_index is not None:
                    # Keep stable indices while excluding other claims' repeated evidence.
                    claims = [None] * (claim_index + 1)
                    claims[claim_index] = cluster_state["claims"][claim_index]
                    cluster_state["claims"] = claims
                scoped = {"clusters": {cluster_id: cluster_state}}
                if self._state_size(scoped) > self.max_state_bytes:
                    logger.warning(
                        "TypeSafe cluster context exceeds local size budget; using fallback without truncating evidence"
                    )
                    continue
                send(scoped, list(group.items()))
        return merged

    @classmethod
    def from_env(cls) -> Optional["TypeSafeReviewJudge"]:
        """Verification is opt-in: it adds remote calls to every professor."""
        enabled = os.getenv("TYPESAFE_VERIFY", "0").strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        if (
            not enabled
            or not os.getenv("TYPESAFE_API_KEY", "").strip()
            or TypeSafeClient is None
        ):
            return None
        return cls(
            support_threshold=TYPESAFE_SUPPORT_THRESHOLD,
            topic_threshold=TYPESAFE_TOPIC_THRESHOLD,
            risk_threshold=TYPESAFE_RISK_THRESHOLD,
        )

    @staticmethod
    def _choice(response: Any, question_id: str) -> tuple[str, float]:
        answer = response.choices[question_id]
        return str(answer.choice), float(answer.confidence)

    @staticmethod
    def _noul(response: Any, question_id: str) -> float:
        answer = response.nouls[question_id]
        return float(answer.noul)

    @staticmethod
    def _score(response: Any, question_id: str) -> tuple[float, float]:
        answer = response.scores[question_id]
        return float(answer.score), float(answer.confidence)

    @staticmethod
    def _cluster_state(
        clusters: Mapping[int, List[ProcessedReview]],
    ) -> Dict[str, Any]:
        return {
            "clusters": {
                str(cluster_id): {
                    "reviews": [
                        {
                            "review_id": review.review_id,
                            "text": review.text,
                            **(
                                {"rating": review.original_rating}
                                if getattr(review, "original_rating", None) is not None
                                else {}
                            ),
                        }
                        for review in reviews
                    ]
                }
                for cluster_id, reviews in clusters.items()
            }
        }

    @staticmethod
    def _claim_sentiment(
        positive_probability: float,
        negative_probability: float,
        mixed_probability: float = 0.0,
    ) -> tuple[str, float]:
        """Conservatively gate claims into one display category."""
        if (
            mixed_probability >= 0.5
            or min(positive_probability, negative_probability) >= 0.4
        ):
            return "mixed", max(
                mixed_probability,
                min(positive_probability, negative_probability),
            )

        # Require clear valence and separation from the opposite valence before
        # putting a claim in either user-facing column.
        if (
            positive_probability >= 0.65
            and positive_probability - negative_probability >= 0.25
        ):
            return "positive", positive_probability
        if (
            negative_probability >= 0.65
            and negative_probability - positive_probability >= 0.25
        ):
            return "negative", negative_probability
        return "neutral", 1.0 - max(
            positive_probability,
            negative_probability,
            mixed_probability,
        )

    def classify_clusters(
        self, clusters: Mapping[int, List[ProcessedReview]]
    ) -> Dict[int, ClusterJudgment]:
        if not clusters or TypeSafeClient is None or Choice is None:
            return {}
        if self._state_size(self._cluster_state(clusters)) > self.max_state_bytes:
            combined = {}
            for cluster_id, reviews in clusters.items():
                chunks, current = [], []
                for review in reviews:
                    if (
                        current
                        and self._state_size(
                            self._cluster_state({cluster_id: current + [review]})
                        )
                        > self.max_state_bytes
                    ):
                        chunks.append(current)
                        current = []
                    current.append(review)
                if current:
                    chunks.append(current)
                if not chunks:
                    continue
                if (
                    len(chunks) == 1
                    and self._state_size(self._cluster_state({cluster_id: chunks[0]}))
                    > self.max_state_bytes
                ):
                    logger.warning(
                        "TypeSafe single review exceeds context budget; cluster will use fallback"
                    )
                    continue
                parts = [
                    (
                        len(chunk),
                        self.classify_clusters({cluster_id: chunk}).get(cluster_id),
                    )
                    for chunk in chunks
                ]
                if any(judgment is None for _, judgment in parts):
                    continue
                weights = defaultdict(int)
                for size, judgment in parts:
                    weights[judgment.topic] += size
                total = sum(size for size, _ in parts)
                sentiments = {judgment.sentiment for _, judgment in parts}
                sentiment = (
                    "mixed"
                    if "mixed" in sentiments
                    or {"positive", "negative"}.issubset(sentiments)
                    else max(
                        sentiments,
                        key=lambda value: sum(
                            size for size, j in parts if j.sentiment == value
                        ),
                    )
                )
                combined[cluster_id] = ClusterJudgment(
                    topic=max(weights, key=weights.get),
                    topic_confidence=sum(size * j.topic_confidence for size, j in parts)
                    / total,
                    sentiment=sentiment,
                    sentiment_confidence=min(j.sentiment_confidence for _, j in parts),
                    topic_probabilities={
                        topic: max(
                            j.topic_probabilities.get(topic, 0) for _, j in parts
                        )
                        for topic in self.TOPIC_CRITERIA
                        if topic != "other"
                    },
                    disagreement_probability=max(
                        [j.disagreement_probability for _, j in parts]
                        + [
                            (
                                1.0
                                if {"positive", "negative"}.issubset(sentiments)
                                else 0.0
                            )
                        ]
                    ),
                    importance=sum(size * j.importance for size, j in parts) / total,
                    importance_confidence=min(
                        j.importance_confidence for _, j in parts
                    ),
                )
            return combined
        questions: Dict[str, Any] = {}
        for cluster_id in clusters:
            path = f"`clusters.{cluster_id}.reviews`"
            questions[f"topic_{cluster_id}"] = Choice(
                instructions=(
                    f"Which single topic is the primary subject of the professor "
                    f"reviews in {path}?"
                ),
                criteria=self.TOPIC_CRITERIA,
            )
            questions[f"sentiment_{cluster_id}"] = Choice(
                instructions=(
                    f"What is the overall sentiment expressed by the professor "
                    f"reviews in {path}?"
                ),
                criteria=self.SENTIMENT_CRITERIA,
            )
            if Noul is not None:
                for topic, description in self.TOPIC_CRITERIA.items():
                    if topic == "other":
                        continue
                    questions[f"contains_{topic}_{cluster_id}"] = Noul(
                        instructions=(
                            f"Do the reviews in {path} contain substantive information "
                            f"about {topic} that merits mention in a useful summary?"
                        ),
                        criteria={"true": description},
                    )
                questions[f"disagreement_{cluster_id}"] = Noul(
                    instructions=(
                        f"Do the reviews in {path} express meaningful disagreement or "
                        "materially different experiences that a faithful summary should "
                        "preserve?"
                    )
                )
            if Score is not None:
                questions[f"importance_{cluster_id}"] = Score(
                    instructions=(
                        f"How important is the information in {path} to a student "
                        "choosing or preparing for this course?"
                    ),
                    criteria=self.IMPORTANCE_CRITERIA,
                )
        try:
            response = self._evaluate(self._cluster_state(clusters), questions)
        except Exception as exc:
            logger.warning("TypeSafe cluster classification failed: %s", exc)
            return {}

        judgments: Dict[int, ClusterJudgment] = {}
        for cluster_id in clusters:
            try:
                topic, topic_confidence = self._choice(response, f"topic_{cluster_id}")
                sentiment, sentiment_confidence = self._choice(
                    response, f"sentiment_{cluster_id}"
                )
            except (AttributeError, KeyError, TypeError, ValueError):
                continue
            topic_probabilities: Dict[str, float] = {}
            for candidate in self.TOPIC_CRITERIA:
                if candidate == "other":
                    continue
                try:
                    topic_probabilities[candidate] = self._noul(
                        response, f"contains_{candidate}_{cluster_id}"
                    )
                except (AttributeError, KeyError, TypeError, ValueError):
                    continue
            try:
                disagreement_probability = self._noul(
                    response, f"disagreement_{cluster_id}"
                )
            except (AttributeError, KeyError, TypeError, ValueError):
                disagreement_probability = 0.0
            try:
                importance, importance_confidence = self._score(
                    response, f"importance_{cluster_id}"
                )
            except (AttributeError, KeyError, TypeError, ValueError):
                importance, importance_confidence = 0.0, 0.0
            judgments[cluster_id] = ClusterJudgment(
                topic=topic,
                topic_confidence=topic_confidence,
                sentiment=sentiment,
                sentiment_confidence=sentiment_confidence,
                topic_probabilities=topic_probabilities,
                disagreement_probability=disagreement_probability,
                importance=importance,
                importance_confidence=importance_confidence,
            )
        return judgments

    def _verify_large_claims(self, clusters, claims):
        """Retain cited evidence in every shard and check all remaining reviews.

        Support uses the lowest result and risks the highest. If any shard lacks
        an answer, the claim remains unverified rather than silently accepted.
        """
        results = {}
        for cluster_id, items in claims.items():
            reviews = clusters[cluster_id]
            for index, claim in enumerate(items):
                cited_ids = set(claim.evidence_review_ids)
                cited = [r for r in reviews if r.review_id in cited_ids]
                extra = (
                    self._state_size(self._cluster_state({cluster_id: cited}))
                    + len(claim.text.encode("utf-8"))
                    + 1000
                )

                def fits(group):
                    return (
                        self._state_size(self._cluster_state({cluster_id: group}))
                        + extra
                        <= self.max_state_bytes
                    )

                if not cited or not fits(cited):
                    logger.warning(
                        "TypeSafe cited evidence exceeds context budget; claim remains unverified"
                    )
                    continue
                shards, current, oversized = [], list(cited), False
                for review in reviews:
                    if review.review_id in cited_ids:
                        continue
                    if not fits(current + [review]):
                        if len(current) > len(cited):
                            shards.append(current)
                        current = list(cited)
                    if not fits(current + [review]):
                        oversized = True
                        break
                    current.append(review)
                if oversized:
                    logger.warning(
                        "TypeSafe single review exceeds verification budget; claim remains unverified"
                    )
                    continue
                shards.append(current)
                answers = [
                    self.verify_claims(
                        {cluster_id: shard}, {cluster_id: [claim]}, _bounded=True
                    ).get((cluster_id, 0))
                    for shard in shards
                ]
                if any(answer is None for answer in answers):
                    continue
                first = answers[0]
                results[(cluster_id, index)] = ClaimVerification(
                    support_probability=min(a.support_probability for a in answers),
                    overgeneralization_probability=max(
                        a.overgeneralization_probability for a in answers
                    ),
                    disagreement_erasure_probability=max(
                        a.disagreement_erasure_probability for a in answers
                    ),
                    topic=(
                        first.topic
                        if all(a.topic == first.topic for a in answers)
                        else "other"
                    ),
                    topic_confidence=min(a.topic_confidence for a in answers),
                    sentiment=(
                        first.sentiment
                        if all(a.sentiment == first.sentiment for a in answers)
                        else "mixed"
                    ),
                    sentiment_confidence=min(a.sentiment_confidence for a in answers),
                    importance=sum(a.importance for a in answers) / len(answers),
                    importance_confidence=min(a.importance_confidence for a in answers),
                )
        return results

    def verify_claims(
        self,
        clusters: Mapping[int, List[ProcessedReview]],
        claims: Mapping[int, List[SummaryClaim]],
        *,
        _bounded: bool = False,
    ) -> Dict[tuple[int, int], ClaimVerification]:
        """Verify each generated claim independently against cited evidence."""
        if (
            not any(claims.values())
            or TypeSafeClient is None
            or Noul is None
            or Choice is None
            or Score is None
        ):
            return {}
        if (
            not _bounded
            and self._state_size(self._cluster_state(clusters))
            > self.max_state_bytes // 2
        ):
            return self._verify_large_claims(clusters, claims)
        state = self._cluster_state(clusters)
        questions: Dict[str, Any] = {}
        for cluster_id, cluster_claims in claims.items():
            reviews_by_id = {
                review.review_id: review for review in clusters[cluster_id]
            }
            claim_state = []
            for claim_index, claim in enumerate(cluster_claims):
                claim_state.append(
                    {
                        "text": claim.text,
                        "cited_reviews": [
                            {
                                "review_id": review_id,
                                "text": reviews_by_id[review_id].text,
                                **(
                                    {"rating": reviews_by_id[review_id].original_rating}
                                    if getattr(
                                        reviews_by_id[review_id],
                                        "original_rating",
                                        None,
                                    )
                                    is not None
                                    else {}
                                ),
                            }
                            for review_id in claim.evidence_review_ids
                            if review_id in reviews_by_id
                        ],
                    }
                )
                path = f"`clusters.{cluster_id}.claims[{claim_index}]`"
                questions[f"claim_supported_{cluster_id}_{claim_index}"] = Noul(
                    instructions=(
                        f"Is every factual assertion in {path}.text directly supported "
                        f"by at least one review in {path}.cited_reviews?"
                    )
                )
                questions[f"claim_overgeneralized_{cluster_id}_{claim_index}"] = Noul(
                    instructions=(
                        f"Does {path}.text overgeneralize beyond what the reviews in "
                        f"{path}.cited_reviews justify?"
                    )
                )
                questions[f"claim_erases_disagreement_{cluster_id}_{claim_index}"] = (
                    Noul(
                        instructions=(
                            f"When compared with all reviews in `clusters.{cluster_id}.reviews`, "
                            f"does {path}.text erase or contradict meaningful disagreement?"
                        )
                    )
                )
                questions[f"claim_topic_{cluster_id}_{claim_index}"] = Choice(
                    instructions=(
                        f"What is the single primary topic of {path}.text? Classify "
                        "helpfulness, caring, accessibility, and availability in office "
                        "hours as personality unless the claim is explicitly about the "
                        "quality of explanations or learning. Judge the claim itself, "
                        "not the overall review cluster."
                    ),
                    criteria=self.TOPIC_CRITERIA,
                )
                questions[f"claim_positive_{cluster_id}_{claim_index}"] = Noul(
                    instructions=(
                        f"Does {path}.text communicate a genuinely favorable trait, "
                        "experience, outcome, or benefit? A merely factual policy is "
                        "not positive. Judge the claim itself using its cited reviews."
                    )
                )
                questions[f"claim_negative_{cluster_id}_{claim_index}"] = Noul(
                    instructions=(
                        f"Does {path}.text communicate a genuinely unfavorable trait, "
                        "experience, outcome, cost, or burden? Judge the claim itself "
                        "using its cited reviews."
                    )
                )
                questions[f"claim_mixed_{cluster_id}_{claim_index}"] = Noul(
                    instructions=(
                        f"Does {path}.text explicitly combine, contrast, hedge, or "
                        "balance both favorable and unfavorable experiences? This is "
                        "true for wording such as 'despite', 'but', 'while', 'although', "
                        "or reports that reviews are split. Judge the claim itself."
                    )
                )
                questions[f"claim_importance_{cluster_id}_{claim_index}"] = Score(
                    instructions=(
                        f"How important is {path}.text to a student choosing or "
                        "preparing for this course?"
                    ),
                    criteria=self.IMPORTANCE_CRITERIA,
                )
            state["clusters"][str(cluster_id)]["claims"] = claim_state

        try:
            response = self._evaluate(state, questions)
        except Exception as exc:
            logger.warning("TypeSafe claim verification failed: %s", exc)
            return {}

        results: Dict[tuple[int, int], ClaimVerification] = {}
        for cluster_id, cluster_claims in claims.items():
            for claim_index, _ in enumerate(cluster_claims):
                try:
                    topic, topic_confidence = self._choice(
                        response, f"claim_topic_{cluster_id}_{claim_index}"
                    )
                    positive_probability = self._noul(
                        response, f"claim_positive_{cluster_id}_{claim_index}"
                    )
                    negative_probability = self._noul(
                        response, f"claim_negative_{cluster_id}_{claim_index}"
                    )
                    mixed_probability = self._noul(
                        response, f"claim_mixed_{cluster_id}_{claim_index}"
                    )
                    sentiment, sentiment_confidence = self._claim_sentiment(
                        positive_probability,
                        negative_probability,
                        mixed_probability,
                    )
                    importance, importance_confidence = self._score(
                        response, f"claim_importance_{cluster_id}_{claim_index}"
                    )
                    results[(cluster_id, claim_index)] = ClaimVerification(
                        support_probability=self._noul(
                            response,
                            f"claim_supported_{cluster_id}_{claim_index}",
                        ),
                        overgeneralization_probability=self._noul(
                            response,
                            f"claim_overgeneralized_{cluster_id}_{claim_index}",
                        ),
                        disagreement_erasure_probability=self._noul(
                            response,
                            f"claim_erases_disagreement_{cluster_id}_{claim_index}",
                        ),
                        topic=topic,
                        topic_confidence=topic_confidence,
                        sentiment=sentiment,
                        sentiment_confidence=sentiment_confidence,
                        importance=importance,
                        importance_confidence=importance_confidence,
                    )
                except (AttributeError, KeyError, TypeError, ValueError):
                    continue
        return results

    def verify_claim_support(
        self,
        claims: List[SummaryClaim],
        review_texts: Mapping[str, str],
    ) -> Dict[int, float]:
        """Support probability per claim index, checked against its citations.

        Only support is asked: the acceptance policy no longer rejects on
        overgeneralization or disagreement risk, so those questions would only
        add cost. Claims are packed into byte-bounded groups, one request each.
        """
        if not claims or TypeSafeClient is None or Noul is None:
            return {}
        groups: List[List[int]] = []
        sizes: List[int] = []
        entries = []
        for index, claim in enumerate(claims):
            entry = {
                "text": claim.text,
                "cited_reviews": [
                    {"review_id": review_id, "text": review_texts[review_id]}
                    for review_id in claim.evidence_review_ids
                    if review_id in review_texts
                ],
            }
            entries.append(entry)
            size = self._state_size(entry)
            if groups and sizes[-1] + size <= self.max_state_bytes // 2:
                groups[-1].append(index)
                sizes[-1] += size
            else:
                groups.append([index])
                sizes.append(size)

        state: Dict[str, Any] = {"clusters": {}}
        questions: Dict[str, Any] = {}
        positions: Dict[str, int] = {}
        for group_id, indices in enumerate(groups):
            state["clusters"][str(group_id)] = {
                "reviews": [],
                "claims": [entries[index] for index in indices],
            }
            for position, index in enumerate(indices):
                key = f"claim_supported_{group_id}_{position}"
                path = f"`clusters.{group_id}.claims[{position}]`"
                questions[key] = Noul(
                    instructions=(
                        f"Is every factual assertion in {path}.text directly supported "
                        f"by at least one review in {path}.cited_reviews?"
                    )
                )
                positions[key] = index
        try:
            response = self._evaluate(state, questions)
        except Exception as exc:
            logger.warning("TypeSafe claim support check failed: %s", exc)
            return {}
        support: Dict[int, float] = {}
        for key, index in positions.items():
            try:
                support[index] = self._noul(response, key)
            except (AttributeError, KeyError, TypeError, ValueError):
                continue
        return support

    def verify_summaries(
        self,
        clusters: Mapping[int, List[ProcessedReview]],
        summaries: Mapping[int, str],
    ) -> Dict[int, float]:
        """Return the probability that every summary claim has review support."""
        if not summaries or TypeSafeClient is None or Noul is None:
            return {}
        state = self._cluster_state(clusters)
        for cluster_id, summary in summaries.items():
            state["clusters"][str(cluster_id)]["summary"] = summary
        questions: Dict[str, Any] = {}
        for cluster_id in summaries:
            questions[f"supported_{cluster_id}"] = Noul(
                instructions=(
                    "Is every factual claim in "
                    f"`clusters.{cluster_id}.summary` supported by at least one review "
                    f"in `clusters.{cluster_id}.reviews`, without contradicting the "
                    "reviews or erasing meaningful disagreement?"
                )
            )
        try:
            response = self._evaluate(state, questions)
        except Exception as exc:
            logger.warning("TypeSafe summary verification failed: %s", exc)
            return {}

        probabilities: Dict[int, float] = {}
        for cluster_id in summaries:
            try:
                probabilities[cluster_id] = self._noul(
                    response, f"supported_{cluster_id}"
                )
            except (AttributeError, KeyError, TypeError, ValueError):
                continue
        return probabilities

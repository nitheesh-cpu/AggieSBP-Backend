import threading
import unittest
from unittest.mock import patch

from pipelines.professors.hierarchical_summarization.pipeline import (
    HierarchicalSummarizationPipeline,
)
from pipelines.professors.hierarchical_summarization.ai_clients import (
    TamuAIQuotaExceeded,
)


class SharedQueueTests(unittest.TestCase):
    def pipeline(self):
        pipeline = object.__new__(HierarchicalSummarizationPipeline)
        owner = threading.get_ident()

        def prepare(raw, pid):
            self.assertEqual(threading.get_ident(), owner)
            return pid, [], len(raw), {str(i): value for i, value in enumerate(raw)}, {}

        def finish(context, results):
            self.assertEqual(threading.get_ident(), owner)
            self.assertEqual(len(results), len(context[3]))
            return context[0]

        pipeline.prepare_professor_reviews = prepare
        pipeline.finish_professor_reviews = finish
        return pipeline

    def test_fast_professor_finishes_before_slow_professor(self):
        pipeline = self.pipeline()
        release = threading.Event()

        def remote(value):
            if value == "slow":
                if not release.wait(3):
                    raise RuntimeError("head-of-line blocking")
            return value

        pipeline._prepare_course = remote
        with patch.dict("os.environ", {"SUMMARY_API_WORKERS": "2"}):
            stream = pipeline.process_professor_batch(
                [("slow", ["slow"]), ("fast", ["fast"])]
            )
            try:
                first = next(stream)
                self.assertEqual(first, ("fast", "fast", None))
            finally:
                release.set()
            self.assertEqual(list(stream), [("slow", "slow", None)])

    def test_four_workers_share_one_queue_across_professors(self):
        pipeline = self.pipeline()
        barrier = threading.Barrier(4)
        lock = threading.Lock()
        active = 0
        peak = 0

        def remote(value):
            nonlocal active, peak
            with lock:
                active += 1
                peak = max(peak, active)
            barrier.wait(3)
            with lock:
                active -= 1
            return value

        pipeline._prepare_course = remote
        with patch.dict("os.environ", {"SUMMARY_API_WORKERS": "4"}):
            results = list(
                pipeline.process_professor_batch([(str(i), [i]) for i in range(8)])
            )
        self.assertEqual(peak, 4)
        self.assertEqual(len(results), 8)
        self.assertTrue(all(error is None for _, _, error in results))

    def test_quota_stops_before_refilling_or_finishing(self):
        pipeline = self.pipeline()
        calls = []

        def remote(value):
            calls.append(value)
            raise TamuAIQuotaExceeded("budget")

        pipeline._prepare_course = remote
        with patch.dict("os.environ", {"SUMMARY_API_WORKERS": "1"}):
            with self.assertRaises(TamuAIQuotaExceeded):
                list(pipeline.process_professor_batch([("p", [1]), ("q", [2])]))
        self.assertEqual(calls, [1])

    def test_failed_professor_does_not_poison_sibling_results(self):
        pipeline = self.pipeline()

        def remote(value):
            if value == "bad":
                raise ValueError("bad course")
            return value

        pipeline._prepare_course = remote
        with patch.dict("os.environ", {"SUMMARY_API_WORKERS": "2"}):
            results = list(
                pipeline.process_professor_batch(
                    [("p", ["bad", "good"]), ("q", ["good"])]
                )
            )
        by_id = {pid: (summary, error) for pid, summary, error in results}
        self.assertIsInstance(by_id["p"][1], ValueError)
        self.assertEqual(by_id["q"], ("q", None))


if __name__ == "__main__":
    unittest.main()

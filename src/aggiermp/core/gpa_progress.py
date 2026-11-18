"""Share the latest GPA import progress between the pipeline and admin API."""

import asyncio
import logging
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, NonNegativeInt
from pydantic.alias_generators import to_camel

from .config import settings

logger = logging.getLogger(__name__)
PROGRESS_PATH = settings.data_dir / "gpa-upsert.json"
STALE_AFTER = timedelta(minutes=5)


class GpaRun(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)

    status: Literal[
        "selecting",
        "fetching",
        "cooldown",
        "finalizing",
        "succeeded",
        "completed_with_errors",
        "failed",
        "interrupted",
        "stale",
    ] = "selecting"
    started_at: AwareDatetime
    updated_at: AwareDatetime
    total_courses: NonNegativeInt | None = None
    attempted_courses: NonNegativeInt = 0
    failed_courses: NonNegativeInt = 0
    records_written: NonNegativeInt = 0
    retry_at: AwareDatetime | None = None


class GpaProgress:
    def __init__(self, path: Path = PROGRESS_PATH):
        self.path = path
        now = datetime.now(timezone.utc)
        self.run = GpaRun(started_at=now, updated_at=now)
        self.write_failed = False

    def publish(self) -> None:
        self.run.updated_at = datetime.now(timezone.utc)
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            temp = tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                dir=self.path.parent,
                delete=False,
            )
            temporary_path = Path(temp.name)
            try:
                with temp:
                    temp.write(self.run.model_dump_json(by_alias=True))
                temporary_path.replace(self.path)
            finally:
                temporary_path.unlink(missing_ok=True)
        except OSError:
            if not self.write_failed:
                logger.warning("Could not publish GPA import progress", exc_info=True)
            self.write_failed = True
        else:
            self.write_failed = False

    async def heartbeat(self) -> None:
        while True:
            await asyncio.sleep(30)
            self.publish()


def read_gpa_progress(path: Path = PROGRESS_PATH) -> GpaRun | None:
    try:
        with path.open("rb") as snapshot:
            data = snapshot.read(4097)
    except FileNotFoundError:
        return None
    if len(data) > 4096:
        raise ValueError("Invalid GPA progress snapshot size")
    run = GpaRun.model_validate_json(data)
    if (
        run.status in {"selecting", "fetching", "cooldown", "finalizing"}
        and datetime.now(timezone.utc) - run.updated_at > STALE_AFTER
    ):
        run.status = "stale"
    return run

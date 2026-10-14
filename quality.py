"""Quality helpers used by the DAG. Reads the metrics file written by the silver job."""
import json
import shutil
from pathlib import Path

from src.config import METRICS, QUARANTINE, SILVER


def read_metrics(ds: str) -> dict:
    return json.loads(Path(f"{METRICS}/dt={ds}.json").read_text())


def bad_row_ratio(ds: str) -> float:
    m = read_metrics(ds)
    return m["rejected_rows"] / m["total_rows"] if m["total_rows"] else 1.0


def quarantine_partition(ds: str) -> str:
    """Move this run's silver partition out of the table so bad data never reaches gold."""
    src = Path(f"{SILVER}/ingest_date={ds}")
    dst = Path(f"{QUARANTINE}/ingest_date={ds}")
    if dst.exists():
        shutil.rmtree(dst)
    dst.parent.mkdir(parents=True, exist_ok=True)
    if src.exists():
        shutil.move(str(src), str(dst))
    return str(dst)

"""Base ingestor class with logging, retry, and catalog registration."""

from abc import ABC, abstractmethod
from pathlib import Path

import structlog
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from catalog.manager import CatalogManager

log = structlog.get_logger()


class BaseIngestor(ABC):
    """Abstract base for all data source ingestors.

    Subclasses must define class attributes and implement fetch().
    """

    name: str
    source_type: str
    data_type: str
    category: str
    schedule: str
    license: str

    def __init__(self, catalog: CatalogManager, bronze_root: Path):
        self.catalog = catalog
        self.bronze_root = bronze_root
        self.bronze_dir = bronze_root / self.data_type / self.name
        self.bronze_dir.mkdir(parents=True, exist_ok=True)

    @abstractmethod
    def fetch(self, **kwargs) -> list[Path]:
        """Download data to bronze_dir. Return list of created file paths."""
        ...

    def run(self, **kwargs) -> None:
        """Execute fetch + register with error handling."""
        log.info("ingestor.start", name=self.name)
        try:
            paths = self.fetch(**kwargs)
            for path in paths:
                self.catalog.register({
                    "dataset_id": self.name,
                    "source": self.name,
                    "category": self.category,
                    "data_type": self.data_type,
                    "layer": "bronze",
                    "file_path": str(path),
                    "format": path.suffix.lstrip("."),
                    "license": self.license,
                    "ingestor": f"{self.name}.py",
                    "status": "complete",
                    "variables": kwargs.get("variables", []),
                    "temporal_start": kwargs.get("start_date"),
                    "temporal_end": kwargs.get("end_date"),
                    "temporal_resolution": kwargs.get("temporal_resolution"),
                    "spatial_bbox": kwargs.get("spatial_bbox"),
                    "spatial_resolution": kwargs.get("spatial_resolution"),
                    "crs": kwargs.get("crs", "EPSG:4326"),
                    "notes": kwargs.get("notes", ""),
                })
            log.info("ingestor.complete", name=self.name, files=len(paths))
        except Exception as e:
            log.error("ingestor.failed", name=self.name, error=str(e))
            self.catalog.register({
                "dataset_id": self.name,
                "source": self.name,
                "category": self.category,
                "data_type": self.data_type,
                "layer": "bronze",
                "file_path": str(self.bronze_dir / ".failed"),
                "format": "",
                "license": self.license,
                "ingestor": f"{self.name}.py",
                "status": "failed",
                "notes": str(e),
            })

    @staticmethod
    def retry_fetch(func):
        """Decorator for fetch methods that call external APIs."""
        return retry(
            stop=stop_after_attempt(3),
            wait=wait_exponential(multiplier=1, min=2, max=60),
            retry=retry_if_exception_type((ConnectionError, TimeoutError)),
            before_sleep=lambda state: log.warning(
                "ingestor.retry",
                attempt=state.attempt_number,
                wait=state.next_action.sleep,
            ),
        )(func)

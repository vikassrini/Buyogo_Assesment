
from typing import Dict, Any

class AnalyticsCache:
    def __init__(self):
        self.cache: Dict[str, Any] = {}
        self.last_updated: float = 0.0

    def is_stale(self, db_last_updated: float) -> bool:
        return db_last_updated > self.last_updated

    def update_cache(self, data: Dict[str, Any], db_last_updated: float):
        self.cache = data
        self.last_updated = db_last_updated

    def get_cache(self) -> Dict[str, Any]:
        return self.cache

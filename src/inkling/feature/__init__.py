from abc import ABC, abstractmethod
from typing import List, Optional


class FeatureMerger(ABC):
    @abstractmethod
    def get_field_names(self) -> List[str]:
        pass

class SingleFieldMerger(FeatureMerger):
    field_name: Optional[str] = None

class MultiFieldMerger(FeatureMerger):
    field_names: Optional[List[str]] = None

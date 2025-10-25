from typing import Protocol, Dict, Any, runtime_checkable
import pandas as pd

@runtime_checkable
class Source(Protocol):
    def extract(self, df: pd.DataFrame, ds: str, dataset_code: str, run_id: int) -> Dict[str, Any]: ...
    def transform(self, extract_result: Dict[str, Any]) -> Dict[str, Any]: ...
    def load(self, transformed: Dict[str, Any], batch_size: int) -> Dict[str, Any]: ...

REGISTRY: Dict[str, Any] = {}

def register_instance(name: str, instance: Any, *, replace: bool = False) -> None:
    if name in REGISTRY and not replace and REGISTRY[name] is not instance:
        raise RuntimeError(f"Duplicate registry entry: {name}")
    REGISTRY[name] = instance

def get(name: str):
    try:
        return REGISTRY[name]
    except KeyError:
        raise KeyError(f"Source '{name}' not found. Registered: {list(REGISTRY.keys())}")
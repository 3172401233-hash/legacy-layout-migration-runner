"""Legacy bucket scanning, record IO and field transformation."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Callable, Dict, Iterable, List, Mapping, Sequence, Union

from .errors import FieldMappingError

Record = Dict[str, object]
Transform = Callable[[Record], Record]
PartitionFn = Callable[[Record], Mapping[str, str]]


def normalize_path_mapping(
    path_mapping: Union[str, Sequence[str], Mapping[str, str]],
) -> Dict[str, str]:
    """Turn the accepted path-mapping shapes into ``{legacy_root: name}``.

    Accepted shapes:
      * a single legacy root directory,
      * a sequence of legacy root directories,
      * a mapping from legacy root to a stable source name.
    """
    if isinstance(path_mapping, str):
        roots = [path_mapping]
    elif isinstance(path_mapping, Mapping):
        return {os.path.abspath(str(root)): str(name)
                for root, name in path_mapping.items()}
    else:
        roots = list(path_mapping)
    result: Dict[str, str] = {}
    for index, root in enumerate(roots):
        abs_root = os.path.abspath(str(root))
        result[abs_root] = os.path.basename(abs_root.rstrip(os.sep)) or f"source_{index}"
    return result


def read_jsonl(path: str) -> List[Record]:
    """Read a JSONL file, ignoring blank lines."""
    records: List[Record] = []
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            records.append(json.loads(line))
    return records


def write_jsonl(path: str, records: Iterable[Record]) -> None:
    """Atomically write ``records`` as JSONL to ``path``."""
    parent = os.path.dirname(os.path.abspath(path))
    os.makedirs(parent, exist_ok=True)
    tmp = os.path.join(parent, f".{os.path.basename(path)}.tmp")
    with open(tmp, "w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True))
            handle.write("\n")
    os.replace(tmp, path)


def as_transform(field_mapping: Union[Mapping[str, str], Transform]) -> Transform:
    """Build a transform from a ``{target_field: source_field}`` mapping.

    An explicit callable is returned unchanged so tests can inject failing
    transforms; both shapes report failures as :class:`FieldMappingError`.
    """
    if callable(field_mapping):
        def wrapped(record: Record) -> Record:
            try:
                result = field_mapping(record)
            except FieldMappingError:
                raise
            except Exception as exc:  # noqa: BLE001 - normalize every failure
                raise FieldMappingError(str(exc)) from exc
            if not isinstance(result, dict):
                raise FieldMappingError("field transform must return a dict")
            return result

        return wrapped

    def map_fields(record: Record) -> Record:
        result: Record = {}
        for target_field, source_field in field_mapping.items():
            if source_field not in record:
                raise FieldMappingError(
                    f"missing source field {source_field!r} for {target_field!r}"
                )
            result[target_field] = record[source_field]
        return result

    return map_fields


def resolve_partition(partition_fn: PartitionFn, record: Record) -> Dict[str, str]:
    """Call the user partition function and validate its output shape."""
    try:
        partition = dict(partition_fn(record))
    except Exception as exc:  # noqa: BLE001 - surface as a migration failure
        raise FieldMappingError(f"partition function failed: {exc}") from exc
    if not partition:
        raise FieldMappingError("partition function returned an empty partition")
    for key, value in partition.items():
        if not str(key) or not str(value):
            raise FieldMappingError("partition keys and values must be non-empty")
    return partition


def partition_relpath(partition: Mapping[str, str]) -> str:
    return os.path.join(*(f"{key}={value}" for key, value in sorted(partition.items())))


@dataclass(frozen=True)
class LegacyFile:
    """A single legacy bucket file and the source name it belongs to."""

    source_name: str
    source_root: str
    path: str


def scan_legacy_files(path_mapping: Union[str, Sequence[str], Mapping[str, str]]
                      ) -> List[LegacyFile]:
    """Recursively collect legacy JSONL files, sorted for deterministic batches."""
    legacy_files: List[LegacyFile] = []
    for root, source_name in normalize_path_mapping(path_mapping).items():
        if not os.path.isdir(root):
            continue
        for dirpath, _dirnames, filenames in os.walk(root):
            for filename in sorted(filenames):
                if filename.endswith(".jsonl"):
                    legacy_files.append(
                        LegacyFile(source_name, root, os.path.join(dirpath, filename))
                    )
    legacy_files.sort(key=lambda item: (item.source_name, item.path))
    return legacy_files


def batch_filename(batch_id: str) -> str:
    """Name of a batch-owned target file; ownership is recoverable from it."""
    return f"batch-{batch_id}.jsonl"


def batch_id_from_filename(filename: str) -> str:
    stem, ext = os.path.splitext(filename)
    if ext != ".jsonl" or not stem.startswith("batch-"):
        return ""
    return stem[len("batch-"):]

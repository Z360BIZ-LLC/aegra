"""General-purpose object serialization for complex objects"""

import dataclasses
import inspect
from base64 import b64encode
from collections import deque
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from pathlib import PurePath
from typing import Any
from uuid import UUID

from aegra_api.core.serializers.base import SerializationError, Serializer

# Safety net against pathological nesting. Real agent output rarely exceeds a
# few dozen levels; Python's default frame limit is ~1000. 200 is well above any
# legitimate payload and well below the interpreter ceiling, so a cyclic or
# adversarial structure degrades to str() instead of a RecursionError that would
# abort the whole finalize_run transaction.
_MAX_DEPTH = 200


class GeneralSerializer(Serializer):
    """Simple object serializer for complex Python objects"""

    def serialize(self, obj: Any) -> Any:
        """Serialize any object to JSON-compatible format"""
        try:
            return self._serialize_object(obj)
        except Exception as e:
            raise SerializationError(f"Failed to serialize object: {str(e)}", obj.__class__.__name__, e) from e

    def _serialize_object(self, obj: Any, _depth: int = 0) -> Any:
        """Core serialization logic for Python objects"""
        if _depth > _MAX_DEPTH:
            return str(obj)

        # Class objects (e.g. a Pydantic class passed to with_structured_output)
        # carry bound-method descriptors but cannot be dump()'d without an
        # instance. Render them by qualname so duck-typed checks below don't
        # invoke unbound methods.
        if inspect.isclass(obj):
            return f"{obj.__module__}.{obj.__qualname__}"

        # Raw binary, checked before the duck-typed dump paths. Reasoning models
        # surface encrypted chain-of-thought as bytes (Bedrock's
        # `reasoningContent.redactedContent`), which lands nested inside an
        # AIMessage's content blocks. JSONB columns are json.dumps()'d, so an
        # unencoded bytes value fails the write and takes the whole finalize_run
        # transaction with it. base64 keeps it lossless, and bytearray/memoryview
        # are just as unencodable as bytes.
        if isinstance(obj, (bytes, bytearray, memoryview)):
            return b64encode(bytes(obj)).decode("ascii")

        # Handle Pydantic v2 models (model_dump method)
        if hasattr(obj, "model_dump") and callable(obj.model_dump):
            return self._serialize_object(obj.model_dump(), _depth + 1)

        # Handle LangChain objects and Pydantic v1 models (dict method)
        elif hasattr(obj, "dict") and callable(obj.dict):
            return self._serialize_object(obj.dict(), _depth + 1)

        # Handle LangGraph Interrupt objects (they don't have .dict() method)
        elif obj.__class__.__name__ == "Interrupt" and hasattr(obj, "value") and hasattr(obj, "id"):
            return {"value": self._serialize_object(obj.value, _depth + 1), "id": obj.id}

        # Dataclasses (including LangGraph Command) have no standard JSON form.
        # Emit all fields recursively so nested values retain their structure.
        elif dataclasses.is_dataclass(obj):
            return {
                field.name: self._serialize_object(getattr(obj, field.name), _depth + 1)
                for field in dataclasses.fields(obj)
            }

        # Handle NamedTuples (like PregelTask) - they have _asdict() method
        elif hasattr(obj, "_asdict") and callable(obj._asdict):
            return {k: self._serialize_object(v, _depth + 1) for k, v in obj._asdict().items()}

        # Handle common scalar types that JSON does not encode natively.
        elif isinstance(obj, Enum):
            return self._serialize_object(obj.value, _depth + 1)
        elif isinstance(obj, (datetime, date)):
            return obj.isoformat()
        elif isinstance(obj, (UUID, Decimal, PurePath)):
            return str(obj)
        elif isinstance(obj, Exception):
            return {"type": obj.__class__.__name__, "message": str(obj)}

        # Handle array-like containers recursively. Members go through the same
        # coercion as any other value - a set of bytes is no more json-safe than
        # a list of bytes, so this cannot shortcut to list(obj).
        elif isinstance(obj, (set, frozenset, deque, tuple, list)):
            return [self._serialize_object(item, _depth + 1) for item in obj]

        # Handle dictionaries recursively
        elif isinstance(obj, dict):
            return self._serialize_mapping(obj, _depth)

        # Handle basic JSON-serializable types
        elif isinstance(obj, (str, int, float, bool, type(None))):
            return obj

        # Fallback to string representation for unknown types
        else:
            return str(obj)

    def _serialize_mapping_key(self, key: Any, _depth: int = 0) -> str:
        """JSON object keys must be strings; coerce anything else.

        json.dumps raises on e.g. a tuple key, which would abort the write the
        same way an unencoded bytes value does.
        """
        serialized_key = self._serialize_object(key, _depth)
        if isinstance(serialized_key, str):
            return serialized_key
        if isinstance(serialized_key, bool):
            return "true" if serialized_key else "false"
        if serialized_key is None:
            return "null"

        return str(serialized_key)

    def _serialize_mapping(self, mapping: dict[Any, Any], _depth: int = 0) -> dict[str, Any]:
        result: dict[str, Any] = {}
        original_keys: dict[str, Any] = {}

        for key, value in mapping.items():
            serialized_key = self._serialize_mapping_key(key, _depth + 1)
            original_key = original_keys.get(serialized_key)
            if serialized_key in result and original_key != key:
                raise ValueError(f"Mapping keys {original_key!r} and {key!r} serialize to the same key")

            original_keys[serialized_key] = key
            result[serialized_key] = self._serialize_object(value, _depth + 1)

        return result

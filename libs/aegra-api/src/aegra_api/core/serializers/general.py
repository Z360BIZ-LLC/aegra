"""General-purpose object serialization for complex objects"""

import base64
import inspect
from typing import Any

from aegra_api.core.serializers.base import SerializationError, Serializer


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
        if _depth > 200:
            return str(obj)

        # Class objects (e.g. a Pydantic class passed to with_structured_output)
        # carry bound-method descriptors but cannot be dump()'d without an
        # instance. Render them by qualname so duck-typed checks below don't
        # invoke unbound methods.
        if inspect.isclass(obj):
            return f"{obj.__module__}.{obj.__qualname__}"

        # Raw binary. Reasoning models surface encrypted chain-of-thought as
        # bytes (Bedrock's `reasoningContent.redactedContent`), which lands
        # nested inside an AIMessage's content blocks. JSONB columns are
        # json.dumps()'d, so an unencoded bytes value fails the write and takes
        # the whole finalize_run transaction with it. base64 keeps it lossless.
        if isinstance(obj, (bytes, bytearray, memoryview)):
            return base64.b64encode(bytes(obj)).decode("ascii")

        # Handle Pydantic v2 models (model_dump method)
        if hasattr(obj, "model_dump") and callable(obj.model_dump):
            return self._serialize_object(obj.model_dump(), _depth + 1)

        # Handle LangChain objects and Pydantic v1 models (dict method)
        elif hasattr(obj, "dict") and callable(obj.dict):
            return self._serialize_object(obj.dict(), _depth + 1)

        # Handle LangGraph Interrupt objects (they don't have .dict() method)
        elif obj.__class__.__name__ == "Interrupt" and hasattr(obj, "value") and hasattr(obj, "id"):
            return {"value": self._serialize_object(obj.value, _depth + 1), "id": obj.id}

        # Handle NamedTuples (like PregelTask) - they have _asdict() method
        elif hasattr(obj, "_asdict") and callable(obj._asdict):
            return {k: self._serialize_object(v, _depth + 1) for k, v in obj._asdict().items()}

        # Handle sequences and sets. Members go through the same coercion as
        # any other value - a set of bytes is no more json-safe than a list of
        # bytes, so this can no longer shortcut to list(obj).
        elif isinstance(obj, (set, frozenset, tuple, list)):
            return [self._serialize_object(item, _depth + 1) for item in obj]

        # Handle dictionaries recursively
        elif isinstance(obj, dict):
            return {self._serialize_key(k): self._serialize_object(v, _depth + 1) for k, v in obj.items()}

        # Handle basic JSON-serializable types
        elif isinstance(obj, (str, int, float, bool, type(None))):
            return obj

        # Fallback to string representation for unknown types
        else:
            return str(obj)

    @staticmethod
    def _serialize_key(key: Any) -> Any:
        """JSON object keys must be str/int/float/bool/None; coerce anything else.

        json.dumps raises on e.g. a tuple key, which would abort the write the
        same way a bytes value does.
        """
        if isinstance(key, (bytes, bytearray)):
            return base64.b64encode(bytes(key)).decode("ascii")
        if isinstance(key, (str, int, float, bool)) or key is None:
            return key
        return str(key)

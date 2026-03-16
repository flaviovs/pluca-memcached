from __future__ import annotations

import math
import pickle
import time
from collections.abc import Iterable, Mapping
from typing import Any, NoReturn, cast

import pluca
from pymemcache.client.base import Client  # type: ignore[import-untyped]

_THIRTY_DAYS_SECONDS = 60 * 60 * 24 * 30


class MemcachedAdapter:
    """pluca adapter backed by a Memcached server.

    This adapter stores mapped keys in Memcached and is compatible with
    ``pluca.Cache`` semantics, including key-miss behavior and expiration
    handling. Mapped keys are treated as opaque values and may optionally be
    prefixed with a namespace.

    Args:
        endpoint: Memcached server address in ``host:port`` format.
        namespace: Optional prefix applied to all mapped keys. Use ``None`` to
            disable namespacing.
        timeout: Socket operation timeout in seconds. Must be greater than 0.
        connect_timeout: Connection timeout in seconds. Must be greater than 0.
        no_delay: Enables TCP_NODELAY when ``True``.

    Raises:
        ValueError: If constructor configuration is invalid (for example,
            malformed endpoint, out-of-range port, empty namespace, or
            non-positive timeout values).
        pluca.CacheBackendError: For runtime Memcached/client failures raised
            while performing cache operations.

    Concurrency:
        Thread-safe shared-instance use is not guaranteed by this adapter.
        Create independent cache instances per thread unless your deployment
        validates stronger guarantees.

    Notes:
        Memcached TTL behavior applies: relative TTL values up to 30 days are
        used as relative expiration, and larger values are converted to absolute
        Unix timestamps as required by Memcached.

    Lifecycle:
        ``shutdown()`` closes the underlying Memcached client connection. The
        adapter should not be reused after shutdown.
    """

    def __init__(
        self,
        endpoint: str,
        namespace: str | None = None,
        timeout: float = 1.0,
        connect_timeout: float = 1.0,
        no_delay: bool = False,
    ) -> None:
        if not isinstance(endpoint, str) or not endpoint.strip():
            raise ValueError("endpoint must be a non-empty string")
        if namespace == "":
            raise ValueError("namespace must not be empty")
        if timeout <= 0:
            raise ValueError("timeout must be > 0")
        if connect_timeout <= 0:
            raise ValueError("connect_timeout must be > 0")

        host, port = self._parse_endpoint(endpoint)
        self.endpoint = endpoint
        self.namespace = namespace
        self._client = Client(
            (host, port),
            timeout=timeout,
            connect_timeout=connect_timeout,
            no_delay=no_delay,
        )

    @staticmethod
    def _parse_endpoint(endpoint: str) -> tuple[str, int]:
        host, sep, port_text = endpoint.rpartition(":")
        if not sep or not host:
            raise ValueError("endpoint must use host:port format")
        try:
            port = int(port_text)
        except ValueError as ex:
            raise ValueError("endpoint port must be an integer") from ex
        if port <= 0 or port > 65535:
            raise ValueError("endpoint port must be between 1 and 65535")
        return host, port

    @staticmethod
    def _validate_max_age(max_age: float | None) -> float | None:
        if max_age is None:
            return None
        if max_age < 0:
            raise ValueError("max_age must be >= 0")
        return float(max_age)

    @staticmethod
    def _serialize(value: Any) -> bytes:
        return pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)

    @staticmethod
    def _deserialize(payload: bytes) -> Any:
        try:
            return pickle.loads(payload)
        except Exception as ex:
            raise pluca.CacheBackendError("memcached returned invalid payload") from ex

    def _raise_backend_error(self, op: str, ex: Exception) -> NoReturn:
        raise pluca.CacheBackendError(f"memcached {op} failed") from ex

    def _k(self, mkey: Any) -> str:
        base = str(mkey)
        if self.namespace is None:
            return base
        return f"{self.namespace}:{base}"

    def _ttl(self, max_age: float | None) -> int:
        if max_age is None:
            return 0
        if max_age <= _THIRTY_DAYS_SECONDS:
            return max(1, math.ceil(max_age))
        return int(time.time() + max_age)

    def put_mapped(self, mkey: Any, value: Any, max_age: float | None = None) -> None:
        valid_max_age = self._validate_max_age(max_age)
        key = self._k(mkey)

        if valid_max_age == 0:
            try:
                self._client.delete(key, noreply=False)
                return
            except Exception as ex:
                self._raise_backend_error("put", ex)

        payload = self._serialize(value)
        ttl = self._ttl(valid_max_age)
        ok: bool | None = None
        try:
            ok = self._client.set(key, payload, expire=ttl, noreply=False)
        except Exception as ex:
            self._raise_backend_error("put", ex)
        if ok is False:
            raise pluca.CacheBackendError("memcached put failed")

    def get_mapped(self, mkey: Any) -> Any:
        key = self._k(mkey)
        payload: bytes | None = None
        try:
            payload = self._client.get(key)
        except Exception as ex:
            self._raise_backend_error("get", ex)
        if payload is None:
            raise KeyError(mkey)
        return self._deserialize(payload)

    def remove_mapped(self, mkey: Any) -> None:
        key = self._k(mkey)
        removed = False
        try:
            removed = self._client.delete(key, noreply=False)
        except Exception as ex:
            self._raise_backend_error("remove", ex)
        if not removed:
            raise KeyError(mkey)

    def flush(self) -> None:
        try:
            self._client.flush_all(noreply=False)
        except Exception as ex:
            self._raise_backend_error("flush", ex)

    def has_mapped(self, mkey: Any) -> bool:
        key = self._k(mkey)
        payload: bytes | None = None
        try:
            payload = self._client.get(key)
        except Exception as ex:
            self._raise_backend_error("has", ex)
        return payload is not None

    def put_many_mapped(
        self,
        data: Mapping[Any, Any] | Iterable[tuple[Any, Any]],
        max_age: float | None = None,
    ) -> None:
        valid_max_age = self._validate_max_age(max_age)
        items = list(data.items()) if isinstance(data, Mapping) else list(data)
        if not items:
            return

        if valid_max_age == 0:
            keys = [self._k(k) for k, _ in items]
            try:
                self._client.delete_many(keys, noreply=False)
                return
            except Exception as ex:
                self._raise_backend_error("put_many", ex)

        serialized = {self._k(k): self._serialize(v) for k, v in items}
        ttl = self._ttl(valid_max_age)
        failed: list[Any] | None = None
        try:
            failed = self._client.set_many(
                cast(dict[Any, Any], serialized),
                expire=ttl,
                noreply=False,
            )
        except Exception as ex:
            self._raise_backend_error("put_many", ex)

        if failed:
            raise pluca.CacheBackendError("memcached put_many failed")

    def get_many_mapped(
        self, keys: Iterable[Any], default: Any = ...
    ) -> list[tuple[Any, Any]]:
        key_list = list(keys)
        mapped = [self._k(key) for key in key_list]
        if not mapped:
            return []

        values: dict[Any, Any] = {}
        try:
            values = self._client.get_many(mapped)
        except Exception as ex:
            self._raise_backend_error("get_many", ex)

        out: list[tuple[Any, Any]] = []
        for original, mapped_key in zip(key_list, mapped):
            if mapped_key in values and values[mapped_key] is not None:
                out.append((original, self._deserialize(values[mapped_key])))
                continue
            if default is not ...:
                out.append((original, default))
        return out

    def remove_many_mapped(self, keys: Iterable[Any]) -> None:
        mapped = [self._k(key) for key in keys]
        if not mapped:
            return
        try:
            self._client.delete_many(mapped, noreply=False)
        except Exception as ex:
            self._raise_backend_error("remove_many", ex)

    def set_max_age_mapped(self, mkey: Any, max_age: float | None = None) -> None:
        valid_max_age = self._validate_max_age(max_age)
        key = self._k(mkey)

        if valid_max_age == 0:
            if not self.has_mapped(mkey):
                raise KeyError(mkey)
            try:
                self._client.delete(key, noreply=False)
            except Exception as ex:
                self._raise_backend_error("set_max_age", ex)
            return

        ttl = self._ttl(valid_max_age)
        touched = False
        try:
            touched = self._client.touch(key, expire=ttl, noreply=False)
        except Exception as ex:
            self._raise_backend_error("set_max_age", ex)
        if not touched:
            raise KeyError(mkey)

    def gc(self) -> None:
        return None

    def shutdown(self) -> None:
        try:
            self._client.close()
        except Exception as ex:
            self._raise_backend_error("shutdown", ex)

# pluca-memcached

`pluca-memcached` is a memcached adapter for *pluca* built on top of
`pymemcache`.

[pluca](https://pypi.org/project/pluca/) is a Python caching library that
provides a unified cache API with pluggable backend adapters.

## Installation

```bash
pip install pluca-memcached
```

For development tools:

```bash
pip install -e .[dev]
```

## Usage

### Basic example

```python
import pluca
import pluca_memcached

adapter = pluca_memcached.Adapter(endpoint="127.0.0.1:11211", namespace="app")
cache = pluca.Cache(adapter)

cache.put("k", {"ok": True}, max_age=60)
value = cache.get("k")
```

Read *pluca* documentation for full usage.

### Adapter parameters

- `endpoint` (`str`, required): memcached endpoint in `host:port` format.
- `namespace` (`str | None`, optional): key prefix namespace.
- `timeout` (`float`, optional, default `1.0`): socket operation timeout.
- `connect_timeout` (`float`, optional, default `1.0`): connection timeout.
- `no_delay` (`bool`, optional, default `False`): enable TCP_NODELAY.

### Behavior notes

- Thread-safe shared-instance usage is not guaranteed.

## Development

### Run tests

Start memcached first (required for integration tests):

```bash
docker compose up -d
```

Or set a custom endpoint:

```bash
export MEMCACHED_ENDPOINT=127.0.0.1:11211
```

Run tests:

```bash
python -m unittest discover --start-directory tests --buffer --failfast
```

Stop memcached when done:

```bash
docker compose down
```

### Type check and lint

```bash
ruff check && mypy
```

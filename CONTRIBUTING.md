# Contributing to NoRedis

Thanks for your interest in contributing! Here's how to get started.

## Development Setup

```bash
# Clone and install
git clone https://github.com/Ayush-e4/noredis.git
cd noredis
python3 -m venv venv
source venv/bin/activate
pip install -e ".[dev]"
```

## Running Tests

```bash
pytest tests/
```

> **Note:** Tests create temporary `.db`, `.db-wal`, and `.db-shm` files. These are automatically cleaned up by the test fixtures, but if a test crashes mid-run, you can safely delete any leftover `test_*.db*` files.

Some tests involve background workers and use `time.sleep()` for synchronization. The full suite takes ~35 seconds.

## Code Style

This project uses [Ruff](https://docs.astral.sh/ruff/) for linting and formatting:

```bash
ruff check .          # lint
ruff format .         # format
```

Pre-commit hooks are configured — install them once and they'll run automatically on every commit:

```bash
pre-commit install
```

## Submitting Changes

1. Fork the repo and create a feature branch from `main`
2. Write tests for any new functionality
3. Ensure `pytest tests/` passes with all tests green
4. Run `ruff check .` and `ruff format .` before committing
5. Open a Pull Request with a clear description of what changed and why

## Reporting Issues

When reporting a bug, please include:

- Python version (`python3 --version`)
- OS and architecture
- Minimal reproduction script
- Full traceback if applicable

## Design Principles

NoRedis is intentionally minimal. Before proposing a new feature, consider:

- **Does it require zero external dependencies?** NoRedis ships with only the Python standard library.
- **Does it work with a single SQLite file?** No Redis, no Postgres, no network.
- **Is it useful for the 90% use case?** We're not trying to replace Redis for high-throughput distributed systems.

## License

By contributing, you agree that your contributions will be licensed under the [MIT License](LICENSE).

# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-03-30

### Added

- **Cache**: Redis-like key-value cache with TTL, LRU eviction, and namespace support
- **Queue**: Background job queue with priority, delay, retry with exponential backoff, and dead letter queue
- **Scheduler**: Cron-like recurring task scheduler (`every`, `daily`)
- **CLI**: `axiom stats`, `axiom jobs`, `axiom flush`, `axiom retry`, `axiom purge`, `axiom inspect`
- **Database**: WAL-mode SQLite with thread-safe connection pooling and automatic schema creation
- **Watchdog**: Automatic recovery of jobs stuck in `running` state for >5 minutes
- Full test suite (29 tests) covering cache, queue, and scheduler

[0.1.0]: https://github.com/Ayush-e4/axiom/releases/tag/v0.1.0

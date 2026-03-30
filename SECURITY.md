# Security Policy

## Supported Versions

| Version | Supported |
|---------|-----------|
| 0.1.x   | ✅         |

## Reporting a Vulnerability

If you discover a security vulnerability, **please do not open a public issue.**

Instead, email the maintainer directly with details:

- Description of the vulnerability
- Steps to reproduce
- Potential impact

We will respond within 48 hours and work with you to understand and address the issue before any public disclosure.

## Security Considerations

Axiom stores data in a local SQLite file. Keep in mind:

- **The `.db` file is not encrypted.** Do not store secrets (API keys, passwords) in the cache without application-level encryption.
- **SQLite files are local.** Axiom is designed for single-server deployments. Do not expose the database file over a network share.
- **Pickle is used for serialization.** Cache values are serialized with Python's `pickle` module. Only cache data you trust — never load an untrusted `.db` file from an unknown source.

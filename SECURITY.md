# Security Policy

## Supported Versions

| Version | Supported          |
|---------|--------------------|
| 4.x     | :white_check_mark: |
| < 4.0   | :x:                |

## Reporting a Vulnerability

If you discover a security vulnerability in disruptor-rs, please report it
responsibly.

**Do NOT open a public GitHub issue for security vulnerabilities.**

Instead, please email: **itzrustz@gmail.com**

Include:
- A description of the vulnerability
- Steps to reproduce (if applicable)
- The potential impact
- Any suggested fix (if you have one)

We will acknowledge your report within 48 hours and aim to provide a fix
within 7 days for critical issues.

## Scope

This project contains `unsafe` code for performance-critical paths. We take
soundness very seriously and verify correctness through:

- **Miri** (undefined behavior detection)
- **Loom** (concurrency model checking)
- **TLA+** (formal verification of the protocol)

If you believe you have found a soundness issue with any `unsafe` block,
please report it using the process above.

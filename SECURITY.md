# Security Policy

## Supported Versions

Security fixes are backported to the latest minor release on the current major line. Older majors receive fixes only for critical vulnerabilities.

| Version | Supported          |
| ------- | ------------------ |
| 1.x     | :white_check_mark: |
| < 1.0   | :x:                |

## Reporting a Vulnerability

**Do not file a public GitHub issue for security vulnerabilities.**

Report through **[GitHub Security Advisories](https://github.com/mbocevski/valkey-flash/security/advisories/new)** — private disclosure, encrypted in transit, and tracked with automatic CVE coordination if applicable.

Include:
- A clear description of the vulnerability
- Steps to reproduce
- Affected version(s) and configuration
- Any proof-of-concept code

We will acknowledge within 3 business days and provide a disclosure timeline within 7 business days. Fix development runs in a private advisory fork before being merged publicly alongside the security release.

## Coordinated Disclosure

We follow responsible disclosure practice:

1. Reporter files a private advisory.
2. Maintainers confirm the issue and assess severity (CVSS v3.1).
3. Fix is developed in the private advisory.
4. A coordinated release date is agreed with the reporter.
5. Fix is merged, released, and publicly disclosed on the agreed date.
6. Reporter is credited in the release notes unless they request anonymity.

## Scope

In-scope:
- Remote code execution, memory corruption, or crashes triggered via the Valkey wire protocol
- Privilege escalation (ACL bypass, sandbox escape)
- Data integrity violations (data loss beyond documented bounded-loss guarantees, silent data corruption)
- Denial of service requiring unauthenticated or minimal-privilege access
- Secrets disclosure (key material, config, logs)

Out of scope:
- Attacks requiring root or `CAP_SYS_RAWIO` / raw NVMe device access
- Social engineering, physical attacks, supply-chain attacks on build dependencies (report those upstream)
- Issues in dependencies (report to the upstream project; we'll backport once upstream ships a fix)
- Denial-of-service solely via resource exhaustion by an authenticated administrator

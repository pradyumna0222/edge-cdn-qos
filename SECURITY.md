# Security Policy

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| 1.0.x   | :white_check_mark: |

## Reporting a Vulnerability

If you discover a security vulnerability within this project, please report it privately:

- **Primary Contact:** [ramadurgampradyumna@gmail.com](mailto:ramadurgampradyumna@gmail.com)
- **GitHub:** Submit via [GitHub Private Vulnerability Reporting](https://github.com/pradyumna0222/edge-cdn-qos/security/advisories/new) or contact the maintainers directly.

### What to Include
1. A summary of the vulnerability and attack vector.
2. Step-by-step instructions or proof-of-concept (PoC) to reproduce the issue.
3. Potential impact on edge caching or origin server operations.

We will acknowledge receipt within 48 hours and work with you to patch and coordinate responsible disclosure.

## Security Architecture & Best Practices

- **Binding & Exposure:** The default implementation binds to localhost (`127.0.0.1`). If exposing edge nodes or gateways on external networks, enforce HTTPS/TLS termination through a hardened reverse proxy.
- **Cache Isolation:** Edge cache directories (`cache/edge1`, `edge2`, `edge3`) maintain filesystem isolation to prevent directory traversal vulnerabilities.
- **Upstream Protection:** The QoS Router enforces request timeouts (`PROBE_TIMEOUT_SECONDS = 0.6s`, `EDGE_TIMEOUT = 1.5s`) to mitigate slowloris and origin resource exhaustion.

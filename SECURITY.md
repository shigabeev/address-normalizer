# Security

Version `2.0.0` is currently supported.

Please report vulnerabilities through
[GitHub private vulnerability reporting](https://github.com/shigabeev/address-normalizer/security/advisories/new).
Do not include private addresses, credentials, or customer data in a public
issue.

Useful reports include the affected version, a minimal synthetic reproduction,
impact, and any suggested mitigation.

The package is designed to run offline with no runtime dependencies. A network
request, hidden download, filesystem write, or process launch during parsing is
a security bug. Incorrect parsing is usually a correctness issue unless it
crosses a trust boundary or causes unsafe authorization, routing, or disclosure.

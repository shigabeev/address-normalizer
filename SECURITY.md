# Security policy

Please report suspected vulnerabilities privately through GitHub's security
advisory feature rather than a public issue.

The parser is intended to run without network access, secrets, or filesystem
writes. Treat changes to packaging, model loading, training data, release
automation, and GitHub Actions as security-sensitive.

Pull-request workflows must use an unprivileged `pull_request` context. Do not
check out or execute contributor code from a privileged `pull_request_target`
workflow.

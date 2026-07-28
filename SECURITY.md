# Security policy

## Supported versions

Version 2 is currently an unreleased alpha. Security fixes are prepared on the
active v2 branch; there is no supported PyPI release or security maintenance
window yet. The historical v1 Elasticsearch application is archived and is not
supported as a deployed service.

This policy will name supported release lines when the first public v2 release
exists.

## Report privately

Do not open a public issue for a suspected vulnerability or include private
addresses, credentials, tokens, or exploit details in public artifacts.

Use GitHub private vulnerability reporting for this repository if the
**Report a vulnerability** option is available under the Security tab. Include:

- affected version, commit, and installation method;
- impact and realistic attack conditions;
- minimal reproduction or proof of concept;
- whether secrets, filesystem access, network access, or untrusted package/model
  data are involved;
- a safe way to contact you.

If private vulnerability reporting is unavailable, use the
[security-contact request](https://github.com/shigabeev/address-normalizer/issues/new?template=security-contact.yml).
It asks only for a private channel and must contain no vulnerability details.
A maintainer can then arrange a private channel. This is a routing fallback,
not a place to disclose the vulnerability.

The project cannot promise a response SLA before a maintainer security contact
and release process are formally established. The reporter should expect an
acknowledgment, impact assessment, coordinated fix, and disclosure timing to be
agreed before publication.

## Security boundaries

The v2 runtime is intended to:

- parse caller-provided text without network access;
- make no filesystem writes during parsing;
- load only its bundled small model;
- require no runtime dependency;
- preserve rather than execute unparsed input.

Changes to packaging, resource loading, training/evaluation data, generated
models, GitHub Actions, build provenance, and release credentials are
security-sensitive. Pull-request workflows must remain unprivileged and must
not execute contributor code in a privileged `pull_request_target` context.

Incorrect address extraction is normally a correctness issue, not a
vulnerability. Treat it as security-sensitive when it crosses a trust boundary
or can lead to authorization bypass, unsafe file/network access, secret
exposure, code execution, or a practical denial of service. Otherwise use the
parsing-failure template and redact personal data.

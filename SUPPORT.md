# Support

`address-normalizer` v2 is an alpha maintained on a best-effort basis. There is
no paid support channel or guaranteed response time.

Before opening an issue:

1. read the README troubleshooting and product-boundary sections;
2. reproduce on the current v2 commit;
3. reduce address data to a synthetic or safely redacted example;
4. include package version, Python version, installation source, command, full
   output, and expected behavior.

Use the matching GitHub issue form for bugs, parsing failures, feature requests,
or data provenance. Public issues are the support record; do not send private
addresses, production logs, credentials, or large corpora.

The project can help explain:

- documented Python and CLI behavior;
- reproducible installation or packaging failures;
- unexpected parsing fields, offsets, warnings, and alternatives;
- evaluation commands and committed metric definitions;
- whether a proposed feature fits the small offline parser boundary.

The project cannot provide:

- a current FIAS/GAR database, identifier, or verification result;
- support for a customer-managed resolver, search cluster, or geocoder;
- legal advice about address data or licensing;
- private application debugging or integration consulting;
- guarantees that an alpha result is suitable for an automated business
  decision.

Security-sensitive reports follow [`SECURITY.md`](SECURITY.md) and must not be
disclosed in a public issue.

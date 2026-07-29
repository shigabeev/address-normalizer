# Suggested repository settings

These are maintainer-facing settings for the GPL-3.0-only v2 package.

## About section

**Description**

> Small offline Python parser for unstructured Russian addresses, with typed
> fields, source offsets, warnings, and no bundled FIAS/GAR database.

**Website**

Leave empty until a maintained documentation or package page exists.

**Topics**

```text
address-parsing
python
russian
nlp
offline
fias
gar
data-quality
```

`fias` and `gar` describe the downstream integration boundary, not bundled
registry data or identifier lookup. Do not add `geocoding`, `address-validation`,
`production-ready`, or an accuracy claim.

## Community settings

- Enable Issues and the issue-form chooser.
- Enable private vulnerability reporting before announcing a release.
- Keep blank issues disabled while the structured forms are new.
- Use Discussions only if a maintainer is prepared to moderate and answer them.
- Do not enable automatic deletion of branches or merge methods without first
  checking the release and backport process.

## Branch and review settings

For the release branch, require:

- pull requests and at least one maintainer review;
- approval from code owners for package data, training, workflows, and release
  metadata;
- passing required checks;
- resolution of review conversations;
- no force pushes or branch deletion.

Keep pull-request workflows unprivileged. Do not use `pull_request_target` to
check out or execute contributor code.

## Labels

Create the labels in [`docs/triage.md`](../docs/triage.md) manually or with an
explicitly reviewed maintainer script. No GitHub API calls have been made for
these suggestions.

## Release state

The license scope and data/model provenance decisions are recorded in
`LICENSING.md`. Publishing still requires the tagged build, artifact, and
protected-environment checks in `docs/releasing.md`.

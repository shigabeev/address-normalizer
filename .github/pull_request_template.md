> **Licensing pause:** substantive reusable code cannot be accepted until the
> maintainer records the v2 licensing scope. Issue reports, reproductions,
> provenance information, and non-substantive documentation corrections remain
> welcome. See `LICENSING.md`.

## Linked issue or reproduced bug

Closes #

<!-- If there is no issue, give exact reproduction steps and explain why this
small change should be reviewed without one. -->

## Behavior and maintenance impact

<!-- Explain, in your own words:
- what happens before this change;
- what happens after this change;
- why this approach fits the package boundary;
- what future maintainers will need to know.
-->

## Evidence

<!-- Include exact commands and output. For parsing changes, include input,
before/after fields, warnings, alternatives, unparsed content, and offsets. -->

- Test that fails before and passes after:
- Focused test result:
- Full `pytest` result:
- Other checks:

## Benchmark and artifact impact

<!-- Required for parser, model, evaluation, packaging, or performance changes.
Keep historical, noisy-window, nationwide-clean, and official-registry domains
separate. Write "not applicable" with a reason when this section does not apply.
-->

- Before/after metrics by relevant domain and field:
- Known regressions:
- Model-size delta:
- Wheel-size delta:
- Data source, version, license/terms, and checksum:

## Author verification

- [ ] This change is linked to an issue or includes a complete reproduced bug.
- [ ] I added a test that fails before the change and passes afterward, or
      explained why no test applies.
- [ ] I ran the focused tests and the complete local suite and reported exact
      results above.
- [ ] Original source offsets, ambiguity, warnings, alternatives, and unparsed
      evidence are preserved where relevant.
- [ ] I did not add a runtime dependency, network call, hidden download,
      FIAS/GAR data, or service requirement.
- [ ] I checked relevant benchmark domains rather than optimizing only one
      aggregate score.
- [ ] I understand every submitted change, including automated or AI-assisted
      portions, and can explain and maintain it.
- [ ] I have the right to submit every code, data, model, and documentation
      artifact in this pull request.
- [ ] No secrets, private addresses, production logs, caches, or large external
      corpora are included.

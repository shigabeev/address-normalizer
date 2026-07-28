# Licensing status

No license currently applies to this repository.

The public GitHub metadata reports no detected license, and the historical tree
contains no `LICENSE`, `LICENCE`, or `COPYING` file. Publishing source code and
granting permission to view or share it does not by itself select an open-source
license.

The history also contains merged contributions from authors other than the
repository owner. Adding one repository-wide license now could therefore imply
authority over historical contributions that has not been documented.

## Recommended resolution

Treat the code in `src/address_normalizer`, `tests_v2`, `training`, and
`evaluation` as the new v2 work. Keep the historical root implementation
explicitly marked as legacy.

The owner should choose one of these paths:

1. **Apache-2.0 for v2 only (recommended for enterprise adoption).** It is
   permissive and includes an explicit patent grant. Add a v2 license file,
   reference it from `pyproject.toml`, and state clearly which directories it
   covers.
2. **MIT for v2 only.** It is shorter and familiar, but has no explicit patent
   grant.
3. **One license for the whole repository.** Do this only after confirming the
   bank permission and obtaining any consent needed for merged third-party
   contributions.

Do not publish the PyPI package or invite substantive reusable contributions
until the owner records the choice. Also confirm that the historical reference
workbook may be used to redistribute the derived compact model. This document is
a repository audit, not legal advice.

# Failure analysis

This note reviews the current 500-row historical parsing regression and the
complete-message detection diagnostic. It separates parser behavior, ambiguous
input, and questionable reference expectations instead of treating every
mismatch as the same kind of model error.

## Historical parsing regression

Current result:

- 500 rows;
- 402 exact rows and 98 non-exact rows;
- 80.4% exact-address rate;
- 95.8538% exact component-value micro F1;
- 76.8% of rows with no unparsed word or number span.

`legacy_reference_500_diagnostics.csv` contains more than 75 columns for every
row. `legacy_reference_500_failure_summary.json` contains the complete primary
and secondary cause counts.

The primary heuristic cause partitions the 98 non-exact rows:

| Primary likely cause | Rows |
| --- | ---: |
| Conflicting street markers | 22 |
| Reference infers a street type absent from raw text | 20 |
| Ambiguous or unsupported abbreviation | 15 |
| Unmarked numeric-role ambiguity | 14 |
| Compound/letter-number boundary | 7 |
| Administrative label or boundary | 6 |
| Street-type recognition | 4 |
| Reference conflicts with explicit numeric marker | 3 |
| Numeric component not recognized | 3 |
| Four single-row causes | 4 |

The primary assignment is a deterministic triage aid. Some rows have secondary
causes, and only a reviewer with authority over the reference schema can
adjudicate whether the parser or expected value should change.

## Ten representative failures

These ten were checked against the raw string, expected fields, current output,
and unparsed evidence. This is a behavior review, not verification against an
authoritative address registry.

| ID | Narrow scenario | Observed mismatch | Reason |
| --- | --- | --- | --- |
| `legacy-good-0005` | `ул.Рязанский проспект` | Expected `пр-кт`, actual `ул` | Two explicit street markers compete. The parser selects the first and leaves `проспект` unparsed. This needs a marker-precedence or ambiguity policy. |
| `legacy-good-0359` | `Батайский проезд, 17, 294` | House becomes `294`; apartment missing | Both numeric roles are unmarked. Rightmost-tail logic selects the last number as house, leaving `17` unparsed. The input cannot be resolved safely without a numeric-role convention or alternative. |
| `legacy-good-0165` | `пр.Ленинского Комсомола` | Expected `пр-кт`, actual type missing | `пр.` is ambiguous between `проспект` and `проезд`; the current grammar does not silently choose. The expected value makes a choice not recoverable from the abbreviation alone. |
| `legacy-good-0751` | `ул.Ореховый бул.` | Street includes `Бул`; type is `ул` instead of `б-р` | Conflicting markers cause first-marker selection and a street boundary error. Both the type decision and value boundary need review. |
| `legacy-good-0089` | `Михайлово-Ярцевское п, Исаково д` | District missing; settlement receives district value | Suffix one-letter administrative markers collide with the parser's prefix-oriented marker grammar, shifting the component label and leaving `Исаково д` unparsed. |
| `legacy-good-0795` | `Ордынка Б., ... с.1` | Punctuation differs; type and structure missing | The reference supplies an implicit `ул`, while `с.` is an ambiguous unsupported short form. The remaining street difference is punctuation-only. This row mixes reference inference, abbreviation policy, and normalization. |
| `legacy-good-0006` | `г. Москва Денисовский переулок` | City absorbs street text; street becomes `Переулок` | A missing separator between city and street causes boundary merging before the suffix street marker is interpreted. |
| `legacy-good-0935` | `Котляковская, 4` | Expected `ул`, actual type missing | No street-type marker exists in the raw string. Scoring an inferred `ул` as an extraction error is a reference-policy issue and should be reviewed before changing the parser. |
| `legacy-good-0029` | `ул. Одоевского` | Expected `пр-д`, actual `ул` | The raw text explicitly says `ул`, while the reference expects `проезд`. The parser follows the source; this is a direct source/reference conflict. |
| `legacy-good-0036` | `стр. 4А` | Expected house `4А`; actual structure `4А` | The raw marker explicitly says `строение`, while the reference places the value in `house_num`. The parser follows the marker; the expected schema needs adjudication. |

At least 24 rows should receive reference review before parser optimization:
20 inferred-but-absent street types, one explicit street-type conflict, and
three explicit numeric-marker conflicts. Fixing the parser to match these rows
without adjudication would increase the score while making source-faithful
extraction worse.

## Complete-message address detection

`evaluate_redmadrobot_detection.py` reconstructs all 2,841 source messages and
defines a gold address window only when a location cluster contains both
`STREET` and `HOUSE`. There are 144 such gold spans in 135 messages; the other
2,706 messages are annotated negatives for this narrow detection definition.

Current development result:

| Metric | Result |
| --- | ---: |
| Any-overlap precision | 98.0% |
| Any-overlap recall | 68.1% |
| Any-overlap F1 | 80.3% |
| Exact-boundary F1 | 23.8% |
| Negative-message specificity | 100.0% |

The 107 non-exact messages carry explicit overlapping reason tags:

| Detection failure tag | Messages |
| --- | ---: |
| Missed address | 44 |
| Span includes context outside gold | 33 |
| Span drops gold text | 33 |
| Spurious address | 1 |

The main missed-address scenarios are markerless or transliterated addresses,
reversed component order, unusual abbreviations, and gold spans without the
strong street/building evidence required by the conservative detector. Boundary
differences commonly involve optional city, postcode, country, company/person
text, or trailing unit fields.

The RedMadRobot detector failures were inspected while developing the current
algorithm. This report is therefore a development diagnostic and must not be
described as untouched final-test performance. A replacement final set needs
complete representative messages, hard negatives, grouped entities, and an
explicit boundary policy.

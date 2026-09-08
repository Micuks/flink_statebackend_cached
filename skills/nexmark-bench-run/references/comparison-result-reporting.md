# Nexmark comparison result reporting

Read this reference whenever reporting a RocksDB/baseline versus
CacheKit/FullOpt/other optimization campaign.

## Identity before results

State the platform, event count, query set, checkpoint policy, topology, metric
unit, baseline name and commit/config, optimization name and commit/config, and
the raw artifact path. Keep identity outside the table so the table remains
readable.

The metric unit must be explicit:

- Use `K/s/core` only when the CPU process-tree and capacity gates pass.
- Otherwise use raw `K/s` and state that per-core results are invalid. Never put
  invalid CPU-normalized values into the required table.

## Required table

Use one table per platform and compatible baseline/protocol, with all its
optimization variants side by side. Queries are rows. RocksDB is the first
numeric column; each optimization's value column is immediately followed by its
uplift column, with all uplifts relative to that RocksDB baseline, not the previous
optimization. Do not group all values first and all uplifts later, or transpose
queries into columns. Render an actual table, not prose or fenced Markdown.

| Query/Group | RocksDB | Java | Java uplift | Native | Native uplift |
|---|---:|---:|---:|---:|---:|
| q4 | value | value | percentage | value | percentage |
| q5 | value | value | percentage | value | percentage |
| q8 | value | value | percentage | value | percentage |
| q9 | value | value | percentage | value | percentage |
| q11 | value | value | percentage | value | percentage |
| ... | ... | ... | ... | ... | ... |
| 大状态 8Q | mean value | mean value | mean query uplift | mean value | mean query uplift |
| 小状态 7Q | mean value | mean value | mean query uplift | mean value | mean query uplift |
| 15Q | mean value | mean value | mean query uplift | mean value | mean query uplift |

Java/Native are examples: use the actual optimization names and append further
`Opt value | Opt uplift` pairs in the requested order. If the experiment has a
non-RocksDB baseline, use its actual name in the same first-numeric position.
Do not mix incompatible baselines in one shared baseline column.

For R1-only results, show R1 values. For multi-round results, show each query's
arithmetic mean raw value over the declared valid rounds and the mean of paired
round uplifts (formulas below); explicitly state round counts. Keep per-round raw
values and formulas in detail tables/sheets, using the same baseline-first,
value/uplift-pair layout. This replaces the old fixed R1/R2/R3 main-column layout.

Show raw throughput values to exactly two decimals and uplift to two decimal
places with a percent sign. Preserve negative values.

Use this same orientation and column order in chat, Feishu and XLSX. For Feishu,
use the native root-level table component, not column_set or Markdown pipe-table
source. Keep numeric boundaries and real line breaks.

## Query groups

The default 15Q ordering and frozen large/small-state taxonomy are:

- 大状态 8Q: `q4,q5,q8,q9,q11,q18,q19,q20`
- 小状态 7Q: `q3,q7,q12,q13,q15,q16,q17`
- 15Q (all15q): the union of the two groups above

List queries in the large-state then small-state order above. Always append the
three summary rows in that order, including in partial reports. Their uplift
cells contain arithmetic means of the member-query uplifts, not a ratio of mean
throughputs. Any explicitly requested additional group belongs before these final
three rows.

Only add `vstate14q` when requested or defined by the campaign; it is not a
mandatory default row. It is an experiment-identity group, not inferred from its name. The
campaign identity must list its exact 14 query IDs. Assert that the list contains
14 unique members drawn from all15q before calculating or reporting it. If the
identity does not define the members, show `N/A (vstate14q membership missing)`;
do not substitute the historical ValueState 11Q group or guess which query to
exclude.

If a campaign intentionally uses a different large/small taxonomy, show that
query list in the identity and label it non-default rather than silently changing
the group.

## Formulas

Pair baseline and optimization by the same query and the same round.

For query `q` and round `r`:

```text
uplift(q,r) = opt(q,r) / baseline(q,r) - 1
```

The table's query-level `Uplift` is the arithmetic mean of the paired round
uplifts, not the ratio of the cross-round throughput means:

```text
uplift(q) = arithmetic_mean_r(uplift(q,r))
```

For group `G`, each round's displayed baseline and optimization values are the
arithmetic means of member-query raw values for that round. The group `Uplift`
is the arithmetic mean of member-query uplifts:

```text
baseline(G,r) = arithmetic_mean_q_in_G(baseline(q,r))
opt(G,r)      = arithmetic_mean_q_in_G(opt(q,r))
uplift(G)     = arithmetic_mean_q_in_G(uplift(q))
```

Do not derive group uplift from `opt(G)/baseline(G)`; that weights queries by
their baseline throughput and changes the agreed reporting statistic. Do not use
a geometric mean, throughput weighting, or pool query-round cells in a way that
gives queries with more valid rounds greater weight: average rounds within each
query first, then average queries equally. Use full stored precision before
formatting the final percentages.

## Missing, extra, or invalid rounds

- A missing R1/R2/R3 cell is `—`, never zero.
- Do not call an R1-only or R1-R2 result a three-round result.
- Calculate a query uplift only from valid paired rounds and show the paired
  round count. A campaign-level `all15q` claim requires the campaign's declared
  completeness gate; otherwise label it partial.
- Keep all three group rows in partial reports. A partial group mean may use only
  valid member-query uplifts, but label each affected Opt's result `partial n/N`
  (in the cell or an adjacent note/count sheet); Java and Native can have different
  counts. Never silently call 13/15 a complete 15Q mean. Exclude missing, invalid,
  and treatment-inapplicable entries rather than replacing them with zero; report
  no-request-path rows explicitly and do not treat them as proof of activation.
  If no member is valid, show `N/A`. Show group raw means only for a clearly
  specified comparable query set; otherwise leave those raw cells as `—`.
- Never pair baseline R1 with optimization R2, reuse one baseline round across
  several optimization rounds without disclosure, or mix campaigns silently.
- For any number of rounds, retain the baseline-first, Opt/uplift-pair main
  layout and keep round-specific data in clearly labeled detail tables/sheets.

## Spreadsheet delivery

When delivering XLSX/ODS, write the per-round and uplift calculations as cell
formulas. Put experiment identity, commits, configs, artifact paths, and group
membership above the result table. Use one sheet per experiment identity and
keep the same column order as the rendered table. Include the three terminal
group rows and valid query/round counts. In a single-round Java/Native example,
query uplift formulas are `=C10/B10-1` and `=E10/B10-1`; a complete front8 summary
uses `=AVERAGE(D10:D17)` and `=AVERAGE(F10:F17)`, not ratios of the raw group means.

# Case-Study Dataset — Causal Decomposition of the Indic Gap

500 stratified tasks, aligned across 7 language variants, for the oracle-ablation
study that decomposes the English→Indic execution-accuracy gap into its causes.

Regenerate with:

```bash
python3 case_studies/build_case_study_dataset.py -n 500 --seed 42 --require-literals
```

## Sampling

- **Eligible pool**: tasks that exist in *all six* core languages (English +
  Hindi, Bengali, Tamil, Telugu, Marathi), drawn from `output/<DB>/sampled_tasks/`,
  **and** whose gold SQL contains at least one string literal (`--require-literals`).
  1,032 tasks satisfy the language constraint; 725 of those also have a literal.
  Restricting to the literal-bearing pool means all 500 sampled tasks are usable
  for condition C (oracle value grounding) — see "Two caveats" below for why this
  matters. Run without `--require-literals` to sample from the full 1,032-task
  pool instead (that build has ~29% zero-literal tasks).
- **Excluded**: `INDIA_UDISE_Right_To_Education_RTE_and_School_Management_data`
  has no Hindi or Hinglish translations, so it cannot enter a paired design.
  11 of 12 candidate DBs remain.
- **Stratification**: DB × difficulty, proportional allocation with
  largest-remainder rounding. Within a stratum, Hinglish-available tasks are
  preferred so the optional Hinglish arm stays as complete as possible.
- **Seed**: 42.

| Split | Counts |
| --- | --- |
| Databases | 11 (20–64 tasks each, proportional to each DB's literal-bearing pool) |
| Difficulty | easy 137 · medium 194 · hard 169 |
| Hinglish coverage | 461 / 500 |
| Literals per task | mean 1.50, max 6, min 1 |

Per-DB counts shifted from the unfiltered build because DBs differ in what
fraction of their tasks carry a literal (e.g. `INDIA_HMIS_Sub_District_Report_*`
is ~90% literal-bearing and now contributes more tasks; `INDIA_Economic_Census_Firms`
is ~66% and contributes fewer). Difficulty stratification is preserved because the
literal-bearing pool's difficulty mix (200/280/245 easy/medium/hard) is close to
the full pool's.

## Files

| File | Rows | Role |
| --- | ---: | --- |
| `tasks_english.jsonl` | 500 | Reference arm — baseline already computed |
| `tasks_hindi.jsonl` | 500 | Treatment arm |
| `tasks_bengali.jsonl` | 500 | Treatment arm |
| `tasks_tamil.jsonl` | 500 | Treatment arm |
| `tasks_telugu.jsonl` | 500 | Treatment arm |
| `tasks_marathi.jsonl` | 500 | Treatment arm |
| `tasks_hinglish.jsonl` | 452 | Optional arm (code-switching) |
| `manifest.json` | — | Provenance, per-stratum counts, source timestamps |

The six core files are **row-order identical** and share `pair_id` 1:1, so paired
statistics (e.g. McNemar on per-item flips) work by zipping files directly.
`tasks_hinglish.jsonl` is a strict subset in the same relative order.

`gold_sql` is byte-identical across all languages for a given `pair_id` — only
`question` is translated. Verified at build time (0 mismatches).

## Record schema

```json
{
  "pair_id": "a5ed7951",
  "db_id": "INDIA_NWMP_Water_Quality_Data",
  "language": "hindi",
  "difficulty": "easy",
  "question": "असम राज्य में स्थित सभी स्टेशनों के लिए ...",
  "gold_sql": "SELECT T1.STATION_CODE, ... WHERE T2.STATE = 'Assam';",
  "schema_used": ["DIM_STATE", "DIM_STATION", "FACT_COLIFORMS"],
  "gold_literals": ["Assam"],
  "has_hinglish": true,
  "source_stem": "20260311_153252"
}
```

`schema_used` and `gold_literals` are precomputed so the experiment runner does
no parsing of its own.

## Experimental conditions

| Condition | Question | Schema block | Literal hint |
| --- | --- | --- | --- |
| Indic baseline | Indic | full DDL | — |
| English reference | English | full DDL | — |
| **B** oracle schema linking | Indic | pruned to `schema_used` | — |
| **C** oracle value grounding | Indic | full DDL | `gold_literals` |
| **B+C** | Indic | pruned | `gold_literals` |
| **MT** translate-then-parse | machine-translated English | full DDL | — |

Both baselines already exist in `output/metric_summaries/`; only B, C, B+C and MT
need new inference.

## One caveat that affects analysis

**Condition C leaks weakly.** Supplying `'Assam'` also hints that *a filter
exists*. This is not fully removable, so treat C as an **upper bound** on the
contribution of value grounding. That framing is what makes it useful as the
oracle ceiling for a transliteration-aware value linker.

(Earlier builds of this dataset, sampled without `--require-literals`, had 146/500
zero-literal tasks — pure projection/join/aggregation queries with no value filter
— which diluted condition C by ~29%. The current build restricts the eligible pool
to literal-bearing tasks, so all 500 rows are usable for condition C directly, no
subsetting needed.)

`gold_literals` is sorted alphabetically, not in SQL order, so the hint does not
leak clause ordering. Unquoted numerics (e.g. `828.55`) are excluded on purpose:
bare numbers survive translation intact and are not part of the value-grounding
problem. Quoted years (e.g. `'2013'`) are included because they are stored as
strings.

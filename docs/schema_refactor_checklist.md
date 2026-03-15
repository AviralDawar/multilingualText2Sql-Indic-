# Schema Refactor Checklist (Reusable)

Use this checklist for any database schema refactor in this repository.

## A. Baseline and Contract

- [ ] Freeze baseline artifacts (current `schema_config.yaml`, `DDL.csv`, sample JSONs, and row counts).
- [ ] Define/verify canonical schema contract in `schema_config.yaml`:
  - [ ] Primary keys (`key_column`)
  - [ ] Foreign keys (`foreign_keys`)
  - [ ] Natural keys/transforms if needed (`key_source_index`, `key_transform`)
  - [ ] Dedup rules (`dedup_columns`)

## B. Artifact Regeneration

- [ ] Regenerate normalized table files and sample JSONs from `total_data.csv` using `scripts/generic_split.py`.
- [ ] Regenerate `DDL.csv` from canonical schema metadata.
- [ ] Cross-check YAML vs DDL FK contract:
  - [ ] Same FK edge count
  - [ ] No missing or extra FK relations

## C. Database Sync and Backfill

- [ ] Sync live Postgres schema from YAML FK contract using `scripts/sync_postgres_schema_from_yaml.py`.
- [ ] Decide reload strategy:
  - [ ] If tables are empty: load directly.
  - [ ] If tables are non-empty: explicitly confirm truncate/reload strategy.
- [ ] Backfill data in FK-safe order:
  - [ ] Dimension tables first
  - [ ] Fact/bridge tables after parents

## D. Integrity Validation

- [ ] Validate key integrity:
  - [ ] PK uniqueness
  - [ ] Key null checks
- [ ] Validate FK integrity:
  - [ ] Orphan checks for every FK
- [ ] Validate logical graph:
  - [ ] Expected connected components
  - [ ] No invalid FK target columns

## E. Task Generation Readiness

- [ ] Ensure DSQG schema loaders consume true FK mapping (no hardcoded `<TABLE>_ID` assumptions).
- [ ] Enforce FK-valid join guidance in SQL generation prompts.
- [ ] Regenerate Text2SQL tasks after schema refactor.
- [ ] Audit generated tasks for:
  - [ ] Non-FK joins
  - [ ] Duplicates
  - [ ] Weak/noisy query patterns

## F. Freeze Outputs

- [ ] Freeze final artifacts:
  - [ ] `schema_config.yaml`
  - [ ] `DDL.csv`
  - [ ] sample JSONs and `data/*.csv`
  - [ ] migration/sync logs
  - [ ] generated task files

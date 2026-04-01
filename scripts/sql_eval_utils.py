#!/usr/bin/env python3
"""
Utilities for SQL exact-match evaluation.

The previous EM implementation compared a handful of clause buckets. That can
overcount matches because it ignores important structure such as HAVING, LIMIT,
CTEs, join shape, and nested queries. This module instead canonicalizes the SQL
AST and compares normalized SQL strings.
"""

from __future__ import annotations

import re
from typing import Optional


def _fallback_normalize_sql(sql: str) -> str:
    """Best-effort normalization if sqlglot parsing fails."""
    sql = (sql or "").strip()
    sql = re.sub(r"\s+", " ", sql)
    sql = sql.rstrip(";")
    sql = sql.replace('"', "")
    return sql.lower()


def _flatten_boolean(node, op_type):
    if isinstance(node, op_type):
        return _flatten_boolean(node.this, op_type) + _flatten_boolean(node.expression, op_type)
    return [node]


def _canonicalize_aliases(ast, exp) -> None:
    """
    Rename aliases deterministically so alias spelling differences do not affect EM.

    This is intentionally simple and global rather than fully scope-aware. It is
    still materially safer than stripping all alias information entirely, and it
    also normalizes aliased vs unaliased table references.
    """
    alias_map = {}
    cte_index = 1
    table_index = 1

    for cte in ast.find_all(exp.CTE):
        alias = cte.args.get("alias")
        if not alias or not getattr(alias, "this", None):
            continue
        alias_name = getattr(alias.this, "name", None)
        if not alias_name:
            continue
        alias_name = str(alias_name).lower()
        canonical_name = alias_map.get(alias_name)
        if canonical_name is None:
            canonical_name = f"cte{cte_index}"
            alias_map[alias_name] = canonical_name
            cte_index += 1
        alias.this.set("this", canonical_name)
        alias.this.set("quoted", False)

    for table in ast.find_all(exp.Table):
        table_name = str(table.name).lower() if table.name else ""
        alias = table.args.get("alias")
        alias_name = getattr(getattr(alias, "this", None), "name", None)
        alias_name = str(alias_name).lower() if alias_name else ""

        # References to canonicalized CTEs.
        if table_name in alias_map and not alias_name:
            table.set("this", exp.to_identifier(alias_map[table_name]))
            continue

        reference_name = alias_name or table_name
        if not reference_name:
            continue

        canonical_alias = alias_map.get(reference_name)
        if canonical_alias is None:
            canonical_alias = f"t{table_index}"
            alias_map[reference_name] = canonical_alias
            table_index += 1

        if alias and getattr(alias, "this", None):
            alias.this.set("this", canonical_alias)
            alias.this.set("quoted", False)
        else:
            table.set("alias", exp.TableAlias(this=exp.to_identifier(canonical_alias)))

    for column in ast.find_all(exp.Column):
        table_name = column.table
        if table_name and table_name.lower() in alias_map:
            column.set("table", exp.to_identifier(alias_map[table_name.lower()]))


def _canonicalize_group_by(ast) -> None:
    group = ast.args.get("group")
    if not group or not getattr(group, "expressions", None):
        return
    group.set(
        "expressions",
        sorted(group.expressions, key=lambda e: e.sql(dialect="postgres", pretty=False)),
    )


def _canonicalize_order_defaults(ast, exp) -> None:
    for ordered in ast.find_all(exp.Ordered):
        desc = ordered.args.get("desc")
        nulls_first = ordered.args.get("nulls_first")

        if desc is False:
            ordered.set("desc", None)
            desc = None

        # PostgreSQL defaults:
        # ASC => NULLS LAST
        # DESC => NULLS FIRST
        if desc in (None, False) and nulls_first is False:
            ordered.set("nulls_first", None)
        elif desc is True and nulls_first is True:
            ordered.set("nulls_first", None)


def _canonicalize_commutative_expressions(ast, exp):
    def _visit(node):
        if isinstance(node, (exp.And, exp.Or)):
            parts = _flatten_boolean(node, type(node))
            parts = sorted(parts, key=lambda e: e.sql(dialect="postgres", pretty=False))
            rebuilt = parts[0]
            for part in parts[1:]:
                rebuilt = type(node)(this=rebuilt, expression=part)
            return rebuilt

        if isinstance(node, exp.EQ):
            left = node.this
            right = node.expression
            if left is not None and right is not None:
                left_sql = left.sql(dialect="postgres", pretty=False)
                right_sql = right.sql(dialect="postgres", pretty=False)
                if right_sql < left_sql:
                    node.set("this", right.copy())
                    node.set("expression", left.copy())
            return node

        return node

    return ast.transform(_visit)


def normalize_sql_for_em(sql: str) -> Optional[str]:
    """
    Canonicalize SQL so structurally equivalent queries compare equal.

    Returns None if parsing fails.
    """
    from sqlglot import parse_one, exp

    try:
        ast = parse_one(sql, read="postgres")
    except Exception:
        return None

    for node in ast.find_all(exp.Identifier):
        node.set("this", str(node.name).lower())
        node.set("quoted", False)

    for table in ast.find_all(exp.Table):
        table.set("db", None)
        table.set("catalog", None)

    _canonicalize_aliases(ast, exp)
    ast = _canonicalize_commutative_expressions(ast, exp)
    _canonicalize_group_by(ast)
    _canonicalize_order_defaults(ast, exp)

    return ast.sql(dialect="postgres", pretty=False)


def calculate_em(gold_sql: str, pred_sql: str) -> int:
    """Return 1 if normalized SQL strings match, else 0."""
    if not gold_sql or not pred_sql:
        return 0

    gold_norm = normalize_sql_for_em(gold_sql)
    pred_norm = normalize_sql_for_em(pred_sql)

    if gold_norm is None or pred_norm is None:
        return int(_fallback_normalize_sql(gold_sql) == _fallback_normalize_sql(pred_sql))

    return int(gold_norm == pred_norm)

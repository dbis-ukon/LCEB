#!/usr/bin/env python3
"""
End-to-End Benchmark of Injected LIKE-Pattern Estimates (SSCard) in DuckDB
==========================================================================


Requirements
------------
  * Python >= 3.8 (standard library only).
  * A DuckDB build patched with the LIKE-selectivity extension. The patched
    binary reads "pattern<TAB>selectivity" lines from the file named by the
    environment variable DUCKDB_LIKE_SELECTIVITY_FILE.
  * A DuckDB database containing the IMDb schema.
  * One injection file per (table, column) pair, each line formatted as
    "<substring>&<selectivity>" (see INJECTION_FILES below).

Usage
-----
  1. Edit the CONFIGURATION section below (paths, injection files, run counts).

Outputs
-------
  * BASELINE_OUTPUT_CSV / INJECTED_OUTPUT_CSV : per-query timings and plan flags
  * INJECTION_LOG_FILE                        : injected / not-injected LIKEs per query
  * PLAN_CHANGE_LOG_FILE                      : details of join-order / estimate changes
  * PLANS_DIR                                 : raw EXPLAIN JSON plans (if SAVE_PLANS)
  * INVALID_LOG_FILE                          : queries DuckDB rejects (pre-flight)

Pipeline overview
-----------------
  1. Split the workload file into statements.
  2. Pre-flight: relax GROUP BY / ORDER BY for DuckDB's strict SQL rules and
     check every statement with EXPLAIN.
  3. Load the estimator's injection files.
  4. For each query: baseline plan/timing -> injected plan/timing -> baseline
     re-check -> plan comparison.
  5. Write CSVs, logs, and a summary.
"""

import os
import re
import csv
import json
import time
import hashlib
import tempfile
import threading
import subprocess
import statistics
from collections import Counter


# =============================================================================
# CONFIGURATION
# =============================================================================

# --- Paths (replace with local paths) ----------------------------------------
DUCKDB_BINARY = "/path/to/duckdb/build/release/duckdb"   # patched DuckDB CLI
DATABASE = "/path/to/imdb.db"                            # DuckDB database file

INPUT_SQL_FILE = "/path/to/workload.sql"                 # one statement per ';'

BASELINE_OUTPUT_CSV = "baseline.csv"
INJECTED_OUTPUT_CSV = "injected.csv"

# --- Selectivity injection channel -------------------------------------------
LIKE_SELECTIVITY_FILE = "/path/to/like_selectivity.txt"
os.environ["DUCKDB_LIKE_SELECTIVITY_FILE"] = LIKE_SELECTIVITY_FILE
# Passed explicitly to every DuckDB subprocess so injection never depends on
# environment inheritance.
DUCK_ENV = {**os.environ, "DUCKDB_LIKE_SELECTIVITY_FILE": LIKE_SELECTIVITY_FILE}

# --- Logging and artifacts ---------------------------------------------------
INJECTION_LOG_FILE = "injected_selectivities_logs.txt"
PLAN_CHANGE_LOG_FILE = "plan_changes_logs.txt"

SAVE_PLANS = True
PLANS_DIR = "plans_duckdb"
PREFLIGHT = True
INVALID_LOG_FILE = "invalid_queries.log"

# --- Injection files (estimator output) --------------------------------------
# file_key -> path to injection file (format: "substring&value" per line)
estimator = "path_to_method_estimates"
INJECTION_FILES = {
    "actor":    f"{estimator}/actorname_inject.csv",
    "akatitle": f"{estimator}/akatitle_inject.csv",
    "charname": f"{estimator}/charname_inject.csv",
    "company":  f"{estimator}/companyname_inject.csv",
    "title":    f"{estimator}/title_inject.csv",
    "keyword":  f"{estimator}/keywords_inject.csv",
}

# file_key -> (table, column) that file's patterns apply to
INJECTION_FILE_TABLE_MAP = {
    "actor": ("name", "name"),
    "akatitle": ("aka_title", "title"),
    "charname": ("char_name", "name"),
    "company": ("company_name", "name"),
    "title": ("title", "title"),
    "keyword": ("keyword", "keyword"),
}

# --- Injection scope ---------------------------------------------------------
ALLOWED_COLUMNS = set(INJECTION_FILE_TABLE_MAP.values())

# If an alias can't be resolved inside the query, fall back to every allowed
# table that owns that column name.
GLOBAL_FALLBACK = True
# Max pattern characters printed per non-injected LIKE.
PRINT_PATTERN_MAX = 80

# --- Measurement protocol ----------------------------------------------------
# Timeout applies PER STATEMENT (each warm-up / timed run).
TIMEOUT_SECONDS = 5 * 60
RUNS_PER_QUERY = 3
WARMUP_RUNS = 1


# =============================================================================
# 1. SQL STATEMENT SPLITTING
# =============================================================================

def is_real_query(s):
    """Return True if `s` contains at least one non-empty, non-comment line."""
    s = s.strip()
    if not s:
        return False
    lines = [l for l in s.splitlines() if l.strip() and not l.strip().startswith("--")]
    return bool(lines)


def split_statements(content: str) -> list:
    """
    Split a SQL script on top-level semicolons. Semicolons inside single-quoted
    strings are ignored ('' is an escaped quote); comment-only chunks are dropped.
    """
    statements = []
    buf = []
    in_string = False
    i = 0
    n = len(content)

    while i < n:
        ch = content[i]
        if in_string:
            buf.append(ch)
            if ch == "'":
                if i + 1 < n and content[i + 1] == "'":
                    buf.append(content[i + 1])
                    i += 2
                    continue
                in_string = False
            i += 1
            continue
        if ch == "'":
            in_string = True
            buf.append(ch)
            i += 1
            continue
        if ch == ";":
            stmt = "".join(buf).strip()
            if is_real_query(stmt):
                statements.append(stmt)
            buf = []
            i += 1
            continue
        buf.append(ch)
        i += 1

    tail = "".join(buf).strip()
    if is_real_query(tail):
        statements.append(tail)
    return statements


# =============================================================================
# 2. QUERY REWRITING FOR DUCKDB STRICT-SQL COMPATIBILITY
#
# The workload was written for a more permissive SQL dialect. DuckDB rejects
# non-aggregated SELECT / ORDER BY expressions that are absent from GROUP BY,
# so these are wrapped in ANY_VALUE(...). The rewrite does not touch the
# LIKE predicates.
# =============================================================================

def skip_with_clause(stmt: str) -> int:
    """
    Return the index of the main SELECT that follows a leading WITH clause
    (CTE definitions), or 0 if the statement does not start with WITH.
    """
    m = re.match(r'^\s*WITH\b', stmt, re.IGNORECASE)
    if not m:
        return 0
    i = m.end()
    n = len(stmt)
    depth = 0
    in_string = False
    while i < n:
        ch = stmt[i]
        if in_string:
            if ch == "'":
                if i + 1 < n and stmt[i + 1] == "'":
                    i += 2
                    continue
                in_string = False
            i += 1
            continue
        if ch == "'":
            in_string = True
            i += 1
            continue
        if ch == '(':
            depth += 1
            i += 1
            continue
        if ch == ')':
            depth -= 1
            i += 1
            continue
        if depth == 0 and stmt[i:i + 6].upper() == "SELECT":
            before_ok = (not stmt[i - 1].isalnum() and stmt[i - 1] != '_')
            after_ok = (i + 6 >= n) or (not stmt[i + 6].isalnum() and stmt[i + 6] != '_')
            if before_ok and after_ok:
                return i
        i += 1
    return 0


def extract_select_aliases(stmt: str) -> set:
    """Return the lower-cased `AS alias` names in the top-level SELECT list."""
    offset = skip_with_clause(stmt)
    stmt = stmt[offset:]

    m = re.match(r'^\s*SELECT\b', stmt, re.IGNORECASE)
    if not m:
        return set()
    select_start = m.end()
    m2 = re.match(r'\s*DISTINCT\b', stmt[select_start:], re.IGNORECASE)
    if m2:
        select_start += m2.end()

    # Locate the top-level FROM keyword.
    depth = 0
    in_string = False
    i = select_start
    n = len(stmt)
    from_pos = None
    while i < n:
        ch = stmt[i]
        if in_string:
            if ch == "'":
                if i + 1 < n and stmt[i + 1] == "'":
                    i += 2
                    continue
                in_string = False
            i += 1
            continue
        if ch == "'":
            in_string = True
            i += 1
            continue
        if ch == '(':
            depth += 1
            i += 1
            continue
        if ch == ')':
            depth -= 1
            i += 1
            continue
        if depth == 0 and stmt[i:i + 4].upper() == "FROM":
            before_ok = (i == 0) or (not stmt[i - 1].isalnum() and stmt[i - 1] != '_')
            after_ok = (i + 4 >= n) or (not stmt[i + 4].isalnum() and stmt[i + 4] != '_')
            if before_ok and after_ok:
                from_pos = i
                break
        i += 1
    if from_pos is None:
        return set()

    select_list = stmt[select_start:from_pos]

    # Split the select list on top-level commas.
    parts = []
    depth = 0
    in_string = False
    buf = []
    j = 0
    m3 = len(select_list)
    while j < m3:
        ch = select_list[j]
        if in_string:
            buf.append(ch)
            if ch == "'":
                if j + 1 < m3 and select_list[j + 1] == "'":
                    buf.append(select_list[j + 1])
                    j += 2
                    continue
                in_string = False
            j += 1
            continue
        if ch == "'":
            in_string = True
            buf.append(ch)
            j += 1
            continue
        if ch == '(':
            depth += 1
            buf.append(ch)
            j += 1
            continue
        if ch == ')':
            depth -= 1
            buf.append(ch)
            j += 1
            continue
        if ch == ',' and depth == 0:
            parts.append(''.join(buf))
            buf = []
            j += 1
            continue
        buf.append(ch)
        j += 1
    if buf:
        parts.append(''.join(buf))

    aliases = set()
    for part in parts:
        m4 = re.search(r'\bAS\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*$', part.strip(), re.IGNORECASE)
        if m4:
            aliases.add(m4.group(1).lower())
    return aliases


def relax_group_by(stmt: str) -> str:
    """
    For statements with GROUP BY, wrap every non-aggregated, non-window SELECT
    item in ANY_VALUE(...) so DuckDB accepts it.
    """
    offset = skip_with_clause(stmt)
    prefix = stmt[:offset]
    sub = stmt[offset:]

    if not re.search(r'\bGROUP\s+BY\b', sub, re.IGNORECASE):
        return stmt

    m = re.match(r'^\s*SELECT\b', sub, re.IGNORECASE)
    if not m:
        return stmt
    select_start = m.end()

    m2 = re.match(r'\s*DISTINCT\b', sub[select_start:], re.IGNORECASE)
    if m2:
        select_start += m2.end()

    # Locate the top-level FROM keyword.
    depth = 0
    in_string = False
    i = select_start
    n = len(sub)
    from_pos = None
    while i < n:
        ch = sub[i]
        if in_string:
            if ch == "'":
                if i + 1 < n and sub[i + 1] == "'":
                    i += 2
                    continue
                in_string = False
            i += 1
            continue
        if ch == "'":
            in_string = True
            i += 1
            continue
        if ch == '(':
            depth += 1
            i += 1
            continue
        if ch == ')':
            depth -= 1
            i += 1
            continue
        if depth == 0 and sub[i:i + 4].upper() == "FROM":
            before_ok = (i == 0) or (not sub[i - 1].isalnum() and sub[i - 1] != '_')
            after_ok = (i + 4 >= n) or (not sub[i + 4].isalnum() and sub[i + 4] != '_')
            if before_ok and after_ok:
                from_pos = i
                break
        i += 1
    if from_pos is None:
        return stmt

    select_list = sub[select_start:from_pos]
    rest = sub[from_pos:]

    # Split the select list on top-level commas.
    parts = []
    depth = 0
    in_string = False
    buf = []
    j = 0
    m3 = len(select_list)
    while j < m3:
        ch = select_list[j]
        if in_string:
            buf.append(ch)
            if ch == "'":
                if j + 1 < m3 and select_list[j + 1] == "'":
                    buf.append(select_list[j + 1])
                    j += 2
                    continue
                in_string = False
            j += 1
            continue
        if ch == "'":
            in_string = True
            buf.append(ch)
            j += 1
            continue
        if ch == '(':
            depth += 1
            buf.append(ch)
            j += 1
            continue
        if ch == ')':
            depth -= 1
            buf.append(ch)
            j += 1
            continue
        if ch == ',' and depth == 0:
            parts.append(''.join(buf))
            buf = []
            j += 1
            continue
        buf.append(ch)
        j += 1
    if buf:
        parts.append(''.join(buf))

    AGG_RE = re.compile(
        r'^\s*(COUNT|SUM|MIN|MAX|AVG|ANY_VALUE|STRING_AGG|ARRAY_AGG|LIST)\s*\(',
        re.IGNORECASE
    )
    WINDOW_RE = re.compile(r'\bOVER\s*\(', re.IGNORECASE)
    SIMPLE_COLUMN_RE = re.compile(r'^([a-zA-Z_][a-zA-Z0-9_]*)\s*\.\s*([a-zA-Z_][a-zA-Z0-9_]*)$')
    BARE_IDENTIFIER_RE = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')

    new_parts = []
    for part in parts:
        stripped = part.strip()
        if stripped == '*' or AGG_RE.match(stripped) or WINDOW_RE.search(stripped):
            new_parts.append(part)
            continue
        as_match = re.search(r'\bAS\b', stripped, re.IGNORECASE)
        if as_match:
            expr_part = stripped[:as_match.start()].rstrip()
            alias_part = stripped[as_match.start():]
            wrapped = f"ANY_VALUE({expr_part}) {alias_part}"
        else:
            col_match = SIMPLE_COLUMN_RE.match(stripped)
            if col_match:
                implicit_name = col_match.group(2)
                wrapped = f"ANY_VALUE({stripped}) AS {implicit_name}"
            elif BARE_IDENTIFIER_RE.match(stripped):
                wrapped = f"ANY_VALUE({stripped}) AS {stripped}"
            else:
                wrapped = f"ANY_VALUE({stripped})"
        new_parts.append(' ' + wrapped)

    new_select_list = ','.join(new_parts)
    new_sub = sub[:select_start] + new_select_list.rstrip() + ' ' + rest
    return prefix + new_sub


def relax_order_by(stmt: str) -> str:
    """
    For statements with GROUP BY, wrap ORDER BY items that are neither
    aggregates, ordinals, nor SELECT aliases in ANY_VALUE(...).
    """
    offset = skip_with_clause(stmt)
    prefix = stmt[:offset]
    sub = stmt[offset:]

    if not re.search(r'\bGROUP\s+BY\b', sub, re.IGNORECASE):
        return stmt

    known_aliases = extract_select_aliases(sub)

    # Locate the top-level ORDER BY keyword.
    depth = 0
    in_string = False
    i = 0
    n = len(sub)
    order_by_start = None
    while i < n:
        ch = sub[i]
        if in_string:
            if ch == "'":
                if i + 1 < n and sub[i + 1] == "'":
                    i += 2
                    continue
                in_string = False
            i += 1
            continue
        if ch == "'":
            in_string = True
            i += 1
            continue
        if ch == '(':
            depth += 1
            i += 1
            continue
        if ch == ')':
            depth -= 1
            i += 1
            continue
        if depth == 0 and sub[i:i + 8].upper() == "ORDER BY":
            before_ok = (i == 0) or (not sub[i - 1].isalnum() and sub[i - 1] != '_')
            after_ok = (i + 8 >= n) or (not sub[i + 8].isalnum() and sub[i + 8] != '_')
            if before_ok and after_ok:
                order_by_start = i + 8
                break
        i += 1

    if order_by_start is None:
        return stmt

    before = sub[:order_by_start]

    # Locate the end of the ORDER BY clause (a top-level LIMIT, or end of text).
    depth = 0
    in_string = False
    i = order_by_start
    end_pos = n
    while i < n:
        ch = sub[i]
        if in_string:
            if ch == "'":
                if i + 1 < n and sub[i + 1] == "'":
                    i += 2
                    continue
                in_string = False
            i += 1
            continue
        if ch == "'":
            in_string = True
            i += 1
            continue
        if ch == '(':
            depth += 1
            i += 1
            continue
        if ch == ')':
            depth -= 1
            i += 1
            continue
        if depth == 0 and sub[i:i + 5].upper() == "LIMIT":
            before_ok = (not sub[i - 1].isalnum() and sub[i - 1] != '_')
            after_ok = (i + 5 >= n) or (not sub[i + 5].isalnum() and sub[i + 5] != '_')
            if before_ok and after_ok:
                end_pos = i
                break
        i += 1

    order_clause = sub[order_by_start:end_pos]
    after = sub[end_pos:]

    # Split the ORDER BY items on top-level commas.
    parts = []
    depth = 0
    in_string = False
    buf = []
    j = 0
    m = len(order_clause)
    while j < m:
        ch = order_clause[j]
        if in_string:
            buf.append(ch)
            if ch == "'":
                if j + 1 < m and order_clause[j + 1] == "'":
                    buf.append(order_clause[j + 1])
                    j += 2
                    continue
                in_string = False
            j += 1
            continue
        if ch == "'":
            in_string = True
            buf.append(ch)
            j += 1
            continue
        if ch == '(':
            depth += 1
            buf.append(ch)
            j += 1
            continue
        if ch == ')':
            depth -= 1
            buf.append(ch)
            j += 1
            continue
        if ch == ',' and depth == 0:
            parts.append(''.join(buf))
            buf = []
            j += 1
            continue
        buf.append(ch)
        j += 1
    if buf:
        parts.append(''.join(buf))

    AGG_RE = re.compile(
        r'^\s*(COUNT|SUM|MIN|MAX|AVG|ANY_VALUE|STRING_AGG|ARRAY_AGG|LIST)\s*\(',
        re.IGNORECASE
    )
    SUFFIX_RE = re.compile(r'\s+(ASC|DESC)?\s*(NULLS\s+(FIRST|LAST))?\s*$', re.IGNORECASE)
    ORDINAL_RE = re.compile(r'^\s*\d+\s*$')

    new_parts = []
    for part in parts:
        suffix_match = SUFFIX_RE.search(part)
        if suffix_match and suffix_match.group(0).strip():
            core = part[:suffix_match.start()]
            suffix = part[suffix_match.start():]
        else:
            core = part
            suffix = ''

        core_stripped = core.strip()
        if (ORDINAL_RE.match(core_stripped) or AGG_RE.match(core_stripped) or not core_stripped
                or core_stripped.lower() in known_aliases):
            new_parts.append(part)
            continue

        new_parts.append(f" ANY_VALUE({core_stripped}){suffix}")

    new_order_clause = ','.join(new_parts)
    new_sub = before + new_order_clause + after
    return prefix + new_sub


def relax_subqueries_recursive(stmt: str) -> str:
    """Apply the GROUP BY / ORDER BY relaxation to every parenthesised subquery."""
    result = []
    i = 0
    n = len(stmt)
    in_string = False
    while i < n:
        ch = stmt[i]
        if in_string:
            result.append(ch)
            if ch == "'":
                if i + 1 < n and stmt[i + 1] == "'":
                    result.append(stmt[i + 1])
                    i += 2
                    continue
                in_string = False
            i += 1
            continue
        if ch == "'":
            in_string = True
            result.append(ch)
            i += 1
            continue
        if ch == '(':
            j = i + 1
            while j < n and stmt[j].isspace():
                j += 1
            if stmt[j:j + 6].upper() == "SELECT" and (j + 6 >= n or not stmt[j + 6].isalnum()):
                depth = 1
                k = j
                in_str2 = False
                while k < n and depth > 0:
                    c2 = stmt[k]
                    if in_str2:
                        if c2 == "'":
                            if k + 1 < n and stmt[k + 1] == "'":
                                k += 2
                                continue
                            in_str2 = False
                        k += 1
                        continue
                    if c2 == "'":
                        in_str2 = True
                        k += 1
                        continue
                    if c2 == '(':
                        depth += 1
                    elif c2 == ')':
                        depth -= 1
                        if depth == 0:
                            break
                    k += 1
                inner = stmt[j:k]
                inner = relax_subqueries_recursive(inner)
                inner = relax_group_by(inner)
                inner = relax_order_by(inner)
                result.append('(')
                result.append(inner)
                result.append(')')
                i = k + 1
                continue
        result.append(ch)
        i += 1
    return ''.join(result)


def strip_leading_comments(stmt: str) -> str:
    """Remove leading blank and `--` comment lines."""
    lines = stmt.splitlines()
    i = 0
    while i < len(lines) and (not lines[i].strip() or lines[i].strip().startswith("--")):
        i += 1
    return "\n".join(lines[i:])


def relax_all(stmt: str) -> str:
    """Full rewrite pipeline: subqueries, then top-level GROUP BY, then ORDER BY."""
    stmt = strip_leading_comments(stmt)
    stmt = relax_subqueries_recursive(stmt)
    stmt = relax_group_by(stmt)
    stmt = relax_order_by(stmt)
    return stmt


# =============================================================================
# 3. ALIAS -> TABLE RESOLUTION (handles unaliased tables / unqualified columns)
#
# The selectivity file is keyed by pattern only, so each LIKE predicate has to
# be attributed to a (table, column) pair. Aliases, unaliased tables, and
# unqualified columns are handled heuristically with regular expressions.
# =============================================================================

_RESERVED_ALIASES = {
    "ON", "WHERE", "GROUP", "ORDER", "HAVING", "LIMIT", "OFFSET", "FETCH", "WINDOW",
    "UNION", "EXCEPT", "INTERSECT", "AS", "USING", "JOIN", "LEFT", "RIGHT", "INNER",
    "FULL", "CROSS", "NATURAL", "LATERAL", "QUALIFY", "SET",
}
# Used inside the regex so a reserved word is never consumed as an alias
# (otherwise "FROM title JOIN x" would swallow "JOIN" and skip table x).
_RESERVED_RX = "|".join(sorted(_RESERVED_ALIASES, key=len, reverse=True))

_KNOWN_TABLES = {t for t, _ in ALLOWED_COLUMNS}


def extract_alias_map(sql):
    """alias (or bare table name) -> set of table names."""
    amap = {}

    def add(key, table):
        amap.setdefault(key.lower(), set()).add(table.lower())

    rx = re.compile(
        rf"\b(?:FROM|JOIN)\s+([a-zA-Z_]\w*)"
        rf"(?:\s+(?:AS\s+)?(?!(?:{_RESERVED_RX})\b)([a-zA-Z_]\w*))?",
        re.IGNORECASE)
    for table, alias in rx.findall(sql):
        add(table, table)  # table name is always usable as its own alias
        if alias and alias.upper() not in _RESERVED_ALIASES:
            add(alias, table)

    # comma joins:  FROM a x, b y
    if _KNOWN_TABLES:
        names = "|".join(sorted(_KNOWN_TABLES, key=len, reverse=True))
        rx2 = re.compile(
            rf",\s*({names})\b(?:\s+(?:AS\s+)?(?!(?:{_RESERVED_RX})\b)([a-zA-Z_]\w*))?",
            re.IGNORECASE)
        for table, alias in rx2.findall(sql):
            add(table, table)
            if alias and alias.upper() not in _RESERVED_ALIASES:
                add(alias, table)
    return amap


# [alias.]column [NOT] LIKE 'pattern'   - alias is OPTIONAL
_PRED_LIKE_RE = re.compile(
    r"""(?<![\w.])
        (?:([a-zA-Z_]\w*)\s*\.\s*)?       # optional alias / table qualifier
        ([a-zA-Z_]\w*)                    # column
        \s+(NOT\s+LIKE|LIKE)\s+
        '((?:[^']|'')*)'                  # SQL string, '' is an escaped quote
    """,
    re.IGNORECASE | re.VERBOSE)


def sql_unescape(pattern: str) -> str:
    """Undo SQL quote escaping ('' -> ')."""
    return pattern.replace("''", "'")


def candidate_tables(alias, column, alias_to_tables):
    """Allowed tables this predicate could belong to."""
    column = column.lower()
    if alias:
        tabs = alias_to_tables.get(alias.lower())
        if tabs:
            return sorted(t for t in tabs if (t, column) in ALLOWED_COLUMNS)
    else:
        tabs = {t for ts in alias_to_tables.values() for t in ts}
        cands = sorted(t for t in tabs if (t, column) in ALLOWED_COLUMNS)
        if cands:
            return cands
    if GLOBAL_FALLBACK:
        if alias and alias.lower() in alias_to_tables:
            return []  # alias resolved to a table, but that column isn't allowed
        return sorted(t for t, c in ALLOWED_COLUMNS if c == column)
    return []


# =============================================================================
# 4. INJECTION LOOKUP (estimator output files)
# =============================================================================

class SelectivityLookup:
    """
    In-memory index of the injection files, keyed by (table, column). A
    predicate is injected only if its pattern, as written in the query, equals
    one of the file's '%substring%' keys exactly.
    """

    def __init__(self, files: dict, table_map: dict):
        self.lookups = {}  # (table, column) -> {'%substring%': value}
        self.hits = 0
        self.misses = 0
        self.reasons = Counter()

        for file_key, file_path in files.items():
            tc = table_map.get(file_key)
            if tc is None:
                print(f"  [WARN] no table/column mapping for '{file_key}' - skipping {file_path}")
                continue
            if not os.path.exists(file_path):
                print(f"  [WARN] injection file not found: {file_path} - skipping")
                continue
            d = self._load_dict(file_path)
            self.lookups[tc] = d
            print(f"  loaded {len(d)} unique pattern(s) from {file_key} -> {tc}")

    @staticmethod
    def _load_dict(file_path):
        """
        Injection files hold bare substrings ("abc&0.01"). method estimates
        infix patterns, so the key stored is '%abc%' - exactly what the query
        must contain to get an injected value.
        """
        d, dup, skipped = {}, 0, 0
        with open(file_path, encoding="utf-8") as f:
            for line in f:
                pattern, sep, value = line.rstrip("\r\n").rpartition("&")
                if not sep:
                    skipped += 1
                    continue
                key = pattern 
                if key in d:
                    dup += 1
                d[key] = value
        if skipped:
            print(f"    [WARN] {skipped} line(s) without '&' skipped in {file_path}")
        if dup:
            print(f"    [WARN] {dup} duplicate pattern(s) in {file_path}, last value kept")
        return d

    def miss(self, reason):
        """Record a failed lookup and return (None, reason)."""
        self.misses += 1
        self.reasons[reason] += 1
        return None, reason

    def find(self, cands, pattern):
        """
        Exact match of the LIKE pattern as written in the query against the
        '%substring%' keys. Returns (value, None) on a hit, (None, reason) on a miss.
        """
        values = []
        for (t, c), d in self.lookups.items():
            if t in cands and pattern in d:
                values.append(d[pattern])
        if not values:
            return self.miss("pattern_not_in_injection_file")
        if len(set(values)) > 1:
            self.reasons["note:ambiguous_candidates_used_first"] += 1
        self.hits += 1
        return values[0], None

    def summary(self):
        """Print hit / miss statistics of all lookups."""
        total = self.hits + self.misses
        pct = (self.hits / total * 100) if total else 0.0
        print(f"\n  Selectivity lookup summary: {self.hits}/{total} LIKE predicate(s) "
              f"got an injected value ({pct:.1f}%), {self.misses} fell back to DuckDB.")
        print(f"  Miss / note reasons : {dict(self.reasons)}")


# =============================================================================
# 5. PER-QUERY INJECTION FILE (like_selectivity.txt) AND LOGGING
# =============================================================================

def extract_injectable_patterns(stmt: str, lookup: SelectivityLookup):
    """
    Returns (matched, missed):
      matched: {pattern: value}
      missed : [(label, op, pattern, reason)] for every LIKE predicate NOT injected
    """
    alias_to_tables = extract_alias_map(stmt)
    matched = {}
    missed = []
    seen = set()

    for alias, column, op, raw_pattern in _PRED_LIKE_RE.findall(stmt):
        pattern = sql_unescape(raw_pattern)
        op_norm = " ".join(op.upper().split())
        label = f"{alias + '.' if alias else ''}{column}"
        key = (label.lower(), op_norm, pattern)
        if key in seen:
            continue
        seen.add(key)

        cands = candidate_tables(alias, column, alias_to_tables)
        if not cands:
            _, reason = lookup.miss("column_not_in_allowed_set")
            missed.append((label, op_norm, pattern, reason))
            continue

        value, reason = lookup.find(cands, pattern)
        if value is None:
            missed.append((label, op_norm, pattern, reason))
        else:
            matched.setdefault(pattern, value)  # NOT LIKE uses the same value as LIKE

    return matched, missed


def _short(p: str) -> str:
    """Shorten a pattern for console output."""
    p = p.replace("\n", " ")
    return p if len(p) <= PRINT_PATTERN_MAX else p[:PRINT_PATTERN_MAX] + "..."


def print_injection_report(matched: dict, missed: list):
    """Print how many LIKE predicates of the current query were injected."""
    total = len(matched) + len(missed)
    print(f"  LIKE predicates: {total} total, {len(matched)} injected, {len(missed)} NOT injected")
    for label, op, pattern, reason in missed:
        print(f"    NOT injected [{reason}]: {label} {op} '{_short(pattern)}'")


def write_like_selectivity_file(matched: dict):
    """Write the "pattern<TAB>selectivity" file read by the patched DuckDB."""
    with open(LIKE_SELECTIVITY_FILE, "w") as f:
        for pattern, value in matched.items():
            f.write(f"{pattern}\t{float(value):.10f}\n")


def log_injected_patterns(log_path: str, query_no: int, matched: dict, missed: list):
    """Append the injected / not-injected patterns of one query to the log."""
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(f"Query {query_no}\n")
        f.write("-" * 40 + "\n")
        f.write(f"INJECTED ({len(matched)}):\n")
        if matched:
            for pattern, value in matched.items():
                f.write(f"  {pattern}\t{float(value):.10f}\n")
        else:
            f.write("  (none)\n")
        f.write(f"NOT INJECTED ({len(missed)}):\n")
        if missed:
            for label, op, pattern, reason in missed:
                f.write(f"  [{reason}] {label} {op} '{pattern}'\n")
        else:
            f.write("  (none)\n")
        f.write("\n")


def clear_like_selectivity_file():
    """Switch injection off (empty file = DuckDB's native estimates)."""
    # Truncate (not append) so a previous query's values can never leak.
    open(LIKE_SELECTIVITY_FILE, "w").close()


# =============================================================================
# 6. RUNNING THE DUCKDB CLI AND TIMING
# =============================================================================

def _body(stmt: str) -> str:
    """Statement text without its trailing semicolon."""
    return stmt.rstrip().rstrip(";")


def run_cli_script(script_text: str):
    """
    Feed `script_text` to ONE duckdb CLI process (via a temp file on stdin).
    The timeout is per statement: the clock restarts every time the process
    prints a line, and every statement in a phase script is followed by a
    .print marker. The process runs with -bail (stops at the first error).

    Returns (stdout_lines, stderr_text, returncode, killed_by_timeout).
    """
    with tempfile.NamedTemporaryFile("w", suffix=".sql", delete=False, encoding="utf-8") as tf:
        tf.write(script_text)
        script_path = tf.name

    out_lines = []
    err_chunks = []
    last_progress = [time.monotonic()]

    try:
        with open(script_path, "r", encoding="utf-8") as stdin_f:
            proc = subprocess.Popen(
                [DUCKDB_BINARY, "-bail", DATABASE],
                stdin=stdin_f,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                errors="replace",
                env=DUCK_ENV,
            )

            def read_out():
                for line in proc.stdout:
                    out_lines.append(line.rstrip("\n"))
                    last_progress[0] = time.monotonic()

            def read_err():
                err_chunks.append(proc.stderr.read())

            t_out = threading.Thread(target=read_out, daemon=True)
            t_err = threading.Thread(target=read_err, daemon=True)
            t_out.start()
            t_err.start()

            killed = False
            while proc.poll() is None:
                if time.monotonic() - last_progress[0] > TIMEOUT_SECONDS:
                    killed = True
                    proc.kill()
                    break
                time.sleep(0.05)

            proc.wait()
            t_out.join()
            t_err.join()
    finally:
        try:
            os.remove(script_path)
        except OSError:
            pass

    return out_lines, "".join(err_chunks), proc.returncode, killed


_TIMER_RE = re.compile(r"Run Time \(s\): real ([0-9]+(?:\.[0-9]+)?)")


def build_phase_script(stmt: str) -> str:
    """
    One process: WARMUP_RUNS untimed executions, then RUNS_PER_QUERY timed
    ones. `.mode trash` discards result rows, `.timer on` prints the engine
    time of each statement, and the `.print` markers tell us which runs completed.
    """
    body = _body(stmt)
    lines = [".timer on", ".mode trash"]
    for w in range(WARMUP_RUNS):
        lines.append(body + "\n;")
        lines.append(f".print WARMUP_DONE {w + 1}")
    for r in range(RUNS_PER_QUERY):
        lines.append(body + "\n;")
        lines.append(f".print RUN_DONE {r + 1}")
    return "\n".join(lines) + "\n"


def parse_phase_output(lines):
    """Returns (number of warm-ups finished, {run_index: seconds})."""
    last_time = None
    warm_done = 0
    times = {}
    for line in lines:
        m = _TIMER_RE.search(line)
        if m:
            last_time = float(m.group(1))
            continue
        if line.startswith("WARMUP_DONE"):
            warm_done += 1
            last_time = None
        elif line.startswith("RUN_DONE"):
            try:
                idx = int(line.split()[1])
            except (IndexError, ValueError):
                continue
            if last_time is not None:
                times[idx] = last_time
            last_time = None
    return warm_done, times


def run_phase(stmt: str):
    """
    Warm-cache timing of one query under the CURRENT like_selectivity.txt.
    Returns (times_list, timed_out, failed).
    """
    script = build_phase_script(stmt)
    lines, stderr, rc, killed = run_cli_script(script)
    warm_done, times = parse_phase_output(lines)

    for w in range(warm_done):
        print(f"    warm-up {w + 1}: ok")

    times_list = []
    for r in range(1, RUNS_PER_QUERY + 1):
        if r in times:
            times_list.append(times[r])
            print(f"    run {r}: ok ({times[r]:.4f}s)")
        else:
            break

    if len(times_list) == RUNS_PER_QUERY:
        return times_list, False, False

    # Something went wrong: a timeout, an SQL error, or a crash.
    if killed:
        status = "timeout"
    else:
        status = "failed"
        msg = stderr.strip()[:200] if stderr.strip() else f"exit code {rc}"
        print(f"      FAILED - {msg}")

    stage = "warm-up" if warm_done < WARMUP_RUNS else f"run {len(times_list) + 1}"
    print(f"    {stage}: {status}")
    times_list.extend([status] * (RUNS_PER_QUERY - len(times_list)))
    return times_list, killed, not killed


# =============================================================================
# 7. PLANNING TIME (timed plain EXPLAIN: parse + bind + optimize, no execution)
# =============================================================================

def build_plan_time_script(stmt: str) -> str:
    """One warm-up EXPLAIN followed by RUNS_PER_QUERY timed EXPLAINs."""
    body = _body(stmt)
    lines = [".timer on", ".mode trash"]
    # one untimed warm-up so catalog / statistics are loaded
    lines.append("EXPLAIN " + body + "\n;")
    lines.append(".print WARMUP_DONE 1")
    for r in range(RUNS_PER_QUERY):
        lines.append("EXPLAIN " + body + "\n;")
        lines.append(f".print RUN_DONE {r + 1}")
    return "\n".join(lines) + "\n"


def run_plan_time(stmt: str):
    """Average planning time in seconds over RUNS_PER_QUERY EXPLAINs, or None on failure."""
    script = build_plan_time_script(stmt)
    lines, stderr, rc, killed = run_cli_script(script)
    _, times = parse_phase_output(lines)

    vals = [times[r] for r in range(1, RUNS_PER_QUERY + 1) if r in times]
    if not vals:
        print(f"    plan time: {'timeout' if killed else 'failed'}")
        return None
    avg_pt = sum(vals) / len(vals)
    print(f"    plan time (EXPLAIN avg): {avg_pt:.4f}s")
    return avg_pt


# =============================================================================
# 8. PLAN CAPTURE AND COMPARISON
# =============================================================================

def get_plan(stmt: str):
    """
    Physical plan of the query under the CURRENT like_selectivity.txt, as the
    parsed EXPLAIN (FORMAT JSON) tree (root operator dict), or None.
    """
    sql = "EXPLAIN (FORMAT JSON) " + _body(stmt) + "\n;"
    try:
        result = subprocess.run(
            [DUCKDB_BINARY, "-json", DATABASE, "-c", sql],
            capture_output=True,
            text=True,
            errors="replace",
            timeout=TIMEOUT_SECONDS,
            env=DUCK_ENV,
        )
    except subprocess.TimeoutExpired:
        return None
    if result.returncode != 0:
        return None
    return _extract_plan_json(result.stdout)


def _extract_plan_json(text: str):
    """
    Parse the JSON plan out of the CLI output. Some DuckDB versions wrap it as
    [{"explain_key": ..., "explain_value": "<json>"}]; both shapes are handled.
    """
    start = text.find("[")
    if start < 0:
        return None
    try:
        data, _ = json.JSONDecoder().raw_decode(text[start:])
    except ValueError:
        return None

    if isinstance(data, list) and data and isinstance(data[0], dict) and "explain_value" in data[0]:
        try:
            data = json.loads(data[0]["explain_value"])
        except (ValueError, TypeError):
            return None

    if isinstance(data, list) and data and isinstance(data[0], dict):
        return data[0]
    if isinstance(data, dict):
        return data
    return None


def _info(node):
    """Return a plan node's extra_info as a dict."""
    info = node.get("extra_info")
    return info if isinstance(info, dict) else ({"raw": info} if info else {})


def structure_of(node):
    """Whole operator tree without cardinality estimates."""
    info = {k: v for k, v in _info(node).items() if "cardinality" not in k.lower()}
    return {
        "name": node.get("name"),
        "info": info,
        "children": [structure_of(c) for c in node.get("children") or []],
    }


def estimates_of(node, acc=None):
    """Pre-order list of per-operator estimated cardinality (with extra_info)."""
    acc = [] if acc is None else acc

    info = _info(node)
    card = None

    for k, v in info.items():
        if "cardinality" in k.lower():
            card = str(v)
            break

    acc.append({
        "operator": node.get("name"),
        "cardinality": card,
        "extra_info": info,
    })

    for child in node.get("children") or []:
        estimates_of(child, acc)

    return acc


def join_tree(node):
    """
    Join order only. Leaves are table names; every operator with 2+ children
    becomes (operator, join type, conditions, (child, child, ...)) with the
    children in plan order, so a build/probe swap counts as a change.
    Single-child operators (filters, projections, aggregates) are transparent.
    """
    info = _info(node)
    kids = node.get("children") or []
    if not kids:
        return info.get("Table") or node.get("name")
    subs = tuple(join_tree(c) for c in kids)
    if len(subs) == 1:
        return subs[0]
    return (node.get("name"), info.get("Join Type"), info.get("Conditions"), subs)


def fmt_join_tree(t):
    """Human-readable rendering of a join tree (for the plan-change log)."""
    if not isinstance(t, tuple):
        return str(t)
    name, jtype, cond, kids = t
    inner = " JOIN ".join(fmt_join_tree(k) for k in kids)
    return f"({inner} [{name} {jtype or ''} {cond or ''}])".replace("  ", " ")


def analyze_plan(root):
    """Derive structure, estimates, join tree, and a structure hash from a plan."""
    if root is None:
        return None
    struct = structure_of(root)
    return {
        "root": root,
        "struct": struct,
        "est": estimates_of(root),
        "join": join_tree(root),
        "md5": hashlib.md5(json.dumps(struct, sort_keys=True).encode("utf-8")).hexdigest(),
    }


def _cmp(a, b):
    """Three-valued comparison: YES (differ), NO (equal), UNKNOWN (missing)."""
    if a is None or b is None:
        return "UNKNOWN"
    return "YES" if a != b else "NO"


def compare_plans(base, inj):
    """Returns (plan_changed, join_order_changed, estimates_changed)."""
    if base is None or inj is None:
        return "UNKNOWN", "UNKNOWN", "UNKNOWN"
    return (_cmp(base["struct"], inj["struct"]),
            _cmp(base["join"], inj["join"]),
            _cmp(base["est"], inj["est"]))


def interpret(n_injected, plan_changed, join_changed, est_changed):
    """Map the three comparison flags to a single interpretation label."""
    if "UNKNOWN" in (plan_changed, join_changed, est_changed):
        return "unknown"
    anything = "YES" in (plan_changed, join_changed, est_changed)
    if n_injected == 0:
        return "changed_without_injection_UNEXPECTED" if anything else "no_patterns_injected"
    if join_changed == "YES":
        return "join_order_changed"
    if plan_changed == "YES":
        return "plan_changed_same_join_order"
    if est_changed == "YES":
        return "same_plan_estimates_changed"
    return "same_plan_estimates_identical_injection_suspect"


def save_plan(plan_root, query_no, tag):
    """Dump a raw EXPLAIN JSON plan to PLANS_DIR (if SAVE_PLANS)."""
    if not SAVE_PLANS or plan_root is None:
        return
    os.makedirs(PLANS_DIR, exist_ok=True)
    path = os.path.join(PLANS_DIR, f"q{query_no:04d}_{tag}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(plan_root, f, indent=2, sort_keys=True)


def estimate_diffs(base, inj, limit=10):
    """List the first `limit` operators whose estimated cardinality changed."""
    b, j = base["est"], inj["est"]

    if len(b) != len(j):
        return ["(plan shapes differ - estimates not aligned)"]

    diffs = []
    for bn, jn in zip(b, j):
        if (bn.get("operator"), bn.get("cardinality")) != (
            jn.get("operator"), jn.get("cardinality")
        ):
            diffs.append(
                f"{bn.get('operator')}: "
                f"{bn.get('cardinality')} -> {jn.get('cardinality')}"
            )

    return diffs[:limit]


def log_plan_change(log_path, query_no, n_injected, flags, base, inj):
    """Append the plan-comparison result of one query to the plan-change log."""
    plan_changed, join_changed, est_changed, unstable, interp = flags
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(f"Query {query_no}\n")
        f.write("-" * 40 + "\n")
        f.write(f"Patterns injected:        {n_injected}\n")
        f.write(f"Join order changed:       {join_changed}\n")
        f.write(f"Plan structure changed:   {plan_changed}\n")
        f.write(f"Estimates changed:        {est_changed}\n")
        f.write(f"Baseline unstable (noise):{unstable}\n")
        f.write(f"Interpretation:           {interp}\n")
        if base is not None and inj is not None:
            if join_changed == "YES":
                f.write("\n--- BASELINE JOIN TREE ---\n" + fmt_join_tree(base["join"]) + "\n")
                f.write("--- INJECTED JOIN TREE ---\n" + fmt_join_tree(inj["join"]) + "\n")
            if est_changed == "YES":
                f.write("\n--- ESTIMATE CHANGES (first 10) ---\n")
                for d in estimate_diffs(base, inj):
                    f.write("  " + d + "\n")
        f.write("\n")


# =============================================================================
# 9. PRE-FLIGHT (which queries does DuckDB actually accept?)
# =============================================================================

def explain_check(stmt: str):
    """EXPLAIN only (nothing is executed). Returns (ok, first error line)."""
    try:
        r = subprocess.run(
            [DUCKDB_BINARY, DATABASE, "-c", "EXPLAIN " + _body(stmt) + "\n;"],
            capture_output=True,
            text=True,
            errors="replace",
            timeout=TIMEOUT_SECONDS,
            env=DUCK_ENV,
        )
    except subprocess.TimeoutExpired:
        return False, "EXPLAIN timed out"
    if r.returncode == 0:
        return True, ""
    lines = [l for l in r.stderr.strip().splitlines() if l.strip()]
    return False, (lines[0] if lines else f"exit code {r.returncode}")[:200]


def preflight(raw_statements):
    """
    Returns a list of (query_no, statement_or_None, status) with status in
    'relaxed' | 'raw_fallback' | 'invalid'. Query numbers are the original
    positions in the input file, so skipped queries never shift the numbering.
    """
    if not PREFLIGHT:
        return [(i + 1, relax_all(s), "relaxed") for i, s in enumerate(raw_statements)]

    clear_like_selectivity_file()
    entries = []
    n_relaxed = n_fallback = n_invalid = 0
    total = len(raw_statements)
    print(f"Pre-flight: checking {total} statements on DuckDB (EXPLAIN only)...")

    with open(INVALID_LOG_FILE, "w", encoding="utf-8") as log:
        for i, raw in enumerate(raw_statements):
            qn = i + 1
            relaxed = relax_all(raw)

            ok, err = explain_check(relaxed)
            if ok:
                entries.append((qn, relaxed, "relaxed"))
                n_relaxed += 1
            else:
                if relaxed != raw:
                    ok_raw, err_raw = explain_check(raw)
                else:
                    ok_raw, err_raw = False, err

                if ok_raw:
                    entries.append((qn, raw, "raw_fallback"))
                    n_fallback += 1
                    log.write(f"Query {qn}: RELAXATION BROKE IT - using the original statement\n"
                              f"  relaxed : {err}\n  original: valid\n\n")
                else:
                    entries.append((qn, None, "invalid"))
                    n_invalid += 1
                    log.write(f"Query {qn}: INVALID ON DUCKDB (as written and relaxed)\n"
                              f"  relaxed : {err}\n  original: {err_raw}\n"
                              f"  SQL     : {' '.join(raw.split())[:300]}\n\n")

            if (i + 1) % 100 == 0:
                print(f"  ... {i + 1}/{total}")

    print(f"Pre-flight result: {n_relaxed} valid (relaxed), {n_fallback} valid only as written, "
          f"{n_invalid} invalid on DuckDB")
    print(f"Reasons for every non-relaxed query: {INVALID_LOG_FILE}\n")
    return entries


# =============================================================================
# 10. RESULT ROWS (CSV schema)
# =============================================================================

def build_row(query_no, times_list, timed_out, failed, plan_time_s=None):
    """Build one CSV row: query number, per-run times, summary statistics, flags."""
    valid = [t for t in times_list if isinstance(t, (int, float))]
    avg_t = sum(valid) / len(valid) if valid else None
    median_t = statistics.median(valid) if valid else None
    min_t = min(valid) if valid else None
    max_t = max(valid) if valid else None
    stdev_t = statistics.stdev(valid) if len(valid) >= 2 else None

    return ([query_no] + times_list
            + [avg_t, median_t, min_t, max_t, stdev_t, len(valid), timed_out, failed,
               plan_time_s])


HEADER_SUFFIX = ["avg(s)", "median(s)", "min(s)", "max(s)", "stdev(s)", "n_valid_runs",
                 "timed_out", "failed", "avg_pt(s)"]

PLAN_COLUMNS = [
    "join_order_changed",   # join tree differs (tables, types, conditions, sides)
    "plan_changed",         # any operator / filter / projection differs
    "estimates_changed",    # estimated cardinality differs => injection took effect
    "baseline_unstable",    # baseline vs baseline re-check differ => noise
    "n_injected",           # LIKE patterns written to like_selectivity.txt
    "n_like_not_injected",  # LIKE predicates that fell back to DuckDB's own estimate
    "interpretation",
    "baseline_plan_md5",
    "injected_plan_md5",
]


# =============================================================================
# 11. MAIN
# =============================================================================

def main():
    """Run the full benchmark (see the module docstring for the pipeline)."""
    for path in (BASELINE_OUTPUT_CSV, INJECTED_OUTPUT_CSV):
        out_dir = os.path.dirname(path)
        if out_dir:
            os.makedirs(out_dir, exist_ok=True)
    if SAVE_PLANS:
        os.makedirs(PLANS_DIR, exist_ok=True)

    with open(INPUT_SQL_FILE, "r", encoding="utf-8") as f:
        content = f.read()
    raw_statements = split_statements(content)
    num_queries = len(raw_statements)
    print(f"\nTOTAL QUERIES FOUND: {num_queries}\n")
    entries = preflight(raw_statements)
    print(f"Timing: {WARMUP_RUNS} warm-up run(s) + {RUNS_PER_QUERY} timed run(s) per phase, "
          f"same process (warm cache)\n")

    print("Loading selectivity lookup tables...")
    lookup = SelectivityLookup(INJECTION_FILES, INJECTION_FILE_TABLE_MAP)
    print()

    open(INJECTION_LOG_FILE, "w").close()
    open(PLAN_CHANGE_LOG_FILE, "w").close()
    print(f"Injected / not-injected LIKEs will be logged to: {INJECTION_LOG_FILE}")
    print(f"Plan change details will be logged to: {PLAN_CHANGE_LOG_FILE}\n")

    time_cols = [f"t{r + 1}(s)" for r in range(RUNS_PER_QUERY)]
    header = ["Query No"] + time_cols + HEADER_SUFFIX + PLAN_COLUMNS

    baseline_rows = []
    injected_rows = []

    total_timeouts = 0
    total_failures = 0
    counts = {"plan": {"YES": 0, "NO": 0, "UNKNOWN": 0},
              "join": {"YES": 0, "NO": 0, "UNKNOWN": 0},
              "est": {"YES": 0, "NO": 0, "UNKNOWN": 0},
              "unstable": {"YES": 0, "NO": 0, "UNKNOWN": 0}}
    interp_counts = {}
    queries_with_injection = 0
    queries_with_missed = 0
    invalid_count = 0
    run_start_all = time.time()

    try:
        for qn, stmt, pf_status in entries:
            print(f"[Query {qn}/{num_queries}]")

            if stmt is None:
                # Rejected by DuckDB as written and relaxed: keep a row, don't run it.
                invalid_count += 1
                print("  skipped: invalid on DuckDB (see invalid-queries log)")
                skipped_times = ["failed"] * RUNS_PER_QUERY
                skipped_plan_cols = ["UNKNOWN", "UNKNOWN", "UNKNOWN", "UNKNOWN", 0, 0,
                                     "invalid_on_duckdb", "", ""]
                baseline_rows.append(build_row(qn, skipped_times, False, True, None) + skipped_plan_cols)
                injected_rows.append(build_row(qn, skipped_times, False, True, None) + skipped_plan_cols)
                continue

            if not stmt.strip():
                continue

            # ---- Baseline: plan, then warm-cache timing, then planning time ----
            clear_like_selectivity_file()
            b_root = get_plan(stmt)
            save_plan(b_root, qn, "baseline")
            print("  baseline timing:")
            b_times, b_timed_out, b_failed = run_phase(stmt)
            total_timeouts += b_timed_out
            total_failures += b_failed
            b_plan_t = run_plan_time(stmt)   # selectivity file still empty -> baseline planning

            # ---- Injection: write file, plan, then warm-cache timing, then planning time ----
            matched, missed = extract_injectable_patterns(stmt, lookup)
            write_like_selectivity_file(matched)
            log_injected_patterns(INJECTION_LOG_FILE, qn, matched, missed)
            n_injected = len(matched)
            n_missed = len(missed)
            if n_injected:
                queries_with_injection += 1
            if n_missed:
                queries_with_missed += 1
            print_injection_report(matched, missed)

            j_root = get_plan(stmt)
            save_plan(j_root, qn, "injected")
            print("  injected timing:")
            j_times, j_timed_out, j_failed = run_phase(stmt)
            total_timeouts += j_timed_out
            total_failures += j_failed
            j_plan_t = run_plan_time(stmt)   # selectivity file still filled -> injected planning

            # ---- Baseline re-check (noise floor), injection switched off ----
            clear_like_selectivity_file()
            r_root = get_plan(stmt)

            # ---- Compare plans ----
            base = analyze_plan(b_root)
            inj = analyze_plan(j_root)
            recheck = analyze_plan(r_root)
            plan_changed, join_changed, est_changed = compare_plans(base, inj)

            if base is None or recheck is None:
                unstable = "UNKNOWN"
            else:
                unstable = "YES" if (base["struct"] != recheck["struct"]
                                     or base["est"] != recheck["est"]) else "NO"

            interp = interpret(n_injected, plan_changed, join_changed, est_changed)
            if unstable == "YES":
                interp += "_BASELINE_UNSTABLE"

            print(f"    JOIN ORDER CHANGED: {join_changed} | plan: {plan_changed} | "
                  f"estimates: {est_changed} | unstable: {unstable} | {interp}")

            counts["plan"][plan_changed] += 1
            counts["join"][join_changed] += 1
            counts["est"][est_changed] += 1
            counts["unstable"][unstable] += 1
            interp_counts[interp] = interp_counts.get(interp, 0) + 1

            log_plan_change(PLAN_CHANGE_LOG_FILE, qn, n_injected,
                            (plan_changed, join_changed, est_changed, unstable, interp),
                            base, inj)

            plan_cols = [join_changed, plan_changed, est_changed, unstable, n_injected, n_missed,
                         interp, base["md5"] if base else "", inj["md5"] if inj else ""]

            baseline_rows.append(build_row(qn, b_times, b_timed_out, b_failed, b_plan_t) + plan_cols)
            injected_rows.append(build_row(qn, j_times, j_timed_out, j_failed, j_plan_t) + plan_cols)

    finally:
        # Always leave injection switched off, even after an exception / Ctrl-C.
        clear_like_selectivity_file()

    lookup.summary()

    total_wall_all = time.time() - run_start_all
    print(f"\nTotal benchmark wall time: {total_wall_all / 60:.1f} min")
    print(f"Timeouts: {total_timeouts}   Failures: {total_failures}")
    print(f"Queries with at least one injected pattern: {queries_with_injection}/{num_queries}")
    print(f"Queries with at least one NOT-injected LIKE: {queries_with_missed}/{num_queries}")
    print(f"Skipped as invalid on DuckDB: {invalid_count}/{num_queries}")

    print("\n--- PLAN COMPARISON SUMMARY ---")
    print(f"Join order changed:      {counts['join']}")
    print(f"Plan structure changed:  {counts['plan']}")
    print(f"Estimates changed:       {counts['est']}")
    print(f"Baseline unstable:       {counts['unstable']}")
    print("Interpretation:")
    for k, v in sorted(interp_counts.items()):
        print(f"   {k}: {v}")
    print(f"Full details in: {PLAN_CHANGE_LOG_FILE}")
    print("--------------------------------")

    with open(BASELINE_OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(baseline_rows)

    with open(INJECTED_OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(injected_rows)

    print("\nDone.")
    print(f"   Total queries: {num_queries}")
    print(f"   Baseline CSV:  {BASELINE_OUTPUT_CSV}")
    print(f"   Injected CSV:  {INJECTED_OUTPUT_CSV}")
    print(f"   Injection log: {INJECTION_LOG_FILE}")
    print(f"   Plan change log: {PLAN_CHANGE_LOG_FILE}")
    if SAVE_PLANS:
        print(f"   Plan dumps:    {PLANS_DIR}/")


if __name__ == "__main__":
    main()

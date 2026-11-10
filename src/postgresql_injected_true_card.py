#!/usr/bin/env python3
"""
End-to-End Benchmark of Injected True LIKE Selectivities in PostgreSQL
======================================================================

Supplementary material for the paper
"Do Accurate LIKE-Pattern Cardinality Estimates Matter?"

How injection works
-------------------
The patched PostgreSQL (a) writes the single-table predicates and its own
selectivities of a query to PREDICATES_PATH when `print_single_tbl_queries` is
on, and (b) reads one selectivity per line from SELECTIVITIES_PATH when
`ml_cardest_enabled` is on. Per query, the script
  1. captures the predicates with a plan-only EXPLAIN (nothing is executed),
  2. counts the true selectivity of every LIKE-family predicate on an allowed
     column (on a separate connection),
  3. writes SELECTIVITIES_PATH, one line per input line, with the true value
     for matching predicates and PostgreSQL's own value for all others.

Requirements
------------
  * Python >= 3.8 and psycopg2.
  * A PostgreSQL build patched with the cardinality-injection hooks (the GUCs
    ml_cardest_enabled, ml_cardest_fname, query_no, print_single_tbl_queries).
  * The server process must be able to read and write PREDICATES_PATH and
    SELECTIVITIES_PATH (the script sets mode 0o777 on them).
  * The StackOverflow database loaded into PostgreSQL.

Usage
-----
  1. Edit the CONFIG section below (paths, database connection, run counts).

Outputs
-------
  * BASELINE_OUTPUT_CSV / INJECTED_OUTPUT_CSV : per-query timings and plan flags
  * INJECTION_LOG_FILE                        : injected / not-injected predicates per query
  * PLAN_CHANGE_LOG_FILE                      : details of join-order / estimate changes
  * INVALID_LOG_FILE                          : queries PostgreSQL rejects (pre-flight)
  * PLANS_DIR                                 : plan dumps (baseline, injected,
                                                baseline_recheck) as JSON
"""

import os
import re
import csv
import json
import time
import hashlib
import statistics
from collections import Counter

import psycopg2
from psycopg2 import errors


# =============================================================================
# CONFIG
# =============================================================================

# --- Paths (replace every placeholder with a local path) ---------------------

# File: SQL workload, one statement per ';'.
INPUT_SQL_FILE = "/path/to/workload/workload.sql"

# File: per-query results of the baseline configuration (CSV).
# Its parent directory is created automatically.
BASELINE_OUTPUT_CSV = "/path/to/output_dir/postgres_query_timings_baseline.csv"
# File: per-query results of the injected configuration (CSV).
# Its parent directory is created automatically.
INJECTED_OUTPUT_CSV = "/path/to/output_dir/postgres_query_timings_injected.csv"

# Directory: per-query EXPLAIN (FORMAT JSON) dumps, for manual auditing.
PLANS_DIR = "/path/to/plans_dir"
# File: injected / not-injected predicates per query (text log).
INJECTION_LOG_FILE = "/path/to/logs_dir/injected_selectivities_log.txt"
# File: plan-change details per query (text log).
PLAN_CHANGE_LOG_FILE = "/path/to/logs_dir/plan_changes_log.txt"
# File: queries PostgreSQL rejects during pre-flight (text log).
INVALID_LOG_FILE = "/path/to/logs_dir/invalid_queries.log"
PREFLIGHT = True

# File: predicates and PostgreSQL's selectivities, written by the patched server.
# Must be readable and writable by the PostgreSQL server process.
PREDICATES_PATH = os.path.abspath("/path/to/shared_dir/predicates.txt")
# File: selectivities read by the patched server when injection is on.
# Must be readable by the PostgreSQL server process.
SELECTIVITIES_PATH = os.path.abspath("/path/to/shared_dir/selectivities.txt")

# --- Injection scope ---------------------------------------------------------
# The only (table, column) pairs whose LIKE / NOT LIKE / ILIKE selectivities are replaced.
ALLOWED_COLUMNS = {
    ("badges", "name"),
    ("posts", "title"),
    ("users", "displayname"),
    ("users", "location"),
    ("tags", "tagname"),
}

# --- Measurement protocol ----------------------------------------------------
TIMEOUT_MS = 5 * 60_000       # per statement: 5 minutes
RUNS_PER_QUERY = 3
WARMUP_RUNS = 1               # untimed executions before each phase, results discarded
PRINT_PATTERN_MAX = 80

# Noise control (both optional).
FORCE_GEQO_OFF = False        # SET geqo = off in every phase
DISABLE_AUTOVACUUM = False    # ALTER TABLE ... SET (autovacuum_enabled = false)

# --- Database connection (replace with local credentials) --------------------
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "stackover",
    "user": "your_user",
}

REASONS = Counter()           # global injection / miss reasons

# PostgreSQL's internal operator names <-> SQL operators.
PG_OPS = {"~~": "LIKE", "!~~": "NOT LIKE", "~~*": "ILIKE", "!~~*": "NOT ILIKE"}
SQL_TO_PG_OP = {"LIKE": "~~", "NOT LIKE": "!~~", "ILIKE": "~~*", "NOT ILIKE": "!~~*"}


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
    statements, buf = [], []
    in_string = False
    i, n = 0, len(content)
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


def strip_leading_comments(stmt: str) -> str:
    """Remove leading blank and `--` comment lines."""
    lines = stmt.splitlines()
    i = 0
    while i < len(lines) and (not lines[i].strip() or lines[i].strip().startswith("--")):
        i += 1
    return "\n".join(lines[i:])


def sql_unescape(pattern: str) -> str:
    """Undo SQL quote escaping ('' -> ')."""
    return pattern.replace("''", "'")


# =============================================================================
# 2. ALIAS -> TABLE RESOLUTION (single statement only)
#
# Each LIKE predicate has to be attributed to a (table, column) pair. Aliases,
# unaliased tables, and unqualified columns are handled heuristically with
# regular expressions.
# =============================================================================

_RESERVED_ALIASES = {
    "ON", "WHERE", "GROUP", "ORDER", "HAVING", "LIMIT", "OFFSET", "FETCH", "WINDOW",
    "UNION", "EXCEPT", "INTERSECT", "AS", "USING", "JOIN", "LEFT", "RIGHT", "INNER",
    "FULL", "CROSS", "NATURAL", "LATERAL", "QUALIFY", "SET", "OUTER", "SELECT", "FROM",
    "ASC", "DESC", "NULLS",
}
# Inside the regex, so a reserved word is never consumed as an alias
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
        add(table, table)
        if alias and alias.upper() not in _RESERVED_ALIASES:
            add(alias, table)

    # comma joins:  FROM a x, b y   (known tables only, so select-list commas are ignored)
    names = "|".join(sorted(_KNOWN_TABLES, key=len, reverse=True))
    rx2 = re.compile(
        rf",\s*({names})\b(?:\s+(?:AS\s+)?(?!(?:{_RESERVED_RX})\b)([a-zA-Z_]\w*))?",
        re.IGNORECASE)
    for table, alias in rx2.findall(sql):
        add(table, table)
        if alias and alias.upper() not in _RESERVED_ALIASES:
            add(alias, table)
    return amap


def candidate_tables(alias, column, amap):
    """Allowed tables a predicate could belong to; ([], reason) if none."""
    if alias:
        tabs = amap.get(alias.lower())
        if not tabs:
            return [], "alias_unresolved"
    else:
        tabs = {t for ts in amap.values() for t in ts}
        if not tabs:
            return [], "alias_unresolved"
    cands = sorted(t for t in tabs if (t, column) in ALLOWED_COLUMNS)
    if not cands:
        return [], "column_not_allowed"
    return cands, None


# =============================================================================
# 3. PREDICATE PARSING
# =============================================================================

# One line of predicates.txt:  "<lhs>,<postgres selectivity>"  where lhs looks like
#   p.title ~~ 'x'   |   (p.title)::text !~~ 'x'::text   |   title ~~* 'x'
_PRED_LINE_RE = re.compile(
    r"""^\s*\(*\s*
        (?:([a-zA-Z_]\w*)\s*\.\s*)?
        ([a-zA-Z_]\w*)
        \s*\)*\s*(?:::\s*[\w ]+?)?\s*
        (!?~~\*?)\s*
        '((?:[^']|'')*)'
        \s*(?:::\s*[\w ]+?)?\s*\)*\s*$""",
    re.VERBOSE)

# A LIKE / ILIKE predicate in the query text (alias optional).
_QUERY_LIKE_RE = re.compile(
    r"""(?<![\w.])
        (?:([a-zA-Z_]\w*)\s*\.\s*)?
        ([a-zA-Z_]\w*)
        \s+(NOT\s+ILIKE|NOT\s+LIKE|ILIKE|LIKE)\s+
        '((?:[^']|'')*)'
    """,
    re.IGNORECASE | re.VERBOSE)


def parse_query_predicates(stmt):
    """[(label, pgop, column, pattern, reason)] for LIKE predicates on allowed columns."""
    amap = extract_alias_map(stmt)
    out, seen = [], set()
    for m in _QUERY_LIKE_RE.finditer(stmt):
        alias, column, op, raw = m.groups()
        column = column.lower()
        pgop = SQL_TO_PG_OP[" ".join(op.upper().split())]
        pattern = sql_unescape(raw)
        label = f"{alias + '.' if alias else ''}{column}"
        key = (label.lower(), pgop, pattern)
        if key in seen:
            continue
        seen.add(key)
        _, reason = candidate_tables(alias, column, amap)
        if reason is None and re.match(r"\s*ESCAPE\b", stmt[m.end():], re.IGNORECASE):
            reason = "escape_unsupported"
        out.append((label, pgop, column, pattern, reason))
    return out


# =============================================================================
# 4. TRUE SELECTIVITY (computed lazily from the database)
# =============================================================================

class TrueSelectivity:
    """
    selectivity = COUNT(rows where the predicate, AS POSTGRES PRINTED IT, is true) / COUNT(*)
    Counted lazily per (table, column, pattern, operator) on a separate autocommit
    connection, so one failing count can never poison another or the benchmark session.
    NOT LIKE / NOT ILIKE are counted directly (not as 1 - positive), which is correct with NULLs.
    """

    def __init__(self):
        self.conn = psycopg2.connect(**DB_CONFIG)
        self.conn.autocommit = True
        self.cur = self.conn.cursor()
        self.totals = {}
        self.values = {}
        self.hits = 0
        self.misses = 0

    def _total(self, table):
        """Row count of `table` (cached)."""
        if table not in self.totals:
            self.cur.execute(f"SELECT COUNT(*) FROM {table};")
            self.totals[table] = self.cur.fetchone()[0]
        return self.totals[table]

    def get(self, table, column, pattern, pgop):
        """True selectivity of the predicate, or None if the count failed."""
        key = (table, column, pattern, pgop)
        if key not in self.values:
            try:
                total = self._total(table)
                self.cur.execute(
                    f"SELECT COUNT(*) FROM {table} WHERE {column} {PG_OPS[pgop]} %s", (pattern,))
                matched = self.cur.fetchone()[0]
                self.values[key] = matched / total if total > 0 else 0.0
            except Exception as e:
                print(f"      COUNT FAILED for {table}.{column} {PG_OPS[pgop]} "
                      f"'{pattern[:40]}': {type(e).__name__}: {str(e).splitlines()[0]}")
                self.values[key] = None
        value = self.values[key]
        if value is None:
            self.misses += 1
        else:
            self.hits += 1
        return value

    def summary(self):
        """Print hit / miss statistics of all lookups."""
        total = self.hits + self.misses
        pct = self.hits / total * 100 if total else 0.0
        print(f"\n  Selectivity lookup summary: {self.hits}/{total} lookups got a true "
              f"database selectivity ({pct:.1f}%), {self.misses} failed.")

    def close(self):
        """Close the counting connection."""
        try:
            self.cur.close()
            self.conn.close()
        except Exception:
            pass


def _short(p):
    """Shorten a pattern for console output."""
    p = p.replace("\n", " ")
    return p if len(p) <= PRINT_PATTERN_MAX else p[:PRINT_PATTERN_MAX] + "..."


# =============================================================================
# 5. predicates.txt -> selectivities.txt
# =============================================================================

def clear_predicates_file(path):
    """Recreate `path` as an empty, world-writable file."""
    if os.path.exists(path):
        os.remove(path)
    open(path, "w").close()
    os.chmod(path, 0o777)


def compute_selectivities(stmt, truesel):
    """
    One output line per input line of predicates.txt, in the same order (the server reads
    the file line by line). Lines on allowed columns get the TRUE selectivity, everything
    else keeps PostgreSQL's own value.
    Returns (n_injected, missed [(label, op, pattern, reason)], total_lines).
    """
    amap = extract_alias_map(stmt)
    injected_keys = set()
    missed = []
    n_injected = 0
    total_lines = 0

    with open(SELECTIVITIES_PATH, "w", encoding="utf-8") as out:
        if os.path.exists(PREDICATES_PATH):
            with open(PREDICATES_PATH, encoding="utf-8") as f:
                for raw_line in f:
                    line = raw_line.strip()
                    if not line or "," not in line:
                        continue
                    total_lines += 1
                    lhs, _, pg_sel = line.rpartition(",")
                    lhs, k = lhs.strip(), pg_sel.strip()

                    m = _PRED_LINE_RE.match(lhs)
                    if not m:
                        REASONS["non_like_line"] += 1
                        out.write(k + "\n")
                        continue

                    alias, column, pgop, raw_pat = m.groups()
                    column = column.lower()
                    pattern = sql_unescape(raw_pat)
                    label = f"{alias + '.' if alias else ''}{column}"

                    cands, reason = candidate_tables(alias, column, amap)
                    value = None
                    if reason is None:
                        for t in cands:
                            value = truesel.get(t, column, pattern, pgop)
                            if value is not None:
                                break
                        if value is None:
                            reason = "count_failed"

                    if value is None:
                        REASONS[reason] += 1
                        missed.append((label, PG_OPS[pgop], pattern, reason))
                    else:
                        k = f"{value:.10f}"
                        n_injected += 1
                        REASONS["injected"] += 1
                        injected_keys.add((column, pattern, pgop))
                    out.write(k + "\n")

    os.chmod(SELECTIVITIES_PATH, 0o777)

    # LIKE predicates in the query text that the planner never printed (rewritten, folded,
    # in a place the estimator doesn't visit...). These cannot be injected.
    already = {(c, p) for _, _, p, _ in missed for c in [None]}  # patterns already reported
    for label, pgop, column, pattern, reason in parse_query_predicates(stmt):
        if (column, pattern, pgop) in injected_keys:
            continue
        if any(pattern == mp for _, _, mp, _ in missed):
            continue
        why = reason or "not_in_predicates_file"
        REASONS[why] += 1
        missed.append((label, PG_OPS[pgop], pattern, why))

    return n_injected, missed, total_lines


def print_injection_report(n_injected, missed):
    """Print how many LIKE predicates of the current query were injected."""
    print(f"  LIKE predicates: {n_injected + len(missed)} total, "
          f"{n_injected} injected, {len(missed)} NOT injected")
    for label, op, pattern, reason in missed:
        print(f"    NOT injected [{reason}]: {label} {op} '{_short(pattern)}'")


def log_injected(log_path, qn, n_injected, missed, total_lines):
    """Append the injected / not-injected predicates of one query to the log."""
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(f"Query {qn}\n" + "-" * 40 + "\n")
        f.write(f"predicate lines seen by the planner: {total_lines}\n")
        f.write(f"INJECTED: {n_injected}\n")
        f.write(f"NOT INJECTED ({len(missed)}):\n")
        if missed:
            for label, op, pattern, reason in missed:
                f.write(f"  [{reason}] {label} {op} '{pattern}'\n")
        else:
            f.write("  (none)\n")
        f.write("\n")


# =============================================================================
# 6. PLAN EXTRACTION
# =============================================================================

def get_plan_json(cursor, conn, stmt):
    """Plan-only EXPLAIN (nothing is executed). Returns the root 'Plan' dict or None."""
    try:
        cursor.execute(f"EXPLAIN (FORMAT JSON) {stmt}")
        result = cursor.fetchone()
        if not result:
            return None
        document = result[0]
        if isinstance(document, str):
            document = json.loads(document)
        if not document:
            return None
        return document[0].get("Plan")
    except Exception as e:
        print(f"      PLAN EXTRACTION FAILED - {type(e).__name__}: {str(e).splitlines()[0]}")
        conn.rollback()
        return None


def save_plan(plan, query_no, tag):
    """Dump a plan as JSON to PLANS_DIR."""
    if plan is None:
        return
    os.makedirs(PLANS_DIR, exist_ok=True)
    with open(os.path.join(PLANS_DIR, f"q{query_no:04d}_{tag}.json"), "w", encoding="utf-8") as f:
        json.dump(plan, f, indent=2, sort_keys=True)


def capture_predicates(cursor, conn, stmt):
    """
    Writes predicates.txt with a plan-only EXPLAIN (no execution), so it works even when the
    query would time out, and the timing phases never run with printing enabled.
    Returns True if the file has content.
    """
    clear_predicates_file(PREDICATES_PATH)
    try:
        cursor.execute("SET ml_cardest_enabled = False;")
        cursor.execute("SET print_single_tbl_queries = True;")
        cursor.execute(f"EXPLAIN {stmt}")
        cursor.fetchall()
    except Exception as e:
        print(f"      PREDICATE CAPTURE FAILED - {type(e).__name__}: {str(e).splitlines()[0]}")
        conn.rollback()
    finally:
        try:
            cursor.execute("SET print_single_tbl_queries = False;")
        except Exception:
            pass
    return os.path.exists(PREDICATES_PATH) and os.path.getsize(PREDICATES_PATH) > 0


# =============================================================================
# 7. PLAN COMPARISON
# =============================================================================

STRUCTURAL_KEYS = (
    "Join Type", "Strategy", "Parent Relationship", "Relation Name", "Schema", "Alias",
    "Index Name", "Scan Direction", "Operation", "Subplan Name", "CTE Name",
    "Parallel Aware", "Async Capable", "Partial Mode", "Single Copy", "Workers Planned",
    "Local Table Name", "Remote Query", "Inner Unique",
)


def normalize_plan(plan):
    """Whitelist comparison: only the listed structural keys."""
    if not plan:
        return None
    normalized = {"Node Type": plan.get("Node Type")}
    for key in STRUCTURAL_KEYS:
        if key in plan:
            normalized[key] = plan[key]
    normalized["Plans"] = [normalize_plan(c) for c in plan.get("Plans", [])]
    return normalized


VOLATILE_KEYS = {
    "Startup Cost", "Total Cost", "Plan Rows", "Plan Width",
    "Actual Rows", "Actual Loops", "Actual Startup Time", "Actual Total Time",
    "Shared Hit Blocks", "Shared Read Blocks", "Shared Dirtied Blocks",
    "Shared Written Blocks", "Local Hit Blocks", "Local Read Blocks",
    "Temp Read Blocks", "Temp Written Blocks", "Rows Removed by Filter",
}


def strip_plan(plan):
    """Strict comparison: everything except costs / rows / timings."""
    if not plan:
        return None
    out = {k: v for k, v in plan.items() if k not in VOLATILE_KEYS and k != "Plans"}
    out["Plans"] = [strip_plan(c) for c in plan.get("Plans", [])]
    return out


def estimates_of(plan, acc=None):
    """Pre-order (node type, alias/relation, estimated rows) for EVERY node (scans AND joins)."""
    acc = [] if acc is None else acc
    acc.append((plan.get("Node Type"), plan.get("Alias") or plan.get("Relation Name"),
                plan.get("Plan Rows")))
    for child in plan.get("Plans", []):
        estimates_of(child, acc)
    return acc


def join_tree(plan):
    """
    Join order only. Leaves are aliases; every node with 2+ children becomes
    (node type, join type, condition, (child, ...)) in plan order, so a build/probe swap
    counts. Single-child nodes (Hash, Sort, Aggregate, Gather...) are transparent.
    """
    kids = plan.get("Plans") or []
    if not kids:
        return plan.get("Alias") or plan.get("Relation Name") or plan.get("Node Type")
    subs = tuple(join_tree(c) for c in kids)
    if len(subs) == 1:
        return subs[0]
    cond = plan.get("Hash Cond") or plan.get("Merge Cond") or plan.get("Join Filter")
    return (plan.get("Node Type"), plan.get("Join Type"), cond, subs)


def fmt_join_tree(t):
    """Human-readable rendering of a join tree (for the plan-change log)."""
    if not isinstance(t, tuple):
        return str(t)
    name, jtype, cond, kids = t
    inner = " JOIN ".join(fmt_join_tree(k) for k in kids)
    return f"({inner} [{name} {jtype or ''} {cond or ''}])".replace("  ", " ")


def analyze_plan(plan):
    """Derive the comparison views and a structure hash from a plan."""
    if plan is None:
        return None
    strict = strip_plan(plan)
    return {
        "plan": plan,
        "white": normalize_plan(plan),
        "strict": strict,
        "est": estimates_of(plan),
        "join": join_tree(plan),
        "md5": hashlib.md5(json.dumps(strict, sort_keys=True).encode("utf-8")).hexdigest(),
    }


def _cmp(a, b):
    """Three-valued comparison: YES (differ), NO (equal), UNKNOWN (missing)."""
    if a is None or b is None:
        return "UNKNOWN"
    return "YES" if a != b else "NO"


def compare_plans(base, inj):
    """Returns (plan_changed [whitelist], plan_changed_full [strict], join_order_changed, estimates_changed)."""
    if base is None or inj is None:
        return "UNKNOWN", "UNKNOWN", "UNKNOWN", "UNKNOWN"
    return (_cmp(base["white"], inj["white"]), _cmp(base["strict"], inj["strict"]),
            _cmp(base["join"], inj["join"]), _cmp(base["est"], inj["est"]))


def interpret(n_injected, plan_full, join_changed, est_changed):
    """Map the comparison flags to a single interpretation label."""
    if "UNKNOWN" in (plan_full, join_changed, est_changed):
        return "unknown"
    anything = "YES" in (plan_full, join_changed, est_changed)
    if n_injected == 0:
        return "changed_without_injection_UNEXPECTED" if anything else "no_patterns_injected"
    if join_changed == "YES":
        return "join_order_changed"
    if plan_full == "YES":
        return "plan_changed_same_join_order"
    if est_changed == "YES":
        return "same_plan_estimates_changed"
    return "same_plan_estimates_identical_injection_suspect"


def estimate_diffs(base, inj, limit=10):
    """List the first `limit` nodes whose estimated rows changed."""
    b, j = base["est"], inj["est"]
    if len(b) != len(j):
        return ["(plan shapes differ - estimates not aligned)"]
    return [f"{bn[0]} {bn[1] or ''}: {bn[2]} -> {jn[2]}" for bn, jn in zip(b, j) if bn != jn][:limit]


def log_plan_change(log_path, qn, n_injected, flags, base, inj):
    """Append the plan-comparison result of one query to the plan-change log."""
    plan_changed, plan_full, join_changed, est_changed, unstable, interp = flags
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(f"Query {qn}\n" + "-" * 40 + "\n")
        f.write(f"Patterns injected:        {n_injected}\n")
        f.write(f"Join order changed:       {join_changed}\n")
        f.write(f"Plan changed (whitelist): {plan_changed}\n")
        f.write(f"Plan changed (strict):    {plan_full}\n")
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
# 8. EXPLAIN ANALYZE (timing)
# =============================================================================

def run_explain(cursor, conn, stmt):
    """
    Run EXPLAIN (ANALYZE, BUFFERS) and parse planning / execution time (ms).
    Returns (planning_ms, execution_ms, wall_seconds, status) with status in
    'ok' | 'timeout' | 'failed'.
    """
    t0 = time.time()
    try:
        cursor.execute(f"EXPLAIN (ANALYZE, BUFFERS) {stmt}")
        plan_text = "\n".join(row[0] for row in cursor.fetchall())
        plan_time_ms = execution_time_ms = None
        for line in plan_text.splitlines():
            low = line.lower()
            if "planning time:" in low:
                plan_time_ms = float(low.split("planning time:")[1].strip().split()[0])
            if "execution time:" in low:
                execution_time_ms = float(low.split("execution time:")[1].strip().split()[0])
        if plan_time_ms is None or execution_time_ms is None:
            raise ValueError("Could not parse times from EXPLAIN output")
        return plan_time_ms, execution_time_ms, round(time.time() - t0, 2), "ok"
    except errors.QueryCanceled:
        conn.rollback()
        return None, None, round(time.time() - t0, 2), "timeout"
    except Exception as e:
        print(f"      FAILED - {type(e).__name__}: {str(e).splitlines()[0]}")
        conn.rollback()
        return None, None, round(time.time() - t0, 2), "failed"


# =============================================================================
# 9. SESSION SETTINGS
# =============================================================================

def bring_postgres_to_normal_status(cursor):
    """Switch the estimator off and reset all injection-related GUCs."""
    cursor.execute("SET ml_cardest_enabled = False;")
    cursor.execute("SET print_single_tbl_queries = False;")
    cursor.execute("RESET ml_cardest_fname;")
    cursor.execute("RESET query_no;")
    cursor.execute("RESET statement_timeout;")
    if FORCE_GEQO_OFF:
        cursor.execute("SET geqo = off;")


def set_injection_settings(cursor):
    """Single place that turns the injected estimator on (query_no is reset every time)."""
    cursor.execute("SET print_single_tbl_queries = False;")
    cursor.execute("SET ml_cardest_enabled = True;")
    cursor.execute("SET query_no = 0;")
    cursor.execute(f"SET ml_cardest_fname = '{SELECTIVITIES_PATH}';")
    if FORCE_GEQO_OFF:
        cursor.execute("SET geqo = off;")


def apply_mode(cursor, injected):
    """Set the statement timeout and switch injection on or off for a timing run."""
    cursor.execute(f"SET statement_timeout = {TIMEOUT_MS};")
    if injected:
        set_injection_settings(cursor)
    else:
        cursor.execute("SET ml_cardest_enabled = False;")
        cursor.execute("SET print_single_tbl_queries = False;")


def maybe_disable_autovacuum(cursor):
    """Optionally disable autovacuum on all public tables (noise control)."""
    if not DISABLE_AUTOVACUUM:
        return
    cursor.execute("SELECT tablename FROM pg_tables WHERE schemaname = 'public';")
    for (table,) in cursor.fetchall():
        cursor.execute(f'ALTER TABLE public."{table}" SET (autovacuum_enabled = false);')
    print("Autovacuum disabled on all public tables.")


def print_pg_settings(cursor):
    """Print the injection-related GUCs of the current session."""
    print("\n--- PostgreSQL settings ---")
    cursor.execute("""
        SELECT name, setting FROM pg_settings
        WHERE name IN ('print_single_tbl_queries','ml_cardest_enabled','ml_cardest_fname',
                       'query_no','statement_timeout','geqo')
        ORDER BY name;
    """)
    for row in cursor.fetchall():
        print("  ", row)
    print("---------------------------\n")


# =============================================================================
# 10. TIMING PHASE (shared by baseline and injected)
# =============================================================================

def run_timing_phase(cursor, conn, stmt, injected, label):
    """Warm-ups (untimed) then RUNS_PER_QUERY timed runs. Returns (pt, et, wall, timed_out, failed)."""
    R = RUNS_PER_QUERY

    for w in range(WARMUP_RUNS):
        apply_mode(cursor, injected)
        _, _, wall, status = run_explain(cursor, conn, stmt)
        print(f"    {label} warm-up {w + 1}: {status}")
        if status != "ok":
            return ([status] * R, [status] * R, [wall] + [status] * (R - 1),
                    status == "timeout", status != "timeout")

    pt_list, et_list, wall_list = [], [], []
    timed_out = failed = False
    for run in range(R):
        apply_mode(cursor, injected)
        pt, et, wall, status = run_explain(cursor, conn, stmt)
        if status == "ok":
            pt_list.append(pt)
            et_list.append(et)
            wall_list.append(wall)
            print(f"    {label} run {run + 1}: ok (e={et:.1f}ms, pt={pt:.1f}ms)")
        else:
            print(f"    {label} run {run + 1}: {status}")
            timed_out = status == "timeout"
            failed = status != "timeout"
            remaining = R - run - 1
            pt_list.append(status)
            et_list.append(status)
            wall_list.append(wall)
            pt_list.extend([status] * remaining)
            et_list.extend([status] * remaining)
            wall_list.extend([status] * remaining)
            break
    return pt_list, et_list, wall_list, timed_out, failed


# =============================================================================
# 11. PRE-FLIGHT
# =============================================================================

def explain_ok(cursor, conn, stmt):
    """Plan-only check. Returns (ok, first error line)."""
    try:
        cursor.execute(f"EXPLAIN {stmt}")
        cursor.fetchall()
        return True, ""
    except Exception as e:
        conn.rollback()
        return False, f"{type(e).__name__}: {str(e).splitlines()[0]}"[:200]


# =============================================================================
# 12. RESULT ROWS (CSV schema)
# =============================================================================

def build_row(query_no, pt_list, et_list, wall_list, timed_out, failed):
    """Build one CSV row: query number, per-run times, summary statistics, flags."""
    valid_pt = [x for x in pt_list if isinstance(x, (int, float))]
    valid_et = [x for x in et_list if isinstance(x, (int, float))]
    return (
        [query_no] + pt_list + et_list + wall_list
        + [
            sum(valid_et) / len(valid_et) if valid_et else None,
            statistics.median(valid_et) if valid_et else None,
            min(valid_et) if valid_et else None,
            max(valid_et) if valid_et else None,
            statistics.stdev(valid_et) if len(valid_et) >= 2 else None,
            sum(valid_pt) / len(valid_pt) if valid_pt else None,
            statistics.median(valid_pt) if valid_pt else None,
            len(valid_et),
            timed_out,
            failed,
        ]
    )


HEADER_SUFFIX = ["avg_e(ms)", "median_e(ms)", "min_e(ms)", "max_e(ms)", "stdev_e(ms)",
                 "avg_pt(ms)", "median_pt(ms)", "n_valid_runs", "timed_out", "failed"]

PLAN_COLUMNS = [
    "join_order_changed",     # join tree differs (tables, types, conditions, sides)
    "plan_changed",           # whitelist comparison
    "plan_changed_full",      # strict comparison (everything but costs/rows)
    "estimates_changed",      # ANY node's row estimate differs => injection took effect
    "baseline_unstable",      # baseline vs baseline re-check differ => noise
    "n_injected",             # predicate lines given a true selectivity
    "n_like_not_injected",    # LIKE predicates that kept PostgreSQL's own estimate
    "interpretation",
    "baseline_plan_md5",
    "injected_plan_md5",
]


# =============================================================================
# 13. MAIN
# =============================================================================

def main():
    """Run the full benchmark (see the module docstring)."""
    for path in (BASELINE_OUTPUT_CSV, INJECTED_OUTPUT_CSV):
        out_dir = os.path.dirname(path)
        if out_dir:
            os.makedirs(out_dir, exist_ok=True)
    os.makedirs(PLANS_DIR, exist_ok=True)

    with open(INPUT_SQL_FILE, "r", encoding="utf-8") as f:
        statements = split_statements(f.read())
    num_queries = len(statements)
    print(f"\nTOTAL QUERIES FOUND: {num_queries}\n")

    R = RUNS_PER_QUERY
    header = (["Query No"] + [f"pt{r + 1}(ms)" for r in range(R)]
              + [f"e{r + 1}(ms)" for r in range(R)]
              + [f"wall{r + 1}(s)" for r in range(R)]
              + HEADER_SUFFIX + PLAN_COLUMNS)

    open(INJECTION_LOG_FILE, "w").close()
    open(PLAN_CHANGE_LOG_FILE, "w").close()
    invalid_log = open(INVALID_LOG_FILE, "w", encoding="utf-8")

    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True
    cursor = conn.cursor()
    truesel = TrueSelectivity()

    maybe_disable_autovacuum(cursor)
    print_pg_settings(cursor)

    baseline_rows, injected_rows = [], []
    total_timeouts = total_failures = invalid_count = 0
    queries_with_injection = queries_with_missed = 0
    counts = {k: {"YES": 0, "NO": 0, "UNKNOWN": 0} for k in ("white", "full", "join", "est", "unstable")}
    interp_counts = {}
    run_start_all = time.time()

    try:
        for i, raw in enumerate(statements):
            qn = i + 1
            stmt = strip_leading_comments(raw).rstrip().rstrip(";")
            print(f"[Query {qn}/{num_queries}]")
            if not stmt.strip():
                continue

            bring_postgres_to_normal_status(cursor)

            # ---- Pre-flight: does PostgreSQL accept it? ----
            if PREFLIGHT:
                ok, err = explain_ok(cursor, conn, stmt)
                if not ok:
                    invalid_count += 1
                    print(f"  skipped: invalid on PostgreSQL ({err})")
                    invalid_log.write(f"Query {qn}: {err}\n  SQL: {' '.join(stmt.split())[:300]}\n\n")
                    failed_t = ["failed"] * R
                    cols = ["UNKNOWN"] * 4 + ["UNKNOWN", 0, 0, "invalid_on_postgres", "", ""]
                    baseline_rows.append(build_row(qn, failed_t, failed_t, failed_t, False, True) + cols)
                    injected_rows.append(build_row(qn, failed_t, failed_t, failed_t, False, True) + cols)
                    continue

            # ---- Plans (plan-only, always captured, even if timing later fails) ----
            bring_postgres_to_normal_status(cursor)
            baseline_plan = get_plan_json(cursor, conn, stmt)
            save_plan(baseline_plan, qn, "baseline")

            # ---- Predicates seen by the planner -> true selectivities ----
            have_predicates = capture_predicates(cursor, conn, stmt)
            n_injected, missed, total_lines = compute_selectivities(stmt, truesel)
            if not have_predicates:
                REASONS["predicates_file_empty"] += 1
            log_injected(INJECTION_LOG_FILE, qn, n_injected, missed, total_lines)
            n_missed = len(missed)
            if n_injected:
                queries_with_injection += 1
            if n_missed:
                queries_with_missed += 1
            print_injection_report(n_injected, missed)
            inject_on = n_injected > 0   # nothing injected -> injected phase == baseline settings

            # ---- Baseline timing ----
            (b_pt, b_et, b_wall, b_to, b_fail) = run_timing_phase(
                cursor, conn, stmt, injected=False, label="baseline")
            total_timeouts += b_to
            total_failures += b_fail

            # ---- Injected plan + timing ----
            bring_postgres_to_normal_status(cursor)
            if inject_on:
                set_injection_settings(cursor)
            injected_plan = get_plan_json(cursor, conn, stmt)
            save_plan(injected_plan, qn, "injected")

            (j_pt, j_et, j_wall, j_to, j_fail) = run_timing_phase(
                cursor, conn, stmt, injected=inject_on, label="injected")
            total_timeouts += j_to
            total_failures += j_fail

            # ---- Baseline re-check (noise floor), injection off ----
            bring_postgres_to_normal_status(cursor)
            recheck_plan = get_plan_json(cursor, conn, stmt)
            save_plan(recheck_plan, qn, "baseline_recheck")

            # ---- Compare ----
            base, inj, rec = analyze_plan(baseline_plan), analyze_plan(injected_plan), analyze_plan(recheck_plan)
            plan_changed, plan_full, join_changed, est_changed = compare_plans(base, inj)
            if base is None or rec is None:
                unstable = "UNKNOWN"
            else:
                unstable = "YES" if (base["strict"] != rec["strict"] or base["est"] != rec["est"]) else "NO"

            interp = interpret(n_injected, plan_full, join_changed, est_changed)
            if unstable == "YES":
                interp += "_BASELINE_UNSTABLE"

            print(f"    JOIN ORDER CHANGED: {join_changed} | plan: {plan_changed} | full: {plan_full} | "
                  f"estimates: {est_changed} | unstable: {unstable} | {interp}")

            counts["white"][plan_changed] += 1
            counts["full"][plan_full] += 1
            counts["join"][join_changed] += 1
            counts["est"][est_changed] += 1
            counts["unstable"][unstable] += 1
            interp_counts[interp] = interp_counts.get(interp, 0) + 1

            log_plan_change(PLAN_CHANGE_LOG_FILE, qn, n_injected,
                            (plan_changed, plan_full, join_changed, est_changed, unstable, interp), base, inj)

            plan_cols = [join_changed, plan_changed, plan_full, est_changed, unstable,
                         n_injected, n_missed, interp,
                         base["md5"] if base else "", inj["md5"] if inj else ""]
            baseline_rows.append(build_row(qn, b_pt, b_et, b_wall, b_to, b_fail) + plan_cols)
            injected_rows.append(build_row(qn, j_pt, j_et, j_wall, j_to, j_fail) + plan_cols)

        bring_postgres_to_normal_status(cursor)
        truesel.summary()

        print(f"\nTotal benchmark wall time: {(time.time() - run_start_all) / 60:.1f} min")
        print(f"Timeouts: {total_timeouts}   Failures: {total_failures}")
        print(f"Queries with at least one injected predicate: {queries_with_injection}/{num_queries}")
        print(f"Queries with at least one NOT-injected LIKE: {queries_with_missed}/{num_queries}")
        print(f"Skipped as invalid on PostgreSQL: {invalid_count}/{num_queries}")
        print(f"Injection / miss reasons: {dict(REASONS)}")

        print("\n--- PLAN COMPARISON SUMMARY ---")
        print(f"Join order changed:       {counts['join']}")
        print(f"Plan changed (whitelist): {counts['white']}")
        print(f"Plan changed (strict):    {counts['full']}")
        print(f"Estimates changed:        {counts['est']}")
        print(f"Baseline unstable:        {counts['unstable']}")
        print("Interpretation:")
        for k, v in sorted(interp_counts.items()):
            print(f"   {k}: {v}")
        print(f"Full details in: {PLAN_CHANGE_LOG_FILE}")
        print("--------------------------------")

        for path, rows in ((BASELINE_OUTPUT_CSV, baseline_rows), (INJECTED_OUTPUT_CSV, injected_rows)):
            with open(path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(header)
                writer.writerows(rows)
        os.chmod(INJECTED_OUTPUT_CSV, 0o777)

        print("\nDone.")
        print(f"   Total queries:   {num_queries}")
        print(f"   Baseline CSV:    {BASELINE_OUTPUT_CSV}")
        print(f"   Injected CSV:    {INJECTED_OUTPUT_CSV}")
        print(f"   Injection log:   {INJECTION_LOG_FILE}")
        print(f"   Plan change log: {PLAN_CHANGE_LOG_FILE}")
        print(f"   Invalid log:     {INVALID_LOG_FILE}")
        print(f"   Plan dumps:      {PLANS_DIR}/")

    finally:
        invalid_log.close()
        truesel.close()
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()

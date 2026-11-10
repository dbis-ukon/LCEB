#!/usr/bin/env python3
"""
End-to-End Benchmark of Injected LIKE-Pattern Estimates in PostgreSQL
=====================================================================

Supplementary material for the paper
"Do Accurate LIKE-Pattern Cardinality Estimates Matter?"


How injection works
-------------------
The patched PostgreSQL (a) writes the single-table predicates and its own
selectivities of the running query to PREDICATES_PATH when
`print_single_tbl_queries` is on, and (b) reads one selectivity per line from
SELECTIVITIES_PATH when `ml_cardest_enabled` is on. After the baseline phase,
`compute_selectivities` copies the predicate file line by line and replaces
PostgreSQL's selectivity with the injected one wherever the predicate is an
exact LIKE / NOT LIKE match in the injection files.

Requirements
------------
  * Python >= 3.8 and psycopg2.
  * A PostgreSQL build patched with the cardinality-injection hooks (the GUCs
    ml_cardest_enabled, ml_cardest_fname, query_no, print_single_tbl_queries).
  * The server process must be able to read and write PREDICATES_PATH and
    SELECTIVITIES_PATH (the script sets mode 0o777 on them).
  * The StackOverflow database loaded into PostgreSQL.
  * One injection file per (table, column) pair, each line formatted as
    "<pattern>&<selectivity>" (see INJECTION_FILES below).

Usage
-----
  1. Edit the CONFIG section below (paths, database connection, run counts).
  2. Run:  python postgres_injection_benchmark.py

Outputs
-------
  * BASELINE_OUTPUT_CSV / INJECTED_OUTPUT_CSV : per-query timings and plan flags
  * PLANS_DIR                                 : plan dumps (baseline, injected,
                                                baseline_recheck) as JSON
"""

import os
import csv
import time
import statistics
import psycopg2
from psycopg2 import errors
import re
import json
import hashlib

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

# File: predicates and PostgreSQL's selectivities, written by the patched server.
# Must be readable and writable by the PostgreSQL server process.
PREDICATES_PATH = os.path.abspath("/path/to/shared_dir/predicates.txt")
# File: selectivities read by the patched server when injection is on.
# Must be readable by the PostgreSQL server process.
SELECTIVITIES_PATH = os.path.abspath("/path/to/shared_dir/selectivities.txt")

# Directory: contains the estimator's injection files listed below.
BASE = '/path/to/injection_files_dir'

# --- Injection files (estimator output, "<pattern>&<selectivity>" per line) ---
# Each file lives in the BASE directory.
INJECTION_FILES = {
    'badges': f'{BASE}/badges_inject.csv',
    "posts": f"{BASE}/post_inject.csv",
    'users': f'{BASE}/username_inject.csv',
    'user': f'{BASE}/location_inject.csv',
    'tags': f'{BASE}/tag_inject.csv',
}

# Injection file key -> (table, column) the file's patterns belong to.
INJECTION_FILE_TABLE_MAP = {
    "badges": ("badges", "name"),
    "posts":  ("posts", "title"),
    "users":  ("users", "displayname"),
    "user":   ("users", "location"),
    "tags":   ("tags", "tagname"),
}

ALLOWED_COLUMNS = set(INJECTION_FILE_TABLE_MAP.values())

# --- Measurement protocol ----------------------------------------------------
# Per-statement timeout: 5 * 60_000 ms = 5 minutes.
TIMEOUT_MS = 5 * 60_000
RUNS_PER_QUERY = 3
WARMUP_RUNS = 1               # untimed executions before each phase, results discarded

# Noise control (both optional).
FORCE_GEQO_OFF = False        # SET geqo = off in every phase (also affects timing runs)
DISABLE_AUTOVACUUM = False    # ALTER TABLE ... SET (autovacuum_enabled = false) on public tables

# --- Database connection (replace with local credentials) --------------------
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "stackover",
    "user": "your_user",
}


# =============================================================================
# 1. SQL STATEMENT SPLITTING
# =============================================================================

def is_real_query(s):
    """Return True if `s` contains at least one non-empty, non-comment line."""
    s = s.strip()
    if not s:
        return False

    lines = [
        line for line in s.splitlines()
        if line.strip() and not line.strip().startswith("--")
    ]
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
# 2. ALIAS -> TABLE RESOLUTION
# =============================================================================

_RESERVED_ALIASES = {
    "ON", "WHERE", "GROUP", "ORDER", "HAVING", "LIMIT",
    "UNION", "EXCEPT", "INTERSECT", "AS", "USING",
}


def extract_alias_map(sql):
    """alias -> table name, for every `FROM|JOIN table [AS] alias` in `sql`."""
    alias_map = {}

    regex = re.compile(
        r"""
        \b(?:FROM|JOIN)
        \s+
        ([a-zA-Z_][a-zA-Z0-9_]*)
        (?:\s+AS)?
        \s+
        ([a-zA-Z_][a-zA-Z0-9_]*)
        """,
        re.IGNORECASE | re.VERBOSE
    )

    for table, alias in regex.findall(sql):
        if alias.upper() in _RESERVED_ALIASES:
            continue

        alias_map[alias.lower()] = table.lower()

    return alias_map


# Matches one line of the predicate file's left-hand side, e.g.  t.name ~~ 'abc%'
# (~~ is PostgreSQL's internal LIKE operator, !~~ is NOT LIKE).
_PRED_LIKE_RE = re.compile(
    r"""
    ^\s*
    ([a-zA-Z_][a-zA-Z0-9_]*)
    \.
    ([a-zA-Z_][a-zA-Z0-9_]*)
    \s*
    (!?~~)
    \s*
    '(.*)'
    \s*$
    """,
    re.VERBOSE
)


# =============================================================================
# 3. INJECTION LOOKUP (estimator output files)
# =============================================================================

class SelectivityLookup:
    """
    In-memory index of the injection files, keyed by (table, column) and then
    by the exact pattern string. A predicate is injected only on an exact match.
    """

    def __init__(self, files: dict, table_map: dict):
        self.lookups = {}
        self.hits = 0
        self.misses = 0

        for file_key, file_path in files.items():
            table_column = table_map.get(file_key)

            if table_column is None:
                print(
                    f"  [WARN] no table/column mapping for "
                    f"'{file_key}' — skipping {file_path}"
                )
                continue

            pattern_dict = self._load_dict(file_path)
            self.lookups[table_column] = pattern_dict

            print(
                f"  loaded {len(pattern_dict)} unique pattern(s) "
                f"from {file_key} -> {table_column}"
            )

    @staticmethod
    def _load_dict(file_path):
        """Read "<pattern>&<selectivity>" lines into {pattern: value}."""
        d = {}
        dup_count = 0
        skipped = 0

        with open(file_path, encoding="utf-8") as f:
            for line in f:
                parts = line.rstrip("\r\n").split("&")

                # Need at least a pattern and a value.
                if len(parts) < 2:
                    skipped += 1
                    continue

                # Last element is the value; everything before it is the pattern.
                # Re-join with "&" so a pattern that contained "&" is restored exactly.
                value = parts[-1]
                pattern = "".join(parts[:-1])

                if pattern in d:
                    dup_count += 1

                d[pattern ] = value

        if skipped:
            print(f"    [WARN] {skipped} line(s) without a delimiter skipped in {file_path}")

        if dup_count:
            print(
                f"    [WARN] {dup_count} duplicate pattern(s) "
                f"in {file_path}, last value kept"
            )

        return d

    def lookup(self, table, column, pattern):
        """Injected selectivity (as a string) for the exact pattern, or None."""
        col_dict = self.lookups.get((table, column))

        if col_dict is None:
            self.misses += 1
            return None

        value = col_dict.get(pattern)

        if value is None:
            self.misses += 1
            return None

        self.hits += 1
        return value

    def summary(self):
        """Print hit / miss statistics of all lookups."""
        total = self.hits + self.misses
        pct = self.hits / total * 100 if total else 0.0

        print(
            f"\n  Selectivity lookup summary: "
            f"{self.hits}/{total} predicate(s) matched "
            f"an exact injected pattern ({pct:.1f}%), "
            f"{self.misses} fell back to PostgreSQL's own estimate."
        )


# =============================================================================
# 4. COMPUTE SELECTIVITIES (build the file read by the patched server)
# =============================================================================

def compute_selectivities(
    predicates_path: str,
    selectivities_path: str,
    lookup: SelectivityLookup,
    stmt: str = None,
):
    """
    Copy the predicate file (written by the server during the baseline run)
    into the selectivity file, one value per line, replacing PostgreSQL's
    selectivity with the injected one for exact LIKE / NOT LIKE matches
    (NOT LIKE uses 1 - selectivity). Lines whose alias does not occur in `stmt`
    are dropped; unmatched predicates keep PostgreSQL's own selectivity.
    """
    stmt_lower = stmt.lower() if stmt else None
    alias_to_table = extract_alias_map(stmt) if stmt else {}

    hit_count = 0
    fallback_count = 0
    total_lines = 0

    with open(selectivities_path, "w", encoding="utf-8") as newfile:
        if os.path.exists(predicates_path):
            with open(predicates_path, encoding="utf-8") as f:
                for raw_line in f:
                    line = raw_line.strip()

                    if not line or "," not in line:
                        continue

                    total_lines += 1

                    lhs, _, pg_selectivity = line.rpartition(",")
                    lhs = lhs.strip()
                    pg_selectivity = pg_selectivity.strip()

                    if stmt_lower and "." in lhs:
                        alias = lhs.split(".", 1)[0].strip().lower()

                        if f"{alias}." not in stmt_lower:
                            continue

                    k = pg_selectivity

                    m = _PRED_LIKE_RE.match(lhs)

                    if m:
                        p_alias, column, op, pattern = m.groups()
                        table = alias_to_table.get(p_alias)

                        if (
                            table is not None
                            and (table, column) in ALLOWED_COLUMNS
                        ):
                            value = lookup.lookup(
                                table, column, pattern
                            )

                            if value is not None:
                                val = float(value)

                                if op == "!~~":
                                    val = 1.0 - val

                                k = f"{val:.10f}"
                                hit_count += 1
                            else:
                                fallback_count += 1
                        else:
                            fallback_count += 1

                    newfile.write(str(k) + "\n")

    if fallback_count:
        print(
            f"    [compute_selectivities] {hit_count} injected, "
            f"{fallback_count}/{total_lines} fell back "
            f"to PostgreSQL estimate."
        )

    os.chmod(selectivities_path, 0o777)


# =============================================================================
# 5. FILE HELPERS
# =============================================================================

def clear_predicates_file(path: str):
    """Recreate `path` as an empty, world-writable file."""
    if os.path.exists(path):
        os.remove(path)

    open(path, "w").close()
    os.chmod(path, 0o777)


def remove_file_if_exists(path: str):
    """Delete `path` if it exists."""
    if os.path.exists(path):
        os.remove(path)


# =============================================================================
# 6. PLAN EXTRACTION
# =============================================================================

def get_plan_json(cursor, conn, stmt):
    """Plan-only EXPLAIN (no execution). Returns the root 'Plan' dict or None."""
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
        print(f"      PLAN EXTRACTION FAILED — {type(e).__name__}: {e}")
        conn.rollback()
        return None


def save_plan(plan, query_no, tag):
    """Dump a plan as JSON to PLANS_DIR."""
    if plan is None:
        return

    os.makedirs(PLANS_DIR, exist_ok=True)
    path = os.path.join(PLANS_DIR, f"q{query_no:04d}_{tag}.json")

    with open(path, "w", encoding="utf-8") as f:
        json.dump(plan, f, indent=2, sort_keys=True)


# =============================================================================
# 7. PLAN COMPARISON
# =============================================================================

# (A) Whitelist comparison: only the listed structural keys.
STRUCTURAL_KEYS = (
    "Join Type",
    "Strategy",
    "Parent Relationship",
    "Relation Name",
    "Schema",
    "Alias",
    "Index Name",
    "Scan Direction",
    "Operation",
    "Subplan Name",
    "CTE Name",
    "Parallel Aware",
    "Async Capable",
    "Partial Mode",
    "Single Copy",
    "Workers Planned",
    "Local Table Name",
    "Remote Query",
    "Inner Unique",
)


def normalize_plan(plan):
    """Reduce a plan tree to its whitelisted structural keys."""
    if not plan:
        return None

    normalized = {"Node Type": plan.get("Node Type")}

    for key in STRUCTURAL_KEYS:
        if key in plan:
            normalized[key] = plan[key]

    normalized["Plans"] = [
        normalize_plan(child) for child in plan.get("Plans", [])
    ]

    return normalized


# (B) Strict comparison: keep everything except volatile numeric keys.
#     This also catches changes in Hash Cond, Index Cond, Filter, Sort Key, etc.
VOLATILE_KEYS = {
    "Startup Cost", "Total Cost", "Plan Rows", "Plan Width",
    "Actual Rows", "Actual Loops", "Actual Startup Time", "Actual Total Time",
    "Shared Hit Blocks", "Shared Read Blocks", "Shared Dirtied Blocks",
    "Shared Written Blocks", "Local Hit Blocks", "Local Read Blocks",
    "Temp Read Blocks", "Temp Written Blocks", "Rows Removed by Filter",
}


def strip_plan(plan):
    """Remove costs, row counts, and timings from a plan tree."""
    if not plan:
        return None

    out = {
        k: v for k, v in plan.items()
        if k not in VOLATILE_KEYS and k != "Plans"
    }
    out["Plans"] = [strip_plan(c) for c in plan.get("Plans", [])]
    return out


def plan_md5(plan):
    """MD5 of the stripped plan (empty string if there is no plan)."""
    if plan is None:
        return ""

    blob = json.dumps(strip_plan(plan), sort_keys=True)
    return hashlib.md5(blob.encode("utf-8")).hexdigest()


def _compare(a, b):
    """Three-valued comparison: YES (differ), NO (equal), UNKNOWN (missing)."""
    if a is None or b is None:
        return "UNKNOWN"
    return "YES" if a != b else "NO"


def compare_plans(baseline_plan, injected_plan):
    """Whitelist-based structural comparison."""
    return _compare(
        normalize_plan(baseline_plan) if baseline_plan else None,
        normalize_plan(injected_plan) if injected_plan else None,
    )


def compare_plans_full(baseline_plan, injected_plan):
    """Strict comparison: everything except costs/rows/timings."""
    return _compare(
        strip_plan(baseline_plan) if baseline_plan else None,
        strip_plan(injected_plan) if injected_plan else None,
    )


# =============================================================================
# 8. ESTIMATE COMPARISON
# =============================================================================

def collect_scan_estimates(plan, acc=None):
    """Sorted list of (alias, estimated rows) for every scan node, plus root rows."""
    top = acc is None
    acc = [] if acc is None else acc

    if "Alias" in plan and "Scan" in plan.get("Node Type", ""):
        acc.append((plan["Alias"], plan.get("Plan Rows")))

    for child in plan.get("Plans", []):
        collect_scan_estimates(child, acc)

    if top:
        return sorted(acc, key=lambda x: (str(x[0]), x[1] if x[1] is not None else -1))

    return acc


def compare_estimates(baseline_plan, injected_plan):
    """
    YES = estimated rows changed on at least one scan node or at the root,
          i.e. injection had an observable effect.
    NO  = identical estimates, injection probably did not apply.
    """
    if baseline_plan is None or injected_plan is None:
        return "UNKNOWN"

    b = (collect_scan_estimates(baseline_plan), baseline_plan.get("Plan Rows"))
    j = (collect_scan_estimates(injected_plan), injected_plan.get("Plan Rows"))

    return "YES" if b != j else "NO"


def interpret(plan_changed_full, estimates_changed):
    """Map the comparison flags to a single interpretation label."""
    if "UNKNOWN" in (plan_changed_full, estimates_changed):
        return "unknown"

    if plan_changed_full == "YES":
        return "plan_changed"

    if estimates_changed == "YES":
        return "same_plan_estimates_changed"

    return "same_plan_estimates_identical_injection_suspect"


# =============================================================================
# 9. EXPLAIN ANALYZE HELPER
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
        plan_results = cursor.fetchall()

        plan_text = "\n".join(row[0] for row in plan_results)

        plan_time_ms = None
        execution_time_ms = None

        for line in plan_text.splitlines():
            line_lower = line.lower()

            if "planning time:" in line_lower:
                plan_time_ms = float(
                    line_lower.split("planning time:")[1].strip().split()[0]
                )

            if "execution time:" in line_lower:
                execution_time_ms = float(
                    line_lower.split("execution time:")[1].strip().split()[0]
                )

        if plan_time_ms is None or execution_time_ms is None:
            raise ValueError("Could not parse times from EXPLAIN output")

        wall = round(time.time() - t0, 2)

        return plan_time_ms, execution_time_ms, wall, "ok"

    except errors.QueryCanceled:
        wall = round(time.time() - t0, 2)
        conn.rollback()
        return None, None, wall, "timeout"

    except Exception as e:
        wall = round(time.time() - t0, 2)

        print(f"      FAILED — {type(e).__name__}: {e}")

        conn.rollback()
        return None, None, wall, "failed"


# =============================================================================
# 10. RESULT ROWS (CSV schema)
# =============================================================================

def build_row(query_no, pt_list, et_list, wall_list, timed_out, failed):
    """Build one CSV row: query number, per-run times, summary statistics, flags."""
    valid_pt = [x for x in pt_list if isinstance(x, (int, float))]
    valid_et = [x for x in et_list if isinstance(x, (int, float))]

    avg_pt = sum(valid_pt) / len(valid_pt) if valid_pt else None
    median_pt = statistics.median(valid_pt) if valid_pt else None

    avg_et = sum(valid_et) / len(valid_et) if valid_et else None
    median_et = statistics.median(valid_et) if valid_et else None
    min_et = min(valid_et) if valid_et else None
    max_et = max(valid_et) if valid_et else None

    stdev_et = statistics.stdev(valid_et) if len(valid_et) >= 2 else None

    return (
        [query_no]
        + pt_list
        + et_list
        + wall_list
        + [
            avg_et,
            median_et,
            min_et,
            max_et,
            stdev_et,
            avg_pt,
            median_pt,
            len(valid_et),
            timed_out,
            failed,
        ]
    )


HEADER_SUFFIX = [
    "avg_e(ms)",
    "median_e(ms)",
    "min_e(ms)",
    "max_e(ms)",
    "stdev_e(ms)",
    "avg_pt(ms)",
    "median_pt(ms)",
    "n_valid_runs",
    "timed_out",
    "failed",
]

PLAN_COLUMNS = [
    "plan_changed",          # whitelist comparison
    "plan_changed_full",     # strict comparison (everything but costs/rows)
    "estimates_changed",     # scan/root row estimates differ => injection took effect
    "baseline_unstable",     # baseline vs baseline re-check differ => noise
    "interpretation",
    "baseline_plan_md5",
    "injected_plan_md5",
]


# =============================================================================
# 11. SESSION SETTINGS
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


def reset_session_state_for_new_query(cursor):
    """Reset the session before each query."""
    bring_postgres_to_normal_status(cursor)


def set_injection_settings(cursor):
    """Single place that turns the injected estimator on."""
    cursor.execute("SET print_single_tbl_queries = False;")
    cursor.execute("SET ml_cardest_enabled = True;")
    cursor.execute("SET query_no = 0;")
    cursor.execute(f"SET ml_cardest_fname = '{SELECTIVITIES_PATH}';")

    if FORCE_GEQO_OFF:
        cursor.execute("SET geqo = off;")


def maybe_disable_autovacuum(cursor):
    """Optionally disable autovacuum on all public tables (noise control)."""
    if not DISABLE_AUTOVACUUM:
        return

    cursor.execute(
        "SELECT tablename FROM pg_tables WHERE schemaname = 'public';"
    )

    for (table,) in cursor.fetchall():
        cursor.execute(
            f'ALTER TABLE public."{table}" SET (autovacuum_enabled = false);'
        )

    print("Autovacuum disabled on all public tables.")


# =============================================================================
# 12. WARM-UP
# =============================================================================

def run_warmup(cursor, conn, stmt, injected):
    """
    Untimed warm-up executions so that both phases start from a warm cache.
    Uses the same settings as the phase it precedes. Results are discarded.
    Returns (status, wall) where status is 'ok', 'timeout' or 'failed'.
    """
    for w in range(WARMUP_RUNS):
        cursor.execute(f"SET statement_timeout = {TIMEOUT_MS};")

        if injected:
            set_injection_settings(cursor)
        else:
            cursor.execute("SET ml_cardest_enabled = False;")
            cursor.execute("SET print_single_tbl_queries = True;")
            clear_predicates_file(PREDICATES_PATH)

        pt, et, wall, status = run_explain(cursor, conn, stmt)

        label = "injected" if injected else "baseline"
        print(f"    {label} warm-up {w + 1}: {status}")

        if status != "ok":
            return status, wall

    return "ok", None


# =============================================================================
# 13. BASELINE PHASE
# =============================================================================

def run_baseline_phase(cursor, conn, stmt):
    """
    Warm-cache timing of one query with PostgreSQL's native estimates. The
    patched server records its predicates and selectivities in PREDICATES_PATH
    during these runs; the last successful recording is kept for the injection
    phase. Returns (pt_list, et_list, wall_list, timed_out, failed, any_success).
    """
    pt_list, et_list, wall_list = [], [], []

    timed_out = False
    failed = False
    any_success = False
    last_good_predicates = None

    if WARMUP_RUNS > 0:
        w_status, w_wall = run_warmup(cursor, conn, stmt, injected=False)

        if w_status != "ok":
            if w_status == "timeout":
                timed_out = True
            else:
                failed = True

            pt_list = ["timeout"] * RUNS_PER_QUERY
            et_list = ["timeout"] * RUNS_PER_QUERY
            wall_list = [w_wall] + ["timeout"] * (RUNS_PER_QUERY - 1)

            return pt_list, et_list, wall_list, timed_out, failed, False

        # The warm-up wrote predicates.txt; keep it in case a timed run fails.
        if os.path.exists(PREDICATES_PATH):
            with open(PREDICATES_PATH, "r", encoding="utf-8") as pf:
                last_good_predicates = pf.read()

    for run in range(RUNS_PER_QUERY):
        cursor.execute(f"SET statement_timeout = {TIMEOUT_MS};")
        cursor.execute("SET ml_cardest_enabled = False;")
        cursor.execute("SET print_single_tbl_queries = True;")

        clear_predicates_file(PREDICATES_PATH)

        pt, et, wall, status = run_explain(cursor, conn, stmt)

        if status == "ok":
            pt_list.append(pt)
            et_list.append(et)
            wall_list.append(wall)

            any_success = True

            print(
                f"    baseline run {run + 1}: ok "
                f"(e={et:.1f}ms, pt={pt:.1f}ms)"
            )

            if os.path.exists(PREDICATES_PATH):
                with open(PREDICATES_PATH, "r", encoding="utf-8") as pf:
                    last_good_predicates = pf.read()

        else:
            pt_list.append("timeout")
            et_list.append("timeout")
            wall_list.append(wall)

            print(f"    baseline run {run + 1}: {status}")

            if status == "timeout":
                timed_out = True
            else:
                failed = True

            remaining = RUNS_PER_QUERY - run - 1

            pt_list.extend(["timeout"] * remaining)
            et_list.extend(["timeout"] * remaining)
            wall_list.extend(["timeout"] * remaining)

            break

    if last_good_predicates is not None:
        with open(PREDICATES_PATH, "w", encoding="utf-8") as pf:
            pf.write(last_good_predicates)

        os.chmod(PREDICATES_PATH, 0o777)

    return pt_list, et_list, wall_list, timed_out, failed, any_success


# =============================================================================
# 14. INJECTION PHASE
# =============================================================================

def run_injection_phase(cursor, conn, stmt):
    """
    Warm-cache timing of one query with the injected selectivities
    (SELECTIVITIES_PATH). Returns (pt_list, et_list, wall_list, timed_out, failed).
    """
    pt_list, et_list, wall_list = [], [], []

    timed_out = False
    failed = False

    if WARMUP_RUNS > 0:
        w_status, w_wall = run_warmup(cursor, conn, stmt, injected=True)

        if w_status != "ok":
            if w_status == "timeout":
                timed_out = True
            else:
                failed = True

            pt_list = ["timeout"] * RUNS_PER_QUERY
            et_list = ["timeout"] * RUNS_PER_QUERY
            wall_list = [w_wall] + ["timeout"] * (RUNS_PER_QUERY - 1)

            cursor.execute("SET print_single_tbl_queries = False;")
            return pt_list, et_list, wall_list, timed_out, failed

    for run in range(RUNS_PER_QUERY):
        cursor.execute(f"SET statement_timeout = {TIMEOUT_MS};")
        set_injection_settings(cursor)

        pt, et, wall, status = run_explain(cursor, conn, stmt)

        if status == "ok":
            pt_list.append(pt)
            et_list.append(et)
            wall_list.append(wall)

            print(
                f"    injected run {run + 1}: ok "
                f"(e={et:.1f}ms, pt={pt:.1f}ms)"
            )

        else:
            pt_list.append("timeout")
            et_list.append("timeout")
            wall_list.append(wall)

            print(f"    injected run {run + 1}: {status}")

            if status == "timeout":
                timed_out = True
            else:
                failed = True

            remaining = RUNS_PER_QUERY - run - 1

            pt_list.extend(["timeout"] * remaining)
            et_list.extend(["timeout"] * remaining)
            wall_list.extend(["timeout"] * remaining)

            break

    cursor.execute("SET print_single_tbl_queries = False;")

    return pt_list, et_list, wall_list, timed_out, failed


# =============================================================================
# 15. POSTGRESQL SETTINGS (diagnostics)
# =============================================================================

def print_pg_settings(cursor, label):
    """Print the injection-related GUCs of the current session."""
    print(f"\n--- PostgreSQL settings ({label}) ---")

    cursor.execute("""
        SELECT name, setting, source, context
        FROM pg_settings
        WHERE name IN (
            'print_single_tbl_queries',
            'ml_cardest_enabled',
            'ml_cardest_fname',
            'query_no',
            'statement_timeout',
            'geqo'
        )
        ORDER BY name;
    """)

    for row in cursor.fetchall():
        print(row)

    print("-----------------------------------\n")


# =============================================================================
# 16. MAIN
# =============================================================================

def main():
    """Run the full benchmark (see the module docstring)."""
    for path in (BASELINE_OUTPUT_CSV, INJECTED_OUTPUT_CSV):
        out_dir = os.path.dirname(path)

        if out_dir:
            os.makedirs(out_dir, exist_ok=True)

    os.makedirs(PLANS_DIR, exist_ok=True)

    with open(INPUT_SQL_FILE, "r", encoding="utf-8") as f:
        content = f.read()

    statements = split_statements(content)
    num_queries = len(statements)

    print(f"\nTOTAL QUERIES FOUND: {num_queries}\n")

    print("Loading selectivity lookup tables...")

    lookup = SelectivityLookup(INJECTION_FILES, INJECTION_FILE_TABLE_MAP)

    print()

    pt_cols = [f"pt{r + 1}(ms)" for r in range(RUNS_PER_QUERY)]
    e_cols = [f"e{r + 1}(ms)" for r in range(RUNS_PER_QUERY)]
    wall_cols = [f"wall{r + 1}(s)" for r in range(RUNS_PER_QUERY)]

    header = (
        ["Query No"]
        + pt_cols
        + e_cols
        + wall_cols
        + HEADER_SUFFIX
        + PLAN_COLUMNS
    )

    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True
    cursor = conn.cursor()

    maybe_disable_autovacuum(cursor)

    baseline_rows = []
    injected_rows = []

    total_timeouts = 0
    total_failures = 0

    counts = {"YES": 0, "NO": 0, "UNKNOWN": 0}
    full_counts = {"YES": 0, "NO": 0, "UNKNOWN": 0}
    est_counts = {"YES": 0, "NO": 0, "UNKNOWN": 0}
    unstable_counts = {"YES": 0, "NO": 0, "UNKNOWN": 0}
    interp_counts = {}

    run_start_all = time.time()

    try:
        for i, stmt in enumerate(statements):
            qn = i + 1
            print(f"[Query {qn}/{num_queries}]")

            if not stmt.strip():
                continue

            # ----------------------------------
            # Phase 1: Baseline timing
            # ----------------------------------

            reset_session_state_for_new_query(cursor)

            (
                b_pt, b_et, b_wall,
                b_timed_out, b_failed, baseline_ok,
            ) = run_baseline_phase(cursor, conn, stmt)

            if b_timed_out:
                total_timeouts += 1

            if b_failed:
                total_failures += 1

            # Baseline plan, estimator off.
            bring_postgres_to_normal_status(cursor)

            baseline_plan = None

            if baseline_ok:
                baseline_plan = get_plan_json(cursor, conn, stmt)
                save_plan(baseline_plan, qn, "baseline")

            # ----------------------------------
            # Phase 2: Prepare injected estimates
            # ----------------------------------

            bring_postgres_to_normal_status(cursor)

            if not baseline_ok:
                print(
                    "    baseline never succeeded — "
                    "attempting injection anyway."
                )

            compute_selectivities(
                PREDICATES_PATH,
                SELECTIVITIES_PATH,
                lookup,
                stmt=stmt,
            )

            # ----------------------------------
            # Phase 3: Injection timing runs
            # ----------------------------------

            (
                j_pt, j_et, j_wall,
                j_timed_out, j_failed,
            ) = run_injection_phase(cursor, conn, stmt)

            if j_timed_out:
                total_timeouts += 1

            if j_failed:
                total_failures += 1

            # Injected plan: re-apply injection settings explicitly so the
            # capture does not depend on leftover session state.
            set_injection_settings(cursor)
            injected_plan = get_plan_json(cursor, conn, stmt)
            save_plan(injected_plan, qn, "injected")

            # ----------------------------------
            # Phase 4: Baseline re-check (noise floor)
            # Same settings as the baseline capture, taken AFTER the
            # injected run. A difference means the plan drifted for
            # reasons unrelated to injection (autoanalyze, GEQO...).
            # ----------------------------------

            bring_postgres_to_normal_status(cursor)

            baseline_recheck = None

            if baseline_ok:
                baseline_recheck = get_plan_json(cursor, conn, stmt)
                save_plan(baseline_recheck, qn, "baseline_recheck")

            # ----------------------------------
            # Phase 5: Compare
            # ----------------------------------

            plan_changed = compare_plans(baseline_plan, injected_plan)
            plan_changed_full = compare_plans_full(baseline_plan, injected_plan)
            estimates_changed = compare_estimates(baseline_plan, injected_plan)
            baseline_unstable = compare_plans_full(baseline_plan, baseline_recheck)
            interpretation = interpret(plan_changed_full, estimates_changed)

            if baseline_unstable == "YES":
                interpretation += "_BASELINE_UNSTABLE"

            print(
                f"    PLAN CHANGED: {plan_changed} | full: {plan_changed_full} | "
                f"estimates changed: {estimates_changed} | "
                f"baseline unstable: {baseline_unstable} | {interpretation}"
            )

            counts[plan_changed] += 1
            full_counts[plan_changed_full] += 1
            est_counts[estimates_changed] += 1
            unstable_counts[baseline_unstable] += 1
            interp_counts[interpretation] = interp_counts.get(interpretation, 0) + 1

            bring_postgres_to_normal_status(cursor)

            # ----------------------------------
            # Phase 6: Build CSV rows
            # ----------------------------------

            baseline_row = build_row(
                qn, b_pt, b_et, b_wall, b_timed_out, b_failed
            )
            injected_row = build_row(
                qn, j_pt, j_et, j_wall, j_timed_out, j_failed
            )

            plan_cols = [
                plan_changed,
                plan_changed_full,
                estimates_changed,
                baseline_unstable,
                interpretation,
                plan_md5(baseline_plan),
                plan_md5(injected_plan),
            ]

            baseline_row.extend(plan_cols)
            injected_row.extend(plan_cols)

            baseline_rows.append(baseline_row)
            injected_rows.append(injected_row)

        bring_postgres_to_normal_status(cursor)

        lookup.summary()

        total_wall_all = time.time() - run_start_all

        print(f"\nTotal benchmark wall time: {total_wall_all / 60:.1f} min")
        print(f"Timeouts: {total_timeouts}   Failures: {total_failures}")

        print("\n--- PLAN COMPARISON SUMMARY ---")
        print(f"Whitelist  plan_changed:      {counts}")
        print(f"Strict     plan_changed_full: {full_counts}")
        print(f"Estimates  changed:           {est_counts}")
        print(f"Baseline   unstable (noise):  {unstable_counts}")
        print("Interpretation:")
        for k, v in sorted(interp_counts.items()):
            print(f"   {k}: {v}")
        print("--------------------------------")

        with open(BASELINE_OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(header)
            writer.writerows(baseline_rows)

        with open(INJECTED_OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(header)
            writer.writerows(injected_rows)

        os.chmod(INJECTED_OUTPUT_CSV, 0o777)

        print("\nDone.")
        print(f"   Total queries: {num_queries}")
        print(f"   Baseline CSV:  {BASELINE_OUTPUT_CSV}")
        print(f"   Injected CSV:  {INJECTED_OUTPUT_CSV}")
        print(f"   Plan dumps:    {PLANS_DIR}/")

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()

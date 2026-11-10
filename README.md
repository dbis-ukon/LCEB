# LIKE-Pattern Cardinality Estimation Benchmark (LCEB)

Cardinality estimation is critical for cost-based query optimization, and estimating LIKE-query cardinality is especially challenging, motivating a growing body of specialized methods that report high accuracy. Yet no benchmark systematically evaluates whether this accuracy translates into better query optimization. We introduce the LIKE-Pattern Cardinality Estimation Benchmark (LCEB), integrate existing LIKE-pattern cardinality estimation methods into PostgreSQL and DuckDB, and evaluate their practical impact on query plan quality and execution performance.

## 1. To Generate SQL Queries

We use [SQLStorm](https://github.com/SQL-Storm/SQLStorm) to generate the SQL query workload for our benchmark. SQLStorm leverages Large Language Models (LLMs) to automatically generate diverse SQL queries from a given database schema. Please follow the setup instructions in the repository's `README.md` to run it.

We use SQLStorm's source code with 8 prompts (its 7 predefined prompts, P1–P7, plus one additional prompt we introduce for single-table query generation) to generate our sample SQL queries.

### 1.1 Enriching Generated Queries

After generating the base SQL queries with SQLStorm, we further enrich them using `enrich_queries.py` to increase predicate diversity and complexity. The script uses an LLM to add additional realistic filtering predicates to each query's `WHERE` clause, following a strict set of schema-safety, type-consistency, and structural rules, without modifying the `SELECT` clause, join structure, or existing predicates. Each enriched query is then passed through a second, self-review pass with the same model to catch and fix common validity issues before being written to disk.

To run it, set your OpenAI API key as an environment variable and update the input/output/schema paths at the top of the script:

```bash
export OPENAI_API_KEY="your-key-here"
python3 enrich_sql_queries.py
```

## 2. How to Setup DBMS

### 2.1 PostgreSQL 18.0

`postgres18_like_predicate_injection.patch` extends PostgreSQL 18.0's query optimizer to accept externally supplied selectivity estimates for single-relation restriction clauses (e.g. `LIKE`-predicates), and to log the selectivities PostgreSQL's own estimator would otherwise use.

#### 2.1.1 Build

```bash
git clone https://github.com/postgres/postgres.git
cd postgres
git checkout REL_18_0
patch -p1 < postgres18_like_predicate_injection.patch
./configure --prefix=/usr/local/pgsql/18.0 --enable-depend --enable-cassert --enable-debug CFLAGS="-ggdb -O0"
make && sudo make install
echo 'export PATH=/usr/local/pgsql/18.0/bin:$PATH' >> ~/.bashrc
echo 'export LD_LIBRARY_PATH=/usr/local/pgsql/18.0/lib/:$LD_LIBRARY_PATH' >> ~/.bashrc
source ~/.bashrc
initdb -D $PSQL_DATA_DIRECTORY
postgres -D $PSQL_DATA_DIRECTORY
```

#### 2.1.2 Integration of Estimation into PostgreSQL

To enable PostgreSQL to use estimates from different cardinality estimation methods, you should trigger some knobs in the clients of PostgreSQL.

#### 2.1.3 Get PostgreSQL Estimates

```sql
stats=# SET print_single_tbl_queries = True;
```

#### 2.1.4 Selectivity Injection

```sql
stats=# SET ml_cardest_enabled = on;
stats=# SET query_no = 0;
stats=# SET ml_cardest_fname = '/absolute/path/to/selectivities.txt';
```

---

### 2.2 DuckDB v1.6.0-dev11215

`duckdb_1.6.0_like_predicate_injection.patch` extends DuckDB to accept externally supplied selectivity estimates for `LIKE`-predicates, and reads them from a plain text file during query optimization.

#### 2.2.1 Build

```bash
git clone https://github.com/duckdb/duckdb.git
cd duckdb
git checkout b95e49bf7a
patch -p1 < duckdb_1.6.0_like_predicate_injection.patch
mkdir -p build/release && cd build/release
cmake -G Ninja -DCMAKE_BUILD_TYPE=Release ../..
cmake --build . --config Release
echo 'export PATH='"$(pwd)"':$PATH' >> ~/.bashrc
source ~/.bashrc
```

#### 2.2.2 Selectivity Injection

The file is read from the current working directory by default, or from the path given in `DUCKDB_LIKE_SELECTIVITY_FILE` if set.

```bash
export DUCKDB_LIKE_SELECTIVITY_FILE=/absolute/path/to/like_selectivity.txt
duckdb test.db -c "EXPLAIN SELECT * FROM title WHERE title LIKE 'abc%';"
```

If no entry exists for a given pattern, DuckDB falls back to its own default selectivity estimate.

## 3. To Get Estimates for Cardinality Estimation Methods

For LBS, CLIQUE, and P-SPH, please use the source code we provide directly. For LPLM, SSCard, and Astrid, please use the source code available at the links below. To obtain estimates for `LIKE`-predicates, follow the instructions in each method's source code `README.md` and its respective paper.

### 3.1 Source Code for LPLM, Astrid, SSCard

| Method | Repository |
|--------|------------|
| LPLM   | [github.com/dbis-ukon/lplm](https://github.com/dbis-ukon/lplm) |
| Astrid | [github.com/saravanan-thirumuruganathan/astrid-string-selectivity](https://github.com/saravanan-thirumuruganathan/astrid-string-selectivity) |
| SSCard | [github.com/marlcplhra/SSCard](https://github.com/marlcplhra/SSCard) |

## 4. Integration of Cardinality Estimates into PostgreSQL/DuckDB

For each cardinality estimation method, prepare a method_column.txt file containing the LIKE-predicates evaluated by that method and their respective estimated selectivity, one pattern<TAB>selectivity entry per line. For example, LPLM_actorname.txt contains LPLM's estimated selectivities for LIKE-predicates over the actor.name column. Sample files can be found in sampleEstimates.
These files are then injected into PostgreSQL and DuckDB using the following scripts:

| System | Injection Type | Script |
|--------|----------------|--------|
| DuckDB | Estimated cardinalities | `inject_estimates_duckdb.py` |
| DuckDB | True cardinalities | `inject_true_duckdb.py` |
| PostgreSQL | Estimated cardinalities | `inject_estimates_postgresql.py` |
| PostgreSQL | True cardinalities | `inject_true_postgresql.py` |

To run:

```bash
python3 inject_estimates_duckdb.py
python3 inject_true_duckdb.py
python3 inject_estimates_postgresql.py
python3 inject_true_postgresql.py
```




## 5. Benchmark

LCEB contains 1000 SQL queries for both IMDb and StackOverflow. For any cardinality estimation method, simply replace LCEB's LIKE-patterns (%%) with the patterns supported by that method.

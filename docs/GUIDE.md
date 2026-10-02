# The complete guide: how this project works, in simple words

This guide explains **what** the project does, **why** each tool was chosen, **what every file does**, how
to **set it up on Snowflake**, and **how to talk about it in an interview**. Read it top to bottom once.
After that, you should be able to explain any part of the project without looking at the code.

---

## 1. The problem

Air pollution is one of India's biggest public-health issues. The government body CPCB publishes a daily
**National Air Quality Index (AQI)**: one number from 0 to 500 that tells you how dirty the air is.

Suppose a news site, a health app or a city office wants that number **every day, for many cities,
automatically, and trustworthy**. Doing it by hand means downloading data, cleaning it in Excel, doing the
AQI maths and making a chart every single day. That is slow and easy to get wrong.

This project automates the whole thing the way a real data team would:

* It runs **by itself every morning**. Nobody presses a button.
* It stores data in a **cloud data warehouse (Snowflake)**, like most companies do.
* It **checks the data** at every stage and **stops loudly** if something is wrong, instead of publishing
  a wrong number.
* It **keeps history**, only re-processes what changed (**incremental**), and can rebuild everything from
  the raw data at any time.
* It publishes a **live dashboard** and **documentation of every table** (dbt docs).

This matches what data-engineer job descriptions ask for: *build and maintain ELT pipelines, Snowflake,
dbt, SQL, Python, Airflow, data modelling, data quality, CI/CD, Docker.*

---

## 2. The big picture (one analogy)

Think of a **restaurant kitchen**:

| Kitchen | This project | Where |
|---|---|---|
| Buying vegetables at the market | **Extract**: download readings from the API | `extract.py` |
| Checking they are fresh before accepting them | **Validate**: the data contract | `contracts.py` |
| Putting them in labelled boxes | **Land**: one Parquet file per city per day | `load.py`, `data/raw/` |
| Moving boxes into the restaurant's big cold store | **Load into Snowflake**: PUT → COPY INTO → MERGE | `snowflake_load.py` |
| Washing and cutting | **Staging** model: clean, fix units, remove duplicates | `dbt/models/staging/` |
| Cooking basic sauces | **Intermediate** model: daily averages | `dbt/models/intermediate/` |
| Plating the final dish | **Marts**: the AQI fact table and summary | `dbt/models/marts/` |
| Head chef tasting before serving | **Tests**: 24 dbt tests, 21 pytest tests | `dbt/…/*.yml`, `tests/` |
| The menu describing each dish | **dbt docs**: every table, column and test, plus a lineage graph | `site/dbt-docs/` |
| The waiter serving | **Report**: the dashboard | `report.py` |
| The kitchen timetable | **Orchestration**: runs at 07:00 IST daily | `daily.yml`, `airflow_dag.py` |

The key rule: **raw data is never changed**. Every clean table is *derived* from it. If a recipe had a
mistake, we fix the recipe and cook again.

---

## 3. The tools, and why each one

| Tool | What it is | Why it is used here |
|---|---|---|
| **Python** | Programming language | Calls the API, validates, writes files, loads Snowflake, glues the steps |
| **requests + urllib3 Retry** | HTTP library | Downloads data; retries automatically (1s, 2s, 4s… waits) if the API is briefly down |
| **Pydantic** | Validation library | A *data contract*: the exact shape the API must send. Anything else is rejected |
| **Parquet + PyArrow** | Columnar file format | Small, fast, keeps data types. Snowflake reads it natively |
| **Snowflake** | Cloud data warehouse | Where the data lives in production. Storage and compute are separate: you pay for compute only while a query runs |
| **snowflake-connector-python** | Snowflake's Python driver | Runs PUT, COPY INTO and MERGE from Python |
| **dbt** (dbt-snowflake) | "data build tool" | Each table is a `SELECT`; dbt builds them in the right order, tests them, documents them, and handles incremental loads |
| **DuckDB** (dbt-duckdb) | A tiny free database in one file | Runs the **same dbt models** on your laptop and in CI, so tests never need a Snowflake account |
| **Apache Airflow** | Workflow scheduler | The most common orchestrator in industry. A ready DAG is included |
| **GitHub Actions** | CI/CD + scheduler | Tests every push; runs the pipeline daily for free |
| **Docker** | Container | The whole pipeline runs identically anywhere |
| **pytest, ruff** | Testing, linting | Code must pass tests and style checks before it is merged |
| **fakesnow, sqlglot** | Snowflake emulator, SQL parser | Test the Snowflake SQL without an account |
| **Jinja2 + Plotly** | HTML template + charts | The static dashboard |
| **GitHub Pages** | Free hosting | Hosts the dashboard and the dbt docs |

**Why keep DuckDB if we have Snowflake?** Every push to GitHub runs the full test suite. If those tests
used Snowflake, every push would cost money and need secret keys on every machine. So the same dbt SQL
runs on DuckDB in development and CI (free, offline), and on Snowflake in production. This dev/prod
split with **dbt targets** is how real teams work. The small differences between the two SQL dialects are
handled with a dbt **dispatch** macro (see `macros/cross_db.sql`).

**Why ELT, not ETL?** We **L**oad raw data into the warehouse first and **T**ransform it there with dbt.
Raw data is always kept, and transformations can be changed and re-run on the full history whenever the
logic changes.

---

## 4. One daily run, step by step

Every day at **07:00 IST**, GitHub Actions starts `.github/workflows/daily.yml`:

**Step 1: Choose the days.** It loads the **last 3 finished days**, not just yesterday, because data
providers sometimes correct recent values. On the very first run it **back-fills 90 days**.

**Step 2: Extract (`extract.py`).** For each of the 10 cities it calls the Open-Meteo Air Quality API
(latitude, longitude, six pollutants per hour, India time). On errors like 503 it waits and retries up to
5 times. If a city still fails, the run **fails and names the city**.

**Step 3: Validate (`contracts.py`).** Pydantic checks all six pollutant lists are present, each has the
same length as the time list, units are exactly µg/m³, and nothing is negative. Otherwise: stop.

**Step 4: Land (`load.py`).** Rows are written as `data/raw/air_quality_hourly/date=2026-09-24/delhi.parquet`.
Same city-day = same file name, so **re-running overwrites instead of duplicating (idempotent)**.
Files are written to a temporary name and then swapped in, so nobody ever reads a half-written file.

**Step 5: Load into Snowflake (`snowflake_load.py`).**
1. `CREATE ... IF NOT EXISTS` the RAW schema, the raw table and an **internal stage** (Snowflake-managed
   file storage).
2. **`PUT`** uploads each day's Parquet files to `@RAW.AQI_LANDING/date=YYYY-MM-DD/`.
3. **`COPY INTO`** a temporary table, reading Parquet by column name. `FORCE = TRUE` because the temp
   table is new every time. Snowflake normally skips files it loaded before, which would silently drop
   corrections.
4. **`MERGE`** into `RAW.AIR_QUALITY_HOURLY` on `(city_id, observed_at)`: update rows that exist, insert
   new ones. Running it twice leaves one row per city-hour. This is an **upsert**.

**Step 6: Transform + test (`dbt build` on Snowflake).** dbt builds seeds → staging → intermediate →
marts and runs the 24 tests right after each model. If a test fails, everything downstream is **skipped**,
so bad data never reaches the final tables. The fact table is **incremental**: after the first build it only
re-processes the last 3 days and MERGEs them in. Then `dbt source freshness` checks the newest raw row is
recent (warn after 30 h, error after 54 h).

**Step 7: Document and report.** `dbt docs generate --static` builds one HTML page describing every model,
column and test, with a lineage graph. `report.py` reads the marts from Snowflake and dbt's result files and
writes the dashboard, including a **Pipeline health** panel.

**Step 8: Publish.** The workflow commits the raw Parquet files to the repo (a backup of the raw layer)
and publishes the dashboard and dbt docs to GitHub Pages. If anything failed, the job turns red,
**GitHub emails you**, and yesterday's dashboard stays online.

---

## 5. The AQI maths, with a worked example

CPCB rules, implemented in `dbt/models/`:

1. **PM2.5, PM10, NO₂, SO₂**: 24-hour average. **O₃, CO**: the highest 8-hour rolling average of the day.
2. Find the **breakpoint band** the value falls in (`dbt/seeds/aqi_breakpoints.csv`).
3. Interpolate in a straight line inside the band:
   `sub_index = index_low + (value − conc_low) × (index_high − index_low) / (conc_high − conc_low)`
4. **AQI = the highest sub-index**. That pollutant is the **dominant pollutant**.
5. **Validity:** a 24-hour average needs **≥ 16 hours** of data; the AQI needs **≥ 3 valid pollutants
   including PM2.5 or PM10**. Otherwise no AQI is published for that day.

**Example** (checked by `tests/test_aqi_calculation.py`):

| Pollutant | Daily value | Band | Sub-index |
|---|---|---|---|
| PM2.5 | 45 µg/m³ | 30–60 → 50–100 | 50 + (45−30)×50/30 = **75** |
| PM10 | 80 µg/m³ | 50–100 → 50–100 | 50 + (80−50)×50/50 = **80** |
| NO₂ | 30 µg/m³ | 0–40 → 0–50 | 30×50/40 = 37.5 → **38** |
| O₃ | 40 µg/m³ | 0–50 → 0–50 | **40** |
| CO | 0.5 mg/m³ | 0–1 → 0–50 | **25** |

Highest is 80 → **AQI 80, "Satisfactory", dominant pollutant PM10.**

**A real bug the tests caught.** When two pollutants tie (say PM2.5 and PM10 both 114),
"pick the pollutant with the highest sub-index" has two answers, and the database may pick differently on
each run. The incremental test (`test_incremental.py`) found this: an incremental run and a full rebuild
disagreed. The fix is a fixed tie-break priority (PM2.5 > PM10 > NO₂ > O₃ > SO₂ > CO), so the answer is
always the same. This is a good interview story: **non-deterministic SQL**, and how a test exposed it.

---

## 6. Every file, explained

```
india-air-quality-pipeline/
├── README.md, pyproject.toml, Dockerfile, .dockerignore, .gitignore, .env.example
├── .github/workflows/ci.yml, daily.yml
├── snowflake/setup.sql
├── src/aqi_pipeline/
│   ├── cli.py, settings.py
│   ├── extract.py, contracts.py, load.py
│   ├── snowflake_load.py, warehouse.py
│   ├── transform.py, report.py, sample.py
│   └── templates/dashboard.html.j2
├── dbt/
│   ├── dbt_project.yml, profiles.yml
│   ├── seeds/cities.csv, aqi_breakpoints.csv
│   ├── macros/cross_db.sql, create_duckdb_raw_view.sql, generate_schema_name.sql
│   ├── models/sources.yml
│   ├── models/staging/…, intermediate/…, marts/…
│   └── tests/ (generic + singular tests)
├── orchestration/airflow_dag.py
├── tests/ (21 pytest tests)
├── data/raw/ (raw Parquet backup, filled by the daily job)
└── docs/ (this guide + screenshots)
```

### Root files

* **`README.md`**: the repo's front page: architecture diagram, skills table, data model, how to run it,
  limitations.
* **`pyproject.toml`**: makes the folder an installable Python package. Lists the libraries, the dev tools,
  and creates the **`aqi` command**. `pip install -e .` reads it.
* **`Dockerfile`**: builds an image with Python 3.12 and the project, runs as a normal (non-root) user, and
  starts the `aqi` command. **`.dockerignore`** keeps data, keys and tests out of the image.
* **`.gitignore`**: never commit the virtual environment, caches, the DuckDB file, generated site, **private
  keys (`keys/`, `*.p8`)** or **`.env`**.
* **`.env.example`**: a template of the environment variables needed for Snowflake. Copy it to `.env`
  (which is ignored) for Docker.

### `.github/workflows/`

* **`ci.yml`**: on every push/PR: ruff lint → 21 pytest tests → `dbt parse` for the Snowflake target → full
  pipeline on sample data. A second job builds the **Docker** image and runs the pipeline inside it.
* **`daily.yml`**: the scheduler (`cron: "30 1 * * *"` = 07:00 IST) with a **Run workflow** button.
  * `AQI_TARGET` becomes `snowflake` when the `SNOWFLAKE_ACCOUNT` secret exists, otherwise `duckdb`, so the
    site keeps updating even if Snowflake is switched off.
  * Writes the private key from the `SNOWFLAKE_PRIVATE_KEY` secret into a temporary file (`chmod 600`).
    The key is never in the code.
  * `concurrency`: two runs can never overlap. `[skip ci]` stops the data commit from re-running CI.

### `snowflake/setup.sql` (run once, by you, as ACCOUNTADMIN)

* **`AQI_WH`**: an X-Small warehouse, `auto_suspend = 60`: it switches off after 60 idle seconds, so you
  pay only for the minute or two the pipeline runs.
* **`AQI_MONITOR`**: a **resource monitor**: emails at 80% and **suspends the warehouse** at 100% of 5
  credits per month. It is a hard spending cap.
* **`AQI` database** and **`AQI_PIPELINE_ROLE`**: the role can use the warehouse and create schemas in this
  database only (**least privilege**). It creates and owns RAW, REF and ANALYTICS itself.
* **`AQI_PIPELINE_USER`**: `TYPE = SERVICE`, **RSA key only, no password**. Snowflake is phasing out
  passwords for service accounts, so key-pair is the right way.

### `src/aqi_pipeline/` (Python)

* **`cli.py`**: the `aqi` commands: `run`, `ingest`, `backfill`, `sample`, `transform`, `report`,
  `snowflake-keygen`. Works out "yesterday" in **India time** and splits long back-fills into 31-day chunks.
  `snowflake-keygen` creates the RSA key pair and prints the public key to paste into `setup.sql`.
* **`settings.py`**: every path and setting in one place, all overridable by environment variables.
  `AQI_TARGET` picks the warehouse. `SnowflakeConfig.from_env()` reads the Snowflake settings and fails
  with a clear message listing any that are missing.
* **`extract.py`**: the API calls, with retries, a timeout, a pause between cities, and one error listing
  every failed city.
* **`contracts.py`**: the Pydantic data contract.
* **`load.py`**: writes the Parquet partitions with a fixed schema and lineage columns
  (`source`, `ingested_at`, `run_id`), zstd compression, and atomic swap.
* **`snowflake_load.py`**: the Snowflake loader. Each SQL statement is built by a small function
  (`ddl_statements`, `put_statement`, `load_table_statements`, `merge_statement`) so it can be unit-tested.
  `load()` runs them in order. Uses **key-pair auth** and a **query tag** (`aqi-pipeline:load`) so you can
  find the pipeline's queries in Snowflake's query history.
* **`warehouse.py`**: one tiny interface, `query(sql)`, that works for both Snowflake and DuckDB, so the
  dashboard code doesn't care where data lives.
* **`transform.py`**: runs dbt inside Python (`dbtRunner`): `build`, then `source freshness`, then
  `docs generate --static`. Saves the build results so the dashboard can show how many tests passed.
* **`report.py`**: reads the marts and dbt results and renders the dashboard.
* **`sample.py`**: synthetic, clearly labelled demo data, so anyone can run the project **offline**.
  It always uses DuckDB and a separate folder, so it can never reach Snowflake or be committed.
* **`templates/dashboard.html.j2`**: the dashboard page: KPIs, latest AQI bar chart, trend chart with CPCB
  colour bands, city × day heatmap, 30-day table, pipeline health, links to dbt docs, method and
  limitations.

### `dbt/` (all the SQL)

* **`dbt_project.yml`**: staging and intermediate are **views**, marts are **tables**, and the fact table is
  **incremental**. Variables hold the CPCB rules (`min_hours_for_valid_day: 16`,
  `min_pollutants_for_aqi: 3`) and `lookback_days: 3`. `on-run-start` calls the DuckDB raw-view macro.
* **`profiles.yml`**: two **targets**: `dev` = DuckDB file, `prod` = Snowflake with key-pair auth (all
  values from environment variables, nothing secret in the file).
* **`seeds/cities.csv`**: the 10 cities (add a row to add a city; Python reads this same file).
* **`seeds/aqi_breakpoints.csv`**: the CPCB breakpoint table, kept as **data**, not a giant `CASE`.
* **`macros/cross_db.sql`**: `arg_max(value, by)` → `MAX_BY` on Snowflake, `arg_max` on DuckDB, chosen by
  dbt's **adapter dispatch**.
* **`macros/create_duckdb_raw_view.sql`**: on DuckDB only, exposes the Parquet files as `raw.air_quality_hourly`
  so the dbt source has the same name on both warehouses.
* **`macros/generate_schema_name.sql`**: clean schema names (`REF`, not `ANALYTICS_REF`).
* **`models/sources.yml`**: declares the raw table and its **freshness** rules.
* **`models/staging/stg_air_quality__hourly.sql`**: cleaning only: trim/lower IDs, cast types, add a date
  column, convert CO to mg/m³, and **de-duplicate** with
  `qualify row_number() over (partition by city_id, observed_at order by ingested_at desc) = 1`.
* **`models/intermediate/int_pollutant_daily.sql`**: 24-hour averages, plus 8-hour rolling O₃/CO using a
  **time-based self-join** (for every hour, join the 8 hours ending at it). This is exact even when
  hours are missing and runs the same on both warehouses. Counts hours so thin data can be rejected.
* **`models/marts/dim_city.sql`**: the **dimension**: city attributes.
* **`models/marts/fct_city_daily_aqi.sql`**: the **fact** and the heart of the project:
  * turns six daily values into rows, joins to the breakpoint seed, calculates sub-indices;
  * `max(sub_index)` = AQI, `MAX_BY(pollutant, …)` = dominant pollutant with a deterministic tie-break;
  * applies CPCB validity rules and category labels; `lag()` and a 7-row window for trends;
  * **incremental**: `unique_key = daily_aqi_id`, `merge` on Snowflake / `delete+insert` on DuckDB. Each
    run re-processes the last 3 days, reading 7 extra days so the trend columns stay correct.
* **`models/marts/mart_city_aqi_summary.sql`**: business-ready 30-day summary per city with `rank()`.
* **`*.yml` files next to models**: descriptions (shown in dbt docs) and tests.
* **`tests/generic/`**: reusable tests written as macros: `unique_combination_of_columns`, `value_in_range`.
* **`tests/assert_*.sql`**: singular business-rule tests (category matches the AQI; no future dates).

### `orchestration/airflow_dag.py`

Airflow 3 DAG: `ingest → dbt_build → dbt_source_freshness → publish_report`, 2 retries 10 minutes apart,
`max_active_runs = 1`, `catchup = False`. Checked to load without errors on Airflow 3.3.

### `tests/` (21 tests)

| File | What it proves |
|---|---|
| `test_contracts.py` (5) | Good data passes; wrong lengths, negatives, wrong units, missing fields are rejected |
| `test_extract.py` (3) | Correct API parameters; one row per hour; a failing city fails the run by name |
| `test_load.py` (2) | One file per day; re-loading overwrites, no temp files left |
| `test_aqi_calculation.py` (3) | End-to-end dbt run: the worked example = AQI 80; thin day = no AQI; extreme value capped at 500 |
| `test_incremental.py` (1) | Incremental run gives **exactly** the same result as a full rebuild |
| `test_snowflake_load.py` (6) | Loader SQL is valid Snowflake SQL; PUT/COPY options; statement order; **MERGE is idempotent** on a Snowflake emulator |
| `test_dbt_on_snowflake_emulator.py` (1) | Every dbt model and test runs with the **Snowflake adapter** (MAX_BY, DATEADD, MODE, QUALIFY) |

---

## 7. What can go wrong, and what catches it

| Problem | Caught by | Result |
|---|---|---|
| API down for a minute | retries with back-off | retried up to 5 times, then fails |
| One city fails | error collection | run fails, names the city |
| API changes a field or unit | Pydantic contract | stops before anything is stored |
| Same day loaded twice | fixed file names + MERGE + staging dedupe | no duplicates anywhere |
| Snowflake skips a re-uploaded file | `COPY … FORCE = TRUE` into a fresh temp table | corrections always loaded |
| Too few hours in a day | CPCB rule in dbt | day kept, no AQI published |
| AQI > 500 or wrong label | range test + singular test | `dbt build` fails, dashboard not updated |
| Tie between pollutants | deterministic tie-break, incremental test | same answer every run |
| API stops sending new data | source freshness | warn at 30 h, fail at 54 h |
| Snowflake bill grows | auto-suspend + resource monitor | warehouse suspended at 5 credits/month |
| Key leaked into Git | `.gitignore` for `keys/`, `*.p8`, `.env`; key only in GitHub Secrets | key never committed |
| Broken code pushed | CI (lint, 21 tests, Docker build) | red ✗ before merge |

---

## 8. Setting it up

### A. Run it on your laptop first (no accounts needed)

```powershell
cd india-air-quality-pipeline
python -m venv .venv
.venv\Scripts\activate
pip install -e ".[dev]"
pytest
aqi sample --days 60
start site\index.html
```

### B. Put it on GitHub (use git, not drag-and-drop)

Create an **empty public repo** named `india-air-quality-pipeline` (no README), then in the project folder:

```powershell
git init -b main
git add .
git commit -m "India air-quality pipeline: Snowflake, dbt, Airflow, CI/CD"
git remote add origin https://github.com/amannnn7/india-air-quality-pipeline.git
git push -u origin main
```

Check the **Actions** tab: the **CI** workflow should go green (tests + Docker build).

### C. Set up Snowflake (about 15 minutes)

1. Sign up for a free Snowflake trial (30 days, $400 of credits). Choose any cloud/region near India
   (e.g. AWS Mumbai).
2. On your laptop, create the key pair:
   ```powershell
   aqi snowflake-keygen
   ```
   It writes `keys\rsa_key.p8` (private, **never share or commit**) and prints the public key.
3. Open `snowflake/setup.sql` in Snowsight (Projects → Worksheets), paste the public key where it says
   `<PASTE_PUBLIC_KEY_HERE>`, and **Run All**. The last query prints your account identifier
   (`ORGNAME-ACCOUNTNAME`).
4. Try it from your laptop:
   ```powershell
   $env:AQI_TARGET = "snowflake"
   $env:SNOWFLAKE_ACCOUNT = "ORGNAME-ACCOUNTNAME"
   $env:SNOWFLAKE_USER = "AQI_PIPELINE_USER"
   $env:SNOWFLAKE_PRIVATE_KEY_PATH = "keys\rsa_key.p8"
   aqi backfill --start 2026-09-01 --end 2026-09-25
   ```
   In Snowsight, you should now see `AQI.RAW.AIR_QUALITY_HOURLY` and `AQI.ANALYTICS.FCT_CITY_DAILY_AQI`.

### D. Turn on the daily automation

1. Repo **Settings → Secrets and variables → Actions → New repository secret**, three times:
   * `SNOWFLAKE_ACCOUNT` = your `ORGNAME-ACCOUNTNAME`
   * `SNOWFLAKE_USER` = `AQI_PIPELINE_USER`
   * `SNOWFLAKE_PRIVATE_KEY` = the **whole content** of `keys\rsa_key.p8` (including the BEGIN/END lines)
2. **Settings → Pages → Source: GitHub Actions.**
3. **Settings → Actions → General → Workflow permissions: Read and write.**
4. **Actions → Daily pipeline → Run workflow.** The first run back-fills 90 days.
5. Open **https://amannnn7.github.io/india-air-quality-pipeline/** and the dbt docs link on it.

---

## 9. Cost

* The warehouse is **X-Small** (1 credit per hour of running) and **suspends after 60 seconds idle**. A daily
  run keeps it busy for roughly 1–3 minutes, so a few credits per month at most, and the **resource monitor
  stops it at 5 credits**.
* The **trial ends after 30 days**. Snowflake then suspends the account unless you add a card. If you don't
  want to pay, delete the three `SNOWFLAKE_*` secrets: the daily job automatically switches to DuckDB and
  the dashboard keeps updating. Keep screenshots of your Snowflake objects and query history from the trial
  for interviews.

---

## 10. Honest limitations

* **Modelled, not measured:** Open-Meteo serves the CAMS global model (~45 km grid), not CPCB station data.
* **Two pollutants missing:** NH₃ and lead are not available, so up to 6 of CPCB's 8 are used.
* **"Severe" upper limit:** CPCB gives none; a conventional cap is used and AQI is capped at 500.
* **Git as a raw backup** is fine for megabytes, not gigabytes. At scale the raw files belong in S3/ADLS
  (an external stage) with **Snowpipe** loading them automatically.
* **Local emulator tests** prove the Snowflake SQL and MERGE logic, but a few features (PUT/COPY of Parquet,
  dbt's incremental temp view) can only be exercised on a real Snowflake account.

---

## 11. How it would grow at a real company

* Raw files in **S3/ADLS** as an **external stage**, loaded by **Snowpipe** (event-driven, no schedule).
* **Terraform** to create the Snowflake objects in `setup.sql` as code.
* **Airflow** (the DAG is here) or Dagster with Slack/Teams alerts on failure.
* **Snowflake Streams & Tasks** or **dynamic tables** for near-real-time.
* Add **CPCB station data** and measure model-vs-station error (a great analysis to add next).
* Serve the marts to **Power BI/Tableau** through Snowflake.

---

## 12. Interview questions you should be ready for

**Walk me through the pipeline.** Daily, GitHub Actions pulls hourly data for 10 cities from Open-Meteo,
validates it with a Pydantic contract, lands it as partitioned Parquet, then PUTs it to a Snowflake internal
stage, COPYs it into a temp table and MERGEs it into RAW. dbt builds staging, intermediate and an incremental
fact table on Snowflake, runs 24 tests and a freshness check, generates docs, and a dashboard is published.

**How did you load data into Snowflake?** PUT to an internal stage, COPY INTO a temp table with
`MATCH_BY_COLUMN_NAME` and `USE_LOGICAL_TYPE` for Parquet, then MERGE on the natural key. At scale I'd
use an external S3 stage with Snowpipe.

**Why FORCE = TRUE in COPY?** Snowflake keeps load metadata and skips files it loaded before. We re-upload
the same file names when data is corrected, and the temp table is new each run, so we must force the read.

**What does idempotent mean here?** Running the same load twice gives the same result: fixed file names
overwrite, MERGE upserts, staging de-duplicates.

**Explain your incremental model.** `unique_key = daily_aqi_id`; each run processes only the last 3 days
(late corrections) and merges them; it reads 7 extra days so `lag` and the 7-day average stay correct; a test
proves the result equals a full refresh.

**Tell me about a bug you found.** Ties between pollutants made the dominant pollutant non-deterministic.
My incremental-vs-full-refresh test caught it; I added a fixed tie-break priority.

**How do you manage Snowflake cost?** X-Small warehouse, 60 s auto-suspend, resource monitor that suspends
at 5 credits/month, query tags to find the pipeline's queries.

**How is it secured?** Service user with key-pair auth only, least-privilege role, key stored in GitHub
Secrets, keys and `.env` git-ignored.

**Why dbt?** SQL-first transformations with dependency ordering, tests and docs next to the code,
incremental materialisation, and the same models on different warehouses via targets.

**Views vs tables vs incremental?** Staging/intermediate are cheap views; marts are tables for fast reads;
the growing fact table is incremental so each run only processes new data.

**Which window functions did you use?** `row_number` + `QUALIFY` for dedupe, `lag` for day-over-day,
`avg … rows between 6 preceding` for the 7-day average, `rank` for the league table, plus a time-based
self-join for 8-hour rolling averages.

**How do you know the data is right?** Contract at ingestion, 24 dbt tests (unique, not null,
relationships, ranges, accepted values, business rules), freshness, and 21 pytest tests including
hand-calculated AQI values.

**What would you change for 100× the data?** External stage + Snowpipe, clustering on date, larger
warehouse only for back-fills, Airflow with alerting, incremental staging too.

**What is the weakest part?** The source is modelled data, not ground stations. Next I'd add CPCB station
data and quantify the difference.

---

## 13. Resume and LinkedIn text

**Resume bullets**
* Built a production-style ELT pipeline (Python, Snowflake, dbt, Airflow/GitHub Actions, Docker) that
  computes the daily CPCB National AQI for 10 Indian cities and publishes a live dashboard and dbt docs.
* Loaded data into Snowflake via internal stages (PUT → COPY INTO → MERGE upserts) with key-pair service
  auth, least-privilege RBAC, 60-second auto-suspend and a resource monitor capping spend at 5 credits/month.
* Modelled a dbt star schema with an incremental fact table, window functions and 24 data tests; wrote 21
  pytest tests, including an incremental-vs-full-refresh check that caught a non-deterministic tie-break bug.

**LinkedIn post (after 2–3 weeks of real data):** the problem, the architecture diagram, one real finding
from your data, the bug story, links to the dashboard, the dbt docs and the code.

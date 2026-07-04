# Mojro ExecuteWyse - Fleet Analytics ETL Pipeline

ETL pipeline that processes raw fleet telemetry, detects anomalies, and builds a dimensional model ready for Power BI dashboards.

---

## Setup

### 1. Clone the repo

```bash
git clone <repo-url>
cd mojro-fleet-etl
```

### 2. Add the raw data files

Place the provided data files inside the following folder (create it if it doesn't exist):

```
DE_Assignment_Data/
└── sample_data/
    ├── telemetry_events.parquet
    ├── vehicles.csv
    ├── drivers.csv
    ├── customers.csv
    └── trips.csv
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Run the pipeline

```bash
python src/etl.py
```

That's it. The script runs in about 5 seconds and you'll see progress logs in the terminal.

---

## Output

Once the pipeline finishes, two things get created:

**`database/mojro_fleet.sqlite`** - the main database file with all dimension and fact tables.

**`output/`** - the same tables exported as CSVs. Use these to import into Power BI directly (Get Data → Text/CSV).

---


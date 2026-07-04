## Mojro ExecuteWyse — Fleet Analytics Pipeline
### Brief Document

---

### What was built

A pipeline that takes raw GPS telemetry (~1.6M events) from the ExecuteWyse fleet platform and transforms it into a clean dimensional model. The output is a SQLite database and a set of CSVs that plug directly into Power BI.

---

### Data Model

Star schema — 4 dimension tables, 3 fact tables.

```
dim_customer
     |
dim_date --- fact_daily_vehicle_summary --- dim_vehicle --- dim_driver
                                                 |
                                        fact_trip_summary
                                                 |
                                       fact_anomaly_events
```

**Dimensions**

| Table | Description |
|---|---|
| dim_vehicle | Vehicle registry — type (BIKE/3W/LCV/HCV) and customer mapping |
| dim_driver | Driver details linked to their assigned vehicle |
| dim_customer | Customer accounts with city and industry |
| dim_date | Calendar attributes for each date found in telemetry |

**Facts**

| Table | Description |
|---|---|
| fact_trip_summary | Per-trip: distance, avg/max speed, idle time, stoppages |
| fact_daily_vehicle_summary | Daily rollup per vehicle: running hours, idle hours, anomaly count |
| fact_anomaly_events | All flagged events — overspeeding, idling, GPS jumps |
| data_quality_log | Audit trail — records rejected at each cleaning stage |

---

### Anomaly Detection

**Overspeeding** — speed thresholds by vehicle type (BIKE: 60, 3W: 50, LCV: 80, HCV: 70 km/h). Severity is HIGH if more than 20% over the limit, MEDIUM otherwise.

**Excessive Idling** — ignition ON and speed = 0 continuously for more than 15 minutes. Severity bands: LOW (<30 min), MEDIUM (<60 min), HIGH (60 min+). Detection uses the SQL gaps-and-islands pattern with a 5-minute gap tolerance.

**GPS Jump** — calculated speed between two consecutive GPS pings exceeds 200 km/h, which is physically impossible. Always flagged as HIGH. The distance for that row is set to zero so it doesn't inflate trip or daily distance totals.

---

### Key Assumptions

1. Around 27,700 telemetry rows were dropped during cleaning — mostly duplicate event IDs (24,608) and negative speed values (3,092). All rejections are logged in `data_quality_log`.

2. Vehicles with IDs starting with `VH_ORPHAN_*` appear in telemetry but have no record in `vehicles.csv`. Since there's no vehicle type for them, overspeeding detection doesn't apply. They're still included in daily summaries.

3. About 12,000 telemetry rows have a null `trip_id`, meaning those events happened outside any tracked trip (vehicle parked, idling in yard, etc.). These are included in daily summaries but not in trip-level aggregations.

4. Distance between GPS points is calculated using the Haversine formula, which accounts for the curvature of the Earth. Flat Euclidean distance on lat/long coordinates would be inaccurate.

5. Idling episodes are considered the same continuous episode if the gap between consecutive idling events is 5 minutes or less. Anything longer is treated as a new episode.

---

### Tradeoffs

**DuckDB over Pandas or PySpark** — DuckDB reads Parquet natively, supports full SQL with window functions, and processed 1.6M rows in under 5 seconds in-memory. Pandas would have worked but the anomaly logic (LAG-based speed, gaps-and-islands) is cleaner and more auditable in SQL. PySpark is overkill for this scale.

**SQLite as output** — the assignment asked for a portable database file that reviewers can open without setting up a server. SQLite is a single file and runs everywhere. In a production setup this would be PostgreSQL or SQL Server.

**CSVs alongside the database** — Power BI on Windows needs an ODBC driver to connect to SQLite, which is an extra setup step. Exporting CSVs means anyone can load the data into Power BI in 30 seconds with no configuration.

**GPS jump distances zeroed out** — rather than dropping GPS jump rows entirely (which would cause gaps in the time series), the distance for those rows is set to 0. The event is still logged in `fact_anomaly_events` and the row is still used for subsequent consecutive calculations.

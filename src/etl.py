import os
import sqlite3
import logging
from datetime import datetime

import duckdb
import pandas as pd


#paths

BASE_DIR   = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR   = os.path.join(BASE_DIR, 'DE_Assignment_Data', 'sample_data').replace('\\', '/')
DB_PATH    = os.path.join(BASE_DIR, 'database', 'mojro_fleet.sqlite')
OUTPUT_DIR = os.path.join(BASE_DIR, 'output')

TELEMETRY_PATH = f"{DATA_DIR}/telemetry_events.parquet"
VEHICLES_PATH  = f"{DATA_DIR}/vehicles.csv"
DRIVERS_PATH   = f"{DATA_DIR}/drivers.csv"
CUSTOMERS_PATH = f"{DATA_DIR}/customers.csv"
TRIPS_PATH     = f"{DATA_DIR}/trips.csv"


#logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s  %(message)s', datefmt='%H:%M:%S')
log = logging.getLogger(__name__)

dq_records = []


def log_dq(source, stage, total, rejected, reason=''):
    dq_records.append({
        'source_file':      source,
        'stage':            stage,
        'total_records':    total,
        'rejected_records': rejected,
        'rejection_reason': reason,
        'processed_at':     datetime.now().isoformat()
    })
    msg = f"[DQ] {stage} | {source} | total={total:,}  rejected={rejected:,}"
    if reason:
        msg += f"  | {reason}"
    log.info(msg)


#stage 1: load files into db

def extract(conn):
    log.info("--- Stage 1: Extract ---")

    conn.execute(f"CREATE OR REPLACE VIEW raw_telemetry  AS SELECT * FROM read_parquet('{TELEMETRY_PATH}')")
    conn.execute(f"CREATE OR REPLACE VIEW raw_vehicles   AS SELECT * FROM read_csv_auto('{VEHICLES_PATH}')")
    conn.execute(f"CREATE OR REPLACE VIEW raw_drivers    AS SELECT * FROM read_csv_auto('{DRIVERS_PATH}')")
    conn.execute(f"CREATE OR REPLACE VIEW raw_customers  AS SELECT * FROM read_csv_auto('{CUSTOMERS_PATH}')")
    conn.execute(f"CREATE OR REPLACE VIEW raw_trips      AS SELECT * FROM read_csv_auto('{TRIPS_PATH}')")

    sources = {
        'telemetry_events.parquet': 'raw_telemetry',
        'vehicles.csv':             'raw_vehicles',
        'drivers.csv':              'raw_drivers',
        'customers.csv':            'raw_customers',
        'trips.csv':                'raw_trips',
    }
    for src, view in sources.items():
        cnt = conn.execute(f"SELECT COUNT(*) FROM {view}").fetchone()[0]
        log.info(f"  {src}: {cnt:,} rows")
        log_dq(src, 'EXTRACT', cnt, 0)


#stage 2 clean and deduplicate

def clean(conn):
    log.info("--- Stage 2: Clean ---")

    raw_count = conn.execute("SELECT COUNT(*) FROM raw_telemetry").fetchone()[0]

    # keep first occurrence of each event_id, drop nulls and negative speeds
    conn.execute("""
        CREATE OR REPLACE TABLE clean_telemetry AS
        WITH deduped AS (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY event_timestamp) AS rn
            FROM raw_telemetry
            WHERE vehicle_id     IS NOT NULL
              AND event_timestamp IS NOT NULL
              AND latitude        IS NOT NULL
              AND longitude       IS NOT NULL
              AND speed_kmph      >= 0
        )
        SELECT event_id, vehicle_id, driver_id, trip_id,
               event_timestamp, latitude, longitude,
               speed_kmph, heading, gps_accuracy,
               ignition_status, battery_level
        FROM deduped
        WHERE rn = 1
    """)

    clean_count = conn.execute("SELECT COUNT(*) FROM clean_telemetry").fetchone()[0]
    neg_speed   = conn.execute("SELECT COUNT(*) FROM raw_telemetry WHERE speed_kmph < 0").fetchone()[0]
    null_rows   = conn.execute("""
        SELECT COUNT(*) FROM raw_telemetry
        WHERE vehicle_id IS NULL OR event_timestamp IS NULL
           OR latitude IS NULL OR longitude IS NULL
    """).fetchone()[0]
    dup_count = max(raw_count - neg_speed - null_rows - clean_count, 0)

    log_dq('telemetry_events.parquet', 'CLEAN - null fields',      raw_count, null_rows,  'null vehicle/timestamp/GPS')
    log_dq('telemetry_events.parquet', 'CLEAN - negative speed',   raw_count, neg_speed,  'negative speed_kmph')
    log_dq('telemetry_events.parquet', 'CLEAN - duplicate event_id', raw_count, dup_count, 'kept first by timestamp')
    log.info(f"  telemetry: {clean_count:,} rows kept, {raw_count - clean_count:,} rejected")

    conn.execute("CREATE OR REPLACE TABLE clean_vehicles  AS SELECT * FROM raw_vehicles  WHERE vehicle_id  IS NOT NULL AND vehicle_type IS NOT NULL")
    conn.execute("CREATE OR REPLACE TABLE clean_drivers   AS SELECT * FROM raw_drivers   WHERE driver_id   IS NOT NULL")
    conn.execute("CREATE OR REPLACE TABLE clean_customers AS SELECT * FROM raw_customers WHERE customer_id IS NOT NULL")
    conn.execute("CREATE OR REPLACE TABLE clean_trips     AS SELECT * FROM raw_trips     WHERE trip_id     IS NOT NULL AND vehicle_id IS NOT NULL")

    for src, raw_view, clean_tbl in [
        ('vehicles.csv',  'raw_vehicles',  'clean_vehicles'),
        ('drivers.csv',   'raw_drivers',   'clean_drivers'),
        ('customers.csv', 'raw_customers', 'clean_customers'),
        ('trips.csv',     'raw_trips',     'clean_trips'),
    ]:
        raw = conn.execute(f"SELECT COUNT(*) FROM {raw_view}").fetchone()[0]
        cln = conn.execute(f"SELECT COUNT(*) FROM {clean_tbl}").fetchone()[0]
        log_dq(src, 'CLEAN', raw, raw - cln, 'null key fields' if raw != cln else '')


#stage 3: dimension tables

def build_dimensions(conn):
    log.info("--- Stage 3: Dimensions ---")

    conn.execute("""
        CREATE OR REPLACE TABLE dim_vehicle AS
        SELECT vehicle_id, vehicle_number, vehicle_type, customer_id, registration_date
        FROM clean_vehicles
    """)

    conn.execute("""
        CREATE OR REPLACE TABLE dim_driver AS
        SELECT driver_id, driver_name, phone, license_number, vehicle_id, joining_date
        FROM clean_drivers
    """)

    conn.execute("""
        CREATE OR REPLACE TABLE dim_customer AS
        SELECT customer_id, customer_name, city, industry
        FROM clean_customers
    """)

    # one row per unique calendar date found in telemetry
    conn.execute("""
        CREATE OR REPLACE TABLE dim_date AS
        SELECT DISTINCT
            CAST(strftime(event_timestamp::DATE, '%Y%m%d') AS INTEGER) AS date_id,
            CAST(event_timestamp::DATE AS VARCHAR)                      AS full_date,
            YEAR(event_timestamp)                                       AS year,
            MONTH(event_timestamp)                                      AS month,
            strftime(event_timestamp, '%B')                             AS month_name,
            DAY(event_timestamp)                                        AS day,
            DAYOFWEEK(event_timestamp) - 1                              AS day_of_week,
            strftime(event_timestamp, '%A')                             AS day_name,
            WEEKOFYEAR(event_timestamp)                                 AS week_number,
            QUARTER(event_timestamp)                                    AS quarter
        FROM clean_telemetry
        ORDER BY date_id
    """)

    for tbl in ['dim_vehicle', 'dim_driver', 'dim_customer', 'dim_date']:
        cnt = conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
        log.info(f"  {tbl}: {cnt:,} rows")


#stage 4: consecutive GPS metrics using LAG + Haversine

def compute_metrics(conn):
    log.info("--- Stage 4: Haversine distance + GPS speed (SQL LAG) ---")

    conn.execute("""
        CREATE OR REPLACE TABLE telemetry_with_metrics AS
        WITH lagged AS (
            SELECT *,
                LAG(latitude)        OVER (PARTITION BY vehicle_id ORDER BY event_timestamp) AS prev_lat,
                LAG(longitude)       OVER (PARTITION BY vehicle_id ORDER BY event_timestamp) AS prev_lon,
                LAG(event_timestamp) OVER (PARTITION BY vehicle_id ORDER BY event_timestamp) AS prev_ts
            FROM clean_telemetry
        ),
        with_dist AS (
            SELECT *,
                CASE
                    WHEN prev_lat IS NULL THEN NULL
                    ELSE
                        -- Haversine formula (Earth radius = 6371 km)
                        2.0 * 6371.0 * ASIN(SQRT(
                            POWER(SIN((RADIANS(latitude)  - RADIANS(prev_lat)) / 2.0), 2)
                          + COS(RADIANS(prev_lat)) * COS(RADIANS(latitude))
                            * POWER(SIN((RADIANS(longitude) - RADIANS(prev_lon)) / 2.0), 2)
                        ))
                END AS raw_dist_km,
                CASE
                    WHEN prev_ts IS NULL THEN NULL
                    ELSE DATEDIFF('second', prev_ts, event_timestamp)
                END AS time_to_prev_secs
            FROM lagged
        ),
        with_speed AS (
            SELECT *,
                CASE
                    WHEN raw_dist_km IS NULL OR time_to_prev_secs IS NULL OR time_to_prev_secs <= 0
                    THEN NULL
                    ELSE raw_dist_km / (time_to_prev_secs / 3600.0)
                END AS calc_speed_kmph
            FROM with_dist
        )
        -- GPS jump rows (calc_speed > 200 km/h = physically impossible) get distance zeroed
        -- so they don't inflate distance totals in the fact tables
        SELECT *,
            CASE
                WHEN calc_speed_kmph > 200 THEN 0.0
                ELSE raw_dist_km
            END AS dist_to_prev_km
        FROM with_speed
    """)

    jump_count = conn.execute("SELECT COUNT(*) FROM telemetry_with_metrics WHERE calc_speed_kmph > 200").fetchone()[0]
    log.info(f"  Done. GPS jump rows zeroed: {jump_count:,}")


#stage 5: anomaly detection

def detect_anomalies(conn):
    log.info("--- Stage 5: Anomaly Detection ---")

    conn.execute("""
        CREATE OR REPLACE TABLE anomaly_overspeeding AS
        WITH thresholds AS (
            SELECT 'BIKE' AS vehicle_type, 60 AS threshold UNION ALL
            SELECT '3W',                   50             UNION ALL
            SELECT 'LCV',                  80             UNION ALL
            SELECT 'HCV',                  70
        )
        SELECT
            t.event_id,
            t.vehicle_id,
            t.driver_id,
            t.trip_id,
            'OVERSPEEDING' AS anomaly_type,
            CASE WHEN t.speed_kmph > th.threshold * 1.2 THEN 'HIGH' ELSE 'MEDIUM' END AS severity,
            t.event_timestamp AS start_time,
            t.event_timestamp AS end_time,
            0                 AS duration_secs,
            t.latitude        AS lat,
            t.longitude       AS long
        FROM telemetry_with_metrics t
        JOIN clean_vehicles v ON t.vehicle_id = v.vehicle_id
        JOIN thresholds th    ON v.vehicle_type = th.vehicle_type
        WHERE t.speed_kmph > th.threshold
    """)
    log.info(f"  Overspeeding: {conn.execute('SELECT COUNT(*) FROM anomaly_overspeeding').fetchone()[0]:,}")

    conn.execute("""
        CREATE OR REPLACE TABLE anomaly_gps_jumps AS
        SELECT
            event_id,
            vehicle_id,
            driver_id,
            trip_id,
            'GPS_JUMP' AS anomaly_type,
            'HIGH'     AS severity,
            event_timestamp AS start_time,
            event_timestamp AS end_time,
            COALESCE(CAST(time_to_prev_secs AS INTEGER), 0) AS duration_secs,
            latitude  AS lat,
            longitude AS long
        FROM telemetry_with_metrics
        WHERE calc_speed_kmph > 200
    """)
    log.info(f"  GPS jumps: {conn.execute('SELECT COUNT(*) FROM anomaly_gps_jumps').fetchone()[0]:,}")


    conn.execute("""
        CREATE OR REPLACE TABLE anomaly_excessive_idling AS
        WITH idling_rows AS (
            SELECT
                vehicle_id, driver_id, trip_id,
                event_timestamp, latitude, longitude,
                -- flag = 1 when this row starts a new idle episode (gap > 5 min or first row)
                CASE
                    WHEN LAG(event_timestamp) OVER (PARTITION BY vehicle_id ORDER BY event_timestamp) IS NULL
                      OR DATEDIFF('second',
                             LAG(event_timestamp) OVER (PARTITION BY vehicle_id ORDER BY event_timestamp),
                             event_timestamp) > 300
                    THEN 1 ELSE 0
                END AS new_episode
            FROM telemetry_with_metrics
            WHERE ignition_status = 'ON' AND speed_kmph = 0
        ),
        episodes_labeled AS (
            SELECT *,
                SUM(new_episode) OVER (
                    PARTITION BY vehicle_id
                    ORDER BY event_timestamp
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS episode_num
            FROM idling_rows
        ),
        episodes_agg AS (
            SELECT
                vehicle_id,
                episode_num,
                FIRST(driver_id ORDER BY event_timestamp) AS driver_id,
                FIRST(trip_id   ORDER BY event_timestamp) AS trip_id,
                MIN(event_timestamp) AS start_time,
                MAX(event_timestamp) AS end_time,
                DATEDIFF('second', MIN(event_timestamp), MAX(event_timestamp)) AS duration_secs,
                FIRST(latitude  ORDER BY event_timestamp) AS lat,
                FIRST(longitude ORDER BY event_timestamp) AS long
            FROM episodes_labeled
            GROUP BY vehicle_id, episode_num
            HAVING DATEDIFF('second', MIN(event_timestamp), MAX(event_timestamp)) > 900
        )
        SELECT
            'IDLE_' || LPAD(CAST(ROW_NUMBER() OVER (ORDER BY vehicle_id, start_time) AS VARCHAR), 7, '0') AS event_id,
            vehicle_id,
            driver_id,
            trip_id,
            'EXCESSIVE_IDLING' AS anomaly_type,
            CASE
                WHEN duration_secs < 1800 THEN 'LOW'
                WHEN duration_secs < 3600 THEN 'MEDIUM'
                ELSE 'HIGH'
            END AS severity,
            start_time,
            end_time,
            duration_secs,
            lat,
            long
        FROM episodes_agg
    """)
    log.info(f"  Excessive idling: {conn.execute('SELECT COUNT(*) FROM anomaly_excessive_idling').fetchone()[0]:,}")


#stage 6: fact tables

def build_facts(conn):
    log.info("--- Stage 6: Fact Tables ---")

    conn.execute("""
        CREATE OR REPLACE TABLE fact_anomaly_events AS
        SELECT
            ROW_NUMBER() OVER (ORDER BY start_time) AS anomaly_id,
            event_id, vehicle_id, driver_id, trip_id,
            anomaly_type, severity,
            start_time, end_time, duration_secs,
            lat, long,
            CAST(strftime(start_time::DATE, '%Y%m%d') AS INTEGER) AS date_id,
            HOUR(start_time)         AS hour_of_day,
            DAYOFWEEK(start_time) - 1 AS day_of_week
        FROM (
            SELECT event_id, vehicle_id, driver_id, trip_id, anomaly_type, severity,
                   start_time, end_time, duration_secs, lat, long
            FROM anomaly_overspeeding
            UNION ALL
            SELECT event_id, vehicle_id, driver_id, trip_id, anomaly_type, severity,
                   start_time, end_time, duration_secs, lat, long
            FROM anomaly_gps_jumps
            UNION ALL
            SELECT event_id, vehicle_id, driver_id, trip_id, anomaly_type, severity,
                   start_time, end_time, duration_secs, lat, long
            FROM anomaly_excessive_idling
        )
    """)
    log.info(f"  fact_anomaly_events: {conn.execute('SELECT COUNT(*) FROM fact_anomaly_events').fetchone()[0]:,} rows")

    conn.execute("""
        CREATE OR REPLACE TABLE fact_trip_summary AS
        WITH trip_tel AS (
            SELECT * FROM telemetry_with_metrics WHERE trip_id IS NOT NULL
        ),
        trip_base AS (
            SELECT
                trip_id,
                FIRST(vehicle_id ORDER BY event_timestamp) AS vehicle_id,
                FIRST(driver_id  ORDER BY event_timestamp) AS driver_id,
                MIN(event_timestamp)                         AS actual_start,
                MAX(event_timestamp)                         AS actual_end,
                ROUND(SUM(COALESCE(dist_to_prev_km, 0)), 3) AS actual_distance_km,
                ROUND(AVG(speed_kmph), 2)                    AS avg_speed,
                ROUND(MAX(speed_kmph), 2)                    AS max_speed
            FROM trip_tel
            GROUP BY trip_id
        ),
        trip_idle AS (
            SELECT trip_id,
                   ROUND(SUM(COALESCE(time_to_prev_secs, 0)) / 60.0, 2) AS idle_time_mins
            FROM trip_tel
            WHERE ignition_status = 'ON' AND speed_kmph = 0
            GROUP BY trip_id
        ),
        trip_stops AS (
            SELECT trip_id, COUNT(*) AS stoppage_count
            FROM trip_tel
            WHERE ignition_status = 'OFF' AND speed_kmph = 0
            GROUP BY trip_id
        )
        SELECT
            b.trip_id, b.vehicle_id, b.driver_id,
            t.origin_city, t.destination_city, t.status,
            t.planned_start, t.planned_end,
            b.actual_start, b.actual_end, b.actual_distance_km,
            b.avg_speed, b.max_speed,
            COALESCE(i.idle_time_mins, 0) AS idle_time_mins,
            COALESCE(s.stoppage_count, 0) AS stoppage_count
        FROM trip_base b
        LEFT JOIN clean_trips t ON b.trip_id = t.trip_id
        LEFT JOIN trip_idle  i  ON b.trip_id = i.trip_id
        LEFT JOIN trip_stops s  ON b.trip_id = s.trip_id
    """)
    log.info(f"  fact_trip_summary: {conn.execute('SELECT COUNT(*) FROM fact_trip_summary').fetchone()[0]:,} rows")

    conn.execute("""
        CREATE OR REPLACE TABLE fact_daily_vehicle_summary AS
        WITH daily_base AS (
            SELECT
                CAST(strftime(event_timestamp::DATE, '%Y%m%d') AS INTEGER) AS date_id,
                vehicle_id,
                ROUND(SUM(COALESCE(dist_to_prev_km, 0)), 3) AS total_distance_km,
                ROUND(AVG(speed_kmph), 2)                    AS avg_speed,
                ROUND(MAX(speed_kmph), 2)                    AS max_speed
            FROM telemetry_with_metrics
            GROUP BY date_id, vehicle_id
        ),
        daily_running AS (
            SELECT
                CAST(strftime(event_timestamp::DATE, '%Y%m%d') AS INTEGER) AS date_id,
                vehicle_id,
                ROUND(SUM(COALESCE(time_to_prev_secs, 0)) / 3600.0, 3) AS running_hours
            FROM telemetry_with_metrics
            WHERE ignition_status = 'ON'
            GROUP BY date_id, vehicle_id
        ),
        daily_idle AS (
            SELECT
                CAST(strftime(event_timestamp::DATE, '%Y%m%d') AS INTEGER) AS date_id,
                vehicle_id,
                ROUND(SUM(COALESCE(time_to_prev_secs, 0)) / 3600.0, 3) AS idle_hours
            FROM telemetry_with_metrics
            WHERE ignition_status = 'ON' AND speed_kmph = 0
            GROUP BY date_id, vehicle_id
        ),
        daily_trips AS (
            SELECT
                CAST(strftime(event_timestamp::DATE, '%Y%m%d') AS INTEGER) AS date_id,
                vehicle_id,
                COUNT(DISTINCT trip_id) AS trip_count
            FROM telemetry_with_metrics
            WHERE trip_id IS NOT NULL
            GROUP BY date_id, vehicle_id
        ),
        daily_anomalies AS (
            SELECT date_id, vehicle_id, COUNT(*) AS anomaly_count
            FROM fact_anomaly_events
            GROUP BY date_id, vehicle_id
        )
        SELECT
            b.date_id, b.vehicle_id,
            b.total_distance_km,
            COALESCE(r.running_hours, 0) AS running_hours,
            COALESCE(i.idle_hours,    0) AS idle_hours,
            b.avg_speed, b.max_speed,
            COALESCE(t.trip_count,    0) AS trip_count,
            COALESCE(a.anomaly_count, 0) AS anomaly_count
        FROM daily_base b
        LEFT JOIN daily_running   r ON b.date_id = r.date_id AND b.vehicle_id = r.vehicle_id
        LEFT JOIN daily_idle      i ON b.date_id = i.date_id AND b.vehicle_id = i.vehicle_id
        LEFT JOIN daily_trips     t ON b.date_id = t.date_id AND b.vehicle_id = t.vehicle_id
        LEFT JOIN daily_anomalies a ON b.date_id = a.date_id AND b.vehicle_id = a.vehicle_id
    """)
    log.info(f"  fact_daily_vehicle_summary: {conn.execute('SELECT COUNT(*) FROM fact_daily_vehicle_summary').fetchone()[0]:,} rows")


#stage 7: write to SQLite

def load_to_sqlite(conn):
    log.info("--- Stage 7: Load to SQLite ---")

    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)

    tables = [
        'dim_vehicle', 'dim_driver', 'dim_customer', 'dim_date',
        'fact_trip_summary', 'fact_daily_vehicle_summary', 'fact_anomaly_events',
    ]

    sqlite_conn = sqlite3.connect(DB_PATH)
    try:
        for tbl in tables:
            df = conn.execute(f"SELECT * FROM {tbl}").df()
            # SQLite doesn't support timestamps natively, convert to string
            for col in df.select_dtypes(include=['datetime64[ns]', 'datetimetz']).columns:
                df[col] = df[col].astype(str)
            df.to_sql(tbl, sqlite_conn, if_exists='replace', index=False)
            log.info(f"  {tbl}: {len(df):,} rows")

        pd.DataFrame(dq_records).to_sql('data_quality_log', sqlite_conn, if_exists='replace', index=False)
        log.info(f"  data_quality_log: {len(dq_records):,} rows")
    finally:
        sqlite_conn.close()

    log.info(f"  Saved to: {DB_PATH}")


#stage 8: export CSVs for Power BI

def export_csvs(conn):
    log.info("--- Stage 8: Export CSVs ---")

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    tables = [
        'dim_vehicle', 'dim_driver', 'dim_customer', 'dim_date',
        'fact_trip_summary', 'fact_daily_vehicle_summary', 'fact_anomaly_events',
    ]

    for tbl in tables:
        path = os.path.join(OUTPUT_DIR, f"{tbl}.csv").replace('\\', '/')
        conn.execute(f"COPY {tbl} TO '{path}' (HEADER, DELIMITER ',')")
        log.info(f"  {tbl}.csv")

    pd.DataFrame(dq_records).to_csv(os.path.join(OUTPUT_DIR, 'data_quality_log.csv'), index=False)
    log.info("  data_quality_log.csv")


def main():
    start = datetime.now()
    log.info("Starting Mojro Fleet ETL pipeline...")

    conn = duckdb.connect()

    extract(conn)
    clean(conn)
    build_dimensions(conn)
    compute_metrics(conn)
    detect_anomalies(conn)
    build_facts(conn)
    load_to_sqlite(conn)
    export_csvs(conn)

    elapsed = round((datetime.now() - start).total_seconds(), 1)
    log.info(f"Done in {elapsed}s  |  DB: {DB_PATH}")

    conn.close()


if __name__ == '__main__':
    main()

from flask import Flask, jsonify, request, render_template, Response
import json
import numpy as np
import matplotlib.cm as cm
from dotenv import load_dotenv
import awswrangler as wr
import boto3
import pandas as pd
from shapely import wkt as shapely_wkt
from cachetools import LRUCache
import threading

load_dotenv()

app = Flask(__name__)

ATHENA_DB_GOLD = "population_demographics_gold"
ATHENA_DB_MARTS = "population_demographics_gold_marts"
ATHENA_WORKGROUP = "population-demographics"

# Thread-safe LRU cache for full GeoJSON responses
geo_cache = LRUCache(maxsize=128)
cache_lock = threading.Lock()


def get_cached_response(key, fetch_and_process_fn):
    with cache_lock:
        if key in geo_cache:
            return geo_cache[key]
    result = fetch_and_process_fn()
    with cache_lock:
        geo_cache[key] = result
    return result

METRICS = {
    "median_household_income": {
        "label": "Median Household Income",
        "description": "Middle income among all households. Half earn more, half earn less.",
        "format": "currency",
        "colormap": cm.YlOrRd,
    },
    "poverty_rate": {
        "label": "Poverty Rate",
        "description": "% of population living below the federal poverty line.",
        "format": "percent",
        "colormap": cm.YlOrRd,
    },
    "bachelors_plus_rate": {
        "label": "Bachelor's+ Rate",
        "description": "% of adults 25+ with a bachelor's degree or higher.",
        "format": "percent",
        "colormap": cm.YlGnBu,
    },
    "renter_rate": {
        "label": "Renter Rate",
        "description": "% of occupied housing units that are renter-occupied.",
        "format": "percent",
        "colormap": cm.PuRd,
    },
    "remote_work_rate": {
        "label": "Remote Work Rate",
        "description": "% of workers who worked from home in the past week.",
        "format": "percent",
        "colormap": cm.BuPu,
    },
}


def get_session():
    return boto3.Session(region_name='us-east-1')


def compute_colors(values, cmap):
    values = np.array(values, dtype=float)
    valid = values[~np.isnan(values)]
    if len(valid) == 0:
        return [[128, 128, 128, 180]] * len(values)
    p5 = float(np.percentile(valid, 5))
    p95 = float(np.percentile(valid, 95))
    colors = []
    for v in values:
        if np.isnan(v):
            colors.append([128, 128, 128, 180])
            continue
        norm = (min(max(v, p5), p95) - p5) / (p95 - p5) if p95 > p5 else 0
        rgba = cmap(norm)
        colors.append([int(rgba[0]*255), int(rgba[1]*255), int(rgba[2]*255), 180])
    return colors, p5, p95


def df_to_geojson(df, metric, include_geometry=True, geom_col='geometry_wkt'):
    cmap = METRICS[metric]["colormap"]
    values = df[metric].fillna(0).values.astype(float)
    colors, p5, p95 = compute_colors(values, cmap)

    features = []
    for i, row in df.iterrows():
        props = {}
        for col in df.columns:
            if col == geom_col:
                continue
            v = row[col]
            props[col] = None if (not isinstance(v, str) and pd.isna(v)) else (v.item() if hasattr(v, 'item') else v)
        props['fill_color'] = colors[i]
        props_json = json.dumps(props)

        if include_geometry and row[geom_col]:
            geom_str = json.dumps(shapely_wkt.loads(row[geom_col]).__geo_interface__)
        else:
            geom_str = "null"

        feature_str = f'{{"type": "Feature", "geometry": {geom_str}, "properties": {props_json}}}'
        features.append(feature_str)

    meta = {
        "min": p5,
        "max": p95,
        "metric": metric,
        "label": METRICS[metric]["label"],
        "format": METRICS[metric]["format"],
        "description": METRICS[metric]["description"],
    }
    meta_json = json.dumps(meta)
    features_joined = ",".join(features)

    return f'{{"type": "FeatureCollection", "features": [{features_joined}], "meta": {meta_json}}}'


@app.route('/')
def index():
    return render_template('index_athena.html')


@app.route('/api/metrics')
def get_metrics():
    return jsonify({
        k: {
            "label": v["label"],
            "description": v["description"],
            "format": v["format"],
        }
        for k, v in METRICS.items()
    })


@app.route('/api/states')
def get_states():
    year = int(request.args.get('year', 2024))
    metric = request.args.get('metric', 'median_household_income')

    if metric not in METRICS:
        return jsonify({"error": "Invalid metric"}), 400

    cache_key = f"geojson:states:{year}:{metric}"

    def generate_states_geojson():
        df = wr.athena.read_sql_query(
            sql=f"""
                SELECT
                    geography_id,
                    geography_name,
                    state_fips,
                    survey_year,
                    total_population,
                    median_age,
                    median_household_income,
                    median_home_value,
                    median_gross_rent,
                    pct_white_alone,
                    pct_black_alone,
                    pct_asian_alone,
                    pct_hispanic_or_latino,
                    poverty_rate,
                    bachelors_plus_rate,
                    high_school_rate,
                    owner_occupancy_rate,
                    renter_rate,
                    drove_alone_rate,
                    walked_to_work_rate,
                    remote_work_rate,
                    centroid_lat,
                    centroid_lon,
                    geometry_wkt
                FROM {ATHENA_DB_MARTS}.mart_socioeconomic_states
                WHERE survey_year = {year}
            """,
            database=ATHENA_DB_MARTS,
            workgroup=ATHENA_WORKGROUP,
            boto3_session=get_session()
        )
        return df_to_geojson(df, metric, include_geometry=True, geom_col='geometry_wkt')

    cached_geojson = get_cached_response(cache_key, generate_states_geojson)
    return Response(cached_geojson, mimetype='application/json')


@app.route('/api/counties')
def get_counties():
    year = int(request.args.get('year', 2024))
    state_fips = request.args.get('state_fips')
    metric = request.args.get('metric', 'median_household_income')

    if metric not in METRICS:
        return jsonify({"error": "Invalid metric"}), 400
    if not state_fips:
        return jsonify({"error": "state_fips required"}), 400

    cache_key = f"geojson:counties:{year}:{state_fips}:{metric}"

    def generate_counties_geojson():
        df = wr.athena.read_sql_query(
            sql=f"""
                SELECT
                    geography_id,
                    geography_name,
                    state_fips,
                    county_fips,
                    survey_year,
                    total_population,
                    median_age,
                    median_household_income,
                    median_home_value,
                    median_gross_rent,
                    pct_white_alone,
                    pct_black_alone,
                    pct_asian_alone,
                    pct_hispanic_or_latino,
                    poverty_rate,
                    bachelors_plus_rate,
                    high_school_rate,
                    owner_occupancy_rate,
                    renter_rate,
                    drove_alone_rate,
                    walked_to_work_rate,
                    remote_work_rate,
                    geometry_wkt
                FROM {ATHENA_DB_MARTS}.mart_socioeconomic_counties
                WHERE state_fips = '{state_fips}'
                AND survey_year = {year}
            """,
            database=ATHENA_DB_MARTS,
            workgroup=ATHENA_WORKGROUP,
            boto3_session=get_session()
        )
        return df_to_geojson(df, metric, include_geometry=True, geom_col='geometry_wkt')

    cached_geojson = get_cached_response(cache_key, generate_counties_geojson)
    return Response(cached_geojson, mimetype='application/json')


@app.route('/api/tracts')
def get_tracts():
    year = int(request.args.get('year', 2024))
    state_fips = request.args.get('state_fips')
    county_fips = request.args.get('county_fips')
    metric = request.args.get('metric', 'median_household_income')

    if metric not in METRICS:
        return jsonify({"error": "Invalid metric"}), 400
    if not state_fips:
        return jsonify({"error": "state_fips required"}), 400

    where_clause = f"state_fips = '{state_fips}' AND survey_year = {year}"
    if county_fips:
        where_clause += f" AND county_fips = '{county_fips}'"

    cache_key = f"geojson:tracts:{year}:{state_fips}:{county_fips or 'all'}:{metric}"

    def generate_tracts_geojson():
        df = wr.athena.read_sql_query(
            sql=f"""
                SELECT
                    geography_id,
                    geography_name,
                    state_fips,
                    county_fips,
                    tract_fips,
                    survey_year,
                    total_population,
                    median_age,
                    median_household_income,
                    median_home_value,
                    median_gross_rent,
                    pct_white_alone,
                    pct_black_alone,
                    pct_asian_alone,
                    pct_hispanic_or_latino,
                    poverty_rate,
                    bachelors_plus_rate,
                    high_school_rate,
                    owner_occupancy_rate,
                    renter_rate,
                    drove_alone_rate,
                    walked_to_work_rate,
                    remote_work_rate,
                    geometry_wkt
                FROM {ATHENA_DB_MARTS}.mart_socioeconomic_tracts
                WHERE {where_clause}
            """,
            database=ATHENA_DB_MARTS,
            workgroup=ATHENA_WORKGROUP,
            boto3_session=get_session()
        )
        return df_to_geojson(df, metric, include_geometry=True, geom_col='geometry_wkt')

    cached_geojson = get_cached_response(cache_key, generate_tracts_geojson)
    return Response(cached_geojson, mimetype='application/json')


@app.route('/api/migration')
def get_migration():
    year = int(request.args.get('year', 2023))
    state_fips = request.args.get('state_fips')
    min_households = int(request.args.get('min_households', 1000))

    session = get_session()

    cache_key = f"migration:{year}:{state_fips or 'all'}:{min_households}"

    def generate_migration_data():
        if state_fips:
            df = wr.athena.read_sql_query(
                sql=f"""
                    SELECT
                        origin_geography_id,
                        dest_geography_id,
                        survey_year,
                        households,
                        individuals,
                        agi,
                        is_non_migrant,
                        origin_state_name,
                        origin_median_income,
                        origin_poverty_rate,
                        dest_state_name,
                        dest_median_income,
                        dest_poverty_rate,
                        income_differential,
                        agi_per_household,
                        origin_lat,
                        origin_lon,
                        dest_lat,
                        dest_lon
                    FROM {ATHENA_DB_MARTS}.mart_migration_flows_states
                    WHERE survey_year = {year}
                    AND (origin_geography_id = '{state_fips}' OR dest_geography_id = '{state_fips}')
                    AND households >= {min_households}
                    AND is_non_migrant = false
                    ORDER BY households DESC
                """,
                database=ATHENA_DB_MARTS,
                workgroup=ATHENA_WORKGROUP,
                boto3_session=session
            )
        else:
            df = wr.athena.read_sql_query(
                sql=f"""
                    SELECT
                        origin_geography_id,
                        dest_geography_id,
                        survey_year,
                        households,
                        individuals,
                        agi,
                        is_non_migrant,
                        origin_state_name,
                        origin_median_income,
                        origin_poverty_rate,
                        dest_state_name,
                        dest_median_income,
                        dest_poverty_rate,
                        income_differential,
                        agi_per_household,
                        origin_lat,
                        origin_lon,
                        dest_lat,
                        dest_lon
                    FROM {ATHENA_DB_MARTS}.mart_migration_flows_states
                    WHERE survey_year = {year}
                    AND households >= {min_households}
                    AND is_non_migrant = false
                    ORDER BY households DESC
                    LIMIT 200
                """,
                database=ATHENA_DB_MARTS,
                workgroup=ATHENA_WORKGROUP,
                boto3_session=session
            )
        return df.to_json(orient='records')

    cached_data = get_cached_response(cache_key, generate_migration_data)
    return jsonify({"flows": json.loads(cached_data)})


@app.route('/api/years')
def get_years():
    df = wr.athena.read_sql_query(
        sql="SELECT DISTINCT survey_year FROM population_demographics_gold.gold_states ORDER BY survey_year",
        database=ATHENA_DB_GOLD,
        workgroup=ATHENA_WORKGROUP,
        boto3_session=get_session()
    )
    years = df['survey_year'].tolist()
    return jsonify({"years": years})


if __name__ == '__main__':
    app.run(debug=True, use_reloader=False, port=8001)
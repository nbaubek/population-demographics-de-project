from flask import Flask, jsonify, request, render_template, send_from_directory, Response
import duckdb
import json
import numpy as np
import matplotlib.cm as cm
from pathlib import Path
from dotenv import load_dotenv
import os

load_dotenv()

app = Flask(__name__)

DB_PATH = Path(__file__).resolve().parent / "data.duckdb"

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


def get_connection():
    return duckdb.connect(str(DB_PATH), read_only=True)


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


def rows_to_geojson(rows, columns, metric, include_geometry=True):
    cmap = METRICS[metric]["colormap"]
    metric_idx = columns.index(metric)
    geom_idx = columns.index("geometry_geojson") if include_geometry else None

    values = [row[metric_idx] if row[metric_idx] is not None else float('nan') for row in rows]
    colors, p5, p95 = compute_colors(values, cmap)

    features = []
    for i, row in enumerate(rows):
        props = {}
        for j, col in enumerate(columns):
            if col == "geometry_geojson":
                continue
            props[col] = row[j]

        props['fill_color'] = colors[i]

        # Serialize only the small properties dictionary (very fast)
        props_json = json.dumps(props)

        # Grab the raw geometry string from DuckDB
        if include_geometry and geom_idx is not None and row[geom_idx]:
            geom_str = row[geom_idx]
        else:
            geom_str = "null"

        # Construct the Feature string manually, bypassing json.loads() completely
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

    # Return a fully constructed raw JSON string
    return f'{{"type": "FeatureCollection", "features": [{features_joined}], "meta": {meta_json}}}'


@app.route('/')
def index():
    return render_template('index.html')


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

    con = get_connection()
    result = con.execute("""
        SELECT geography_id, geography_name, state_fips, survey_year,
               total_population, median_age, median_household_income,
               median_home_value, median_gross_rent,
               pct_white_alone, pct_black_alone, pct_asian_alone,
               pct_hispanic_or_latino, poverty_rate, bachelors_plus_rate,
               high_school_rate, owner_occupancy_rate, renter_rate,
               drove_alone_rate, walked_to_work_rate, remote_work_rate,
               centroid_lat, centroid_lon, geometry_geojson, aland, awater
        FROM states
        WHERE survey_year = ?
    """, [year]).fetchall()
    columns = [d[0] for d in con.description]
    con.close()

    # Serve the raw string directly
    return Response(rows_to_geojson(result, columns, metric), mimetype='application/json')


@app.route('/api/counties')
def get_counties():
    year = int(request.args.get('year', 2024))
    state_fips = request.args.get('state_fips')
    metric = request.args.get('metric', 'median_household_income')

    if metric not in METRICS:
        return jsonify({"error": "Invalid metric"}), 400
    if not state_fips:
        return jsonify({"error": "state_fips required"}), 400

    con = get_connection()
    result = con.execute("""
        SELECT geography_id, geography_name, state_fips, county_fips,
               survey_year, total_population, median_age,
               median_household_income, median_home_value, median_gross_rent,
               pct_white_alone, pct_black_alone, pct_asian_alone,
               pct_hispanic_or_latino, poverty_rate, bachelors_plus_rate,
               high_school_rate, owner_occupancy_rate, renter_rate,
               drove_alone_rate, walked_to_work_rate, remote_work_rate,
               geometry_geojson, aland, awater
        FROM counties
        WHERE state_fips = ? AND survey_year = ?
    """, [state_fips, year]).fetchall()
    columns = [d[0] for d in con.description]
    con.close()

    # Serve the raw string directly
    return Response(rows_to_geojson(result, columns, metric), mimetype='application/json')


@app.route('/api/migration')
def get_migration():
    year = int(request.args.get('year', 2023))
    state_fips = request.args.get('state_fips')
    min_households = int(request.args.get('min_households', 1000))

    con = get_connection()

    if state_fips:
        result = con.execute("""
            SELECT origin_geography_id, dest_geography_id, survey_year,
                   households, individuals, agi, is_non_migrant,
                   origin_state_name, origin_median_income, origin_poverty_rate,
                   dest_state_name, dest_median_income, dest_poverty_rate,
                   income_differential, agi_per_household,
                   origin_lat, origin_lon, dest_lat, dest_lon
            FROM migration_states
            WHERE survey_year = ?
            AND (origin_geography_id = ? OR dest_geography_id = ?)
            AND households >= ?
            AND is_non_migrant = false
            ORDER BY households DESC
        """, [year, state_fips, state_fips, min_households]).fetchall()
    else:
        result = con.execute("""
            SELECT origin_geography_id, dest_geography_id, survey_year,
                   households, individuals, agi, is_non_migrant,
                   origin_state_name, origin_median_income, origin_poverty_rate,
                   dest_state_name, dest_median_income, dest_poverty_rate,
                   income_differential, agi_per_household,
                   origin_lat, origin_lon, dest_lat, dest_lon
            FROM migration_states
            WHERE survey_year = ?
            AND households >= ?
            AND is_non_migrant = false
            ORDER BY households DESC
            LIMIT 200
        """, [year, min_households]).fetchall()

    columns = [d[0] for d in con.description]
    con.close()

    flows = []
    for row in result:
        flow = {col: row[i] for i, col in enumerate(columns)}
        flows.append(flow)

    return jsonify({"flows": flows})


@app.route('/api/years')
def get_years():
    con = get_connection()
    years = con.execute("SELECT DISTINCT survey_year FROM states ORDER BY survey_year").fetchall()
    con.close()
    return jsonify({"years": [y[0] for y in years]})


if __name__ == '__main__':
    app.run(debug=True, use_reloader=False, port=8000)
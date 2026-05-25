"""Shared Athena query helper for asset checks.

Uses fetch_results=True to get actual query results (not None).
Returns list-of-dicts with values, casting numeric strings to int/float.
"""

from typing import List, Dict, Any


def athena_query(athena, query: str) -> List[Dict[str, Any]]:
    """Execute Athena query with fetch_results=True and return list of dicts.

    Args:
        athena: CensusAthenaResource instance
        query: SQL query string

    Returns:
        List of dicts mapping column names to values. Numeric strings are
        cast to int/float. Returns [] on error or no results.
    """
    try:
        result = athena.execute_query(query, fetch_results=True)
        if not result:
            return []
        cols = _extract_column_names(query)
        rows = []
        for row in result:
            typed_row = {}
            for k, v in zip(cols, row):
                typed_row[k] = _cast_value(v)
            rows.append(typed_row)
        return rows
    except Exception:
        return []


def _cast_value(v: Any) -> Any:
    """Cast string values to int/float where possible, leave others unchanged."""
    if not isinstance(v, str):
        return v
    s = v.strip()
    if s == "" or s.lower() in ("none", "null"):
        return None
    # Try int
    try:
        return int(s)
    except ValueError:
        pass
    # Try float
    try:
        return float(s)
    except ValueError:
        pass
    return s


def _extract_column_names(query: str) -> List[str]:
    """Extract output column names from a SELECT query.

    Handles the SQL patterns used in this codebase:
        COUNT(*) as cnt
        COUNT(DISTINCT x) as year_count
        MAX(survey_year) as max_year
        bare column_name AS alias
        bare column_name

    NOTE: This regex-based parser only covers the patterns above. It will NOT
    correctly parse column names for:
        - COALESCE(...) as alias
        - Subqueries in SELECT
        - Window functions with OVER()
        - Complex expressions (arithmetics, CAST, etc.)
    If a future check uses a more complex SELECT, either extend this function
    or use Athena's query result metadata directly (column names are available
    in the GetQueryResults API response).
    """
    import re

    match = re.search(r"\bSELECT\s+(.*?)\s+FROM\b", query, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        return []
    select_part = match.group(1).strip()

    cols = []
    # Split by comma (respecting parentheses)
    for segment in re.split(r",(?![^()]*\))", select_part):
        segment = segment.strip()
        upper = segment.upper()

        as_match = re.search(r"\bAS\s+(\w+)\s*$", segment, flags=re.IGNORECASE)
        if as_match:
            cols.append(as_match.group(1))
        else:
            # Bare column or expression — strip function call wrapper
            name = re.sub(r"\(.*", "", segment).strip()
            cols.append(name)
    return cols
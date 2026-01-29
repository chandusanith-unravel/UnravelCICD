def recommend_environment_advanced(query_kpi: dict) -> str:
    """
    Advanced rule-based environment recommendation using physical plan & KPI metadata.
    """
    env = query_kpi.get("environment", "").lower()
    rows_read = query_kpi.get("rows_read", 0)
    rows_written = query_kpi.get("rows_written", 0)
    bytes_read = query_kpi.get("bytes_read_mb", 0)
    bytes_written = query_kpi.get("bytes_written_mb", 0)
    duration = query_kpi.get("total_duration_seconds", 0)
    operators = set(op.lower() for op in query_kpi.get("operator_names", []))

    # Normalize operator tags
    def has_ops(*ops):
        return any(op in operators for op in ops)

    # --------------------------
    # Heavy Compute conditions
    # --------------------------
    if has_ops("broadcasthashjoin", "shufflerepartition", "sortmergejoin", "shuffleexchange"):
        return "Prefer Photon Compute (heavy join or repartitioning)"

    if has_ops("batchevalpython"):
        return "Prefer Photon Compute (Python UDFs detected)"

    if rows_written > 10_000_000 or bytes_written > 2048:
        return "Prefer Photon Compute (Large write volume)"

    if duration > 1800:
        return "Prefer Photon Compute (long execution time)"

    if has_ops("scan", "union", "aggregate") and duration > 900 and bytes_read > 2048:
        return "Prefer Photon Compute (long read-heavy pipeline)"



    # --------------------------
    # Serverless-friendly cases
    # --------------------------
    if (
        rows_written < 2_000_000 and
        not has_ops("broadcasthashjoin", "shufflerepartition", "sortmergejoin", "batchevalpython") and
        duration < 600 and
        bytes_written < 1000 and
        has_ops("photonparquetwriter")
    ):
        return "Prefer Serverless (Photon write, no heavy ops, short job)"

    if (
        duration < 300 and
        rows_read < 1_000_000 and
        not has_ops("join", "union", "aggregate", "python")
    ):
        return "Prefer Serverless (lightweight read/project job)"

    # --------------------------
    # Neutral / Inconclusive
    # --------------------------
    return "Either environment acceptable or further tuning needed"

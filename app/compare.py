"""Comparison logic for comparing two job runs."""

from typing import Dict, List, Any, Optional, Tuple
from difflib import unified_diff
import json

# Allowlist of numeric Global Status metrics to compare
GLOBAL_STATUS_COMPARE_ALLOWLIST = [
    "Threads_running",
    "Threads_connected",
    "Slow_queries",
    "Select_scan",
    "Created_tmp_disk_tables",
    "Innodb_row_lock_waits",
    "Innodb_log_waits",
    "Opened_tables",
    "Table_open_cache_misses",
    "Table_open_cache_overflows",
]


def compare_global_status(
    status_a: Dict[str, Any], 
    status_b: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """
    Compare numeric Global Status counters between two jobs.
    Returns list of {metric, value_a, value_b, delta, direction}.
    """
    results = []
    
    for metric in GLOBAL_STATUS_COMPARE_ALLOWLIST:
        val_a = status_a.get(metric)
        val_b = status_b.get(metric)
        
        # Try to convert to numeric
        try:
            num_a = int(val_a) if val_a is not None else None
            num_b = int(val_b) if val_b is not None else None
        except (ValueError, TypeError):
            continue
        
        if num_a is None and num_b is None:
            continue
            
        num_a = num_a or 0
        num_b = num_b or 0
        delta = num_b - num_a
        
        # Determine direction (for these metrics, lower is usually better)
        if delta > 0:
            direction = "increase"  # Red - regression
        elif delta < 0:
            direction = "decrease"  # Green - improvement
        else:
            direction = "unchanged"  # Gray
            
        results.append({
            "metric": metric,
            "value_a": num_a,
            "value_b": num_b,
            "delta": delta,
            "direction": direction,
        })
    
    return results


def compare_processlist(
    pl_a: List[Dict[str, Any]], 
    pl_b: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Compare processlist summaries between two jobs.
    Returns summary comparison, not individual query diffs.
    """
    def summarize(pl: List[Dict[str, Any]]) -> Dict[str, Any]:
        if not pl:
            return {
                "total": 0,
                "long_running": 0,  # Time > 10s
                "with_state": 0,    # State != NULL
                "distinct_users": 0,
            }
        
        total = len(pl)
        long_running = sum(1 for p in pl if (p.get("Time") or 0) > 10)
        with_state = sum(1 for p in pl if p.get("State"))
        distinct_users = len(set(p.get("User", "") for p in pl if p.get("User")))
        
        return {
            "total": total,
            "long_running": long_running,
            "with_state": with_state,
            "distinct_users": distinct_users,
        }
    
    summary_a = summarize(pl_a)
    summary_b = summarize(pl_b)
    
    comparison = {}
    for key in summary_a:
        val_a = summary_a[key]
        val_b = summary_b[key]
        delta = val_b - val_a
        
        if delta > 0:
            direction = "increase"
        elif delta < 0:
            direction = "decrease"
        else:
            direction = "unchanged"
            
        comparison[key] = {
            "value_a": val_a,
            "value_b": val_b,
            "delta": delta,
            "direction": direction,
        }
    
    return comparison


def compare_config(
    config_a: Dict[str, Any], 
    config_b: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """
    Compare config variables between two jobs.
    Returns list of {variable, value_a, value_b, changed}.
    """
    all_keys = sorted(set(config_a.keys()) | set(config_b.keys()))
    results = []
    
    for key in all_keys:
        val_a = config_a.get(key, "—")
        val_b = config_b.get(key, "—")
        changed = str(val_a) != str(val_b)
        
        results.append({
            "variable": key,
            "value_a": val_a,
            "value_b": val_b,
            "changed": changed,
        })
    
    return results


def compare_buffer_pool(
    bp_a: Dict[str, Any],
    bp_b: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Compare buffer pool metrics between two jobs.
    Returns comparison dict for each metric.
    """
    metrics = [
        ("pool_size_gb", "Pool Size (GB)", False),  # (key, label, lower_is_better)
        ("used_gb", "Used (GB)", False),
        ("used_percent", "Used %", False),
        ("free_gb", "Free (GB)", False),
        ("free_percent", "Free %", False),
        ("dirty_percent", "Dirty %", True),  # Lower dirty pages is generally better
        ("hit_ratio", "Hit Ratio %", False),  # Higher is better, so increase is good
    ]
    
    results = {}
    for key, label, lower_is_better in metrics:
        val_a = bp_a.get(key)
        val_b = bp_b.get(key)
        
        # Handle None values
        if val_a is None and val_b is None:
            continue
        
        val_a = val_a if val_a is not None else 0
        val_b = val_b if val_b is not None else 0
        
        # Calculate delta
        try:
            delta = round(float(val_b) - float(val_a), 2)
        except (ValueError, TypeError):
            delta = 0
        
        # Determine direction based on whether lower is better
        if key == "hit_ratio":
            # For hit ratio, higher is better
            if delta > 0:
                direction = "improvement"
            elif delta < 0:
                direction = "regression"
            else:
                direction = "unchanged"
        elif lower_is_better:
            if delta > 0:
                direction = "regression"
            elif delta < 0:
                direction = "improvement"
            else:
                direction = "unchanged"
        else:
            # Neutral metrics (size, used, etc.) - just show change
            direction = "increase" if delta > 0 else "decrease" if delta < 0 else "unchanged"
        
        results[key] = {
            "label": label,
            "value_a": val_a,
            "value_b": val_b,
            "delta": delta,
            "direction": direction,
        }
    
    # Add health comparison
    health_a = bp_a.get("health", "unknown")
    health_b = bp_b.get("health", "unknown")
    results["health"] = {
        "label": "Health",
        "value_a": health_a,
        "value_b": health_b,
        "changed": health_a != health_b,
    }
    
    return results


def compare_innodb_text(
    text_a: str, 
    text_b: str
) -> List[Dict[str, Any]]:
    """
    Generate a simple text diff for InnoDB status.
    Returns list of {line, type} where type is 'added', 'removed', 'context', 'header'.
    """
    lines_a = (text_a or "").splitlines()
    lines_b = (text_b or "").splitlines()
    
    diff = unified_diff(
        lines_a, 
        lines_b, 
        fromfile="Job A", 
        tofile="Job B",
        lineterm=""
    )
    
    result = []
    for line in diff:
        if line.startswith("+++") or line.startswith("---"):
            result.append({"line": line, "type": "header"})
        elif line.startswith("@@"):
            result.append({"line": line, "type": "header"})
        elif line.startswith("+"):
            result.append({"line": line[1:], "type": "added"})
        elif line.startswith("-"):
            result.append({"line": line[1:], "type": "removed"})
        else:
            result.append({"line": line[1:] if line.startswith(" ") else line, "type": "context"})
    
    return result


def find_common_hosts(hosts_a: List[str], hosts_b: List[str]) -> List[str]:
    """Find hosts that exist in both jobs."""
    return sorted(set(hosts_a) & set(hosts_b))


def _safe_int(value: Any, default: int = 0) -> int:
    """Safely convert value to int."""
    if value is None:
        return default
    try:
        return int(value)
    except (ValueError, TypeError):
        return default


def _safe_float(value: Any, default: float = 0.0) -> float:
    """Safely convert value to float."""
    if value is None:
        return default
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def detect_regressions(
    status_a: Dict[str, Any],
    status_b: Dict[str, Any],
    processlist_a: List[Dict[str, Any]],
    processlist_b: List[Dict[str, Any]],
    system_info: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """
    Detect regression heuristics between two job snapshots.
    
    Returns list of regressions:
    [
        {
            "category": "locking",
            "severity": "critical",  # or "warning"
            "message": "Increased row lock contention",
            "metric": "Innodb_row_lock_waits",
            "section": "global_status"  # for UI linking
        }
    ]
    """
    regressions = []
    
    # Get system info with safe defaults
    cpu_cores = _safe_int(system_info.get("cpu_cores"), 4)
    
    # Helper to get metric values
    def get_a(key: str) -> int:
        return _safe_int(status_a.get(key))
    
    def get_b(key: str) -> int:
        return _safe_int(status_b.get(key))
    
    def delta(key: str) -> int:
        return get_b(key) - get_a(key)
    
    # =========================================================================
    # 1️⃣ Thread Pressure Regression
    # Threads_running increased by > 50% AND Threads_running > cpu_cores
    # =========================================================================
    threads_a = get_a("Threads_running")
    threads_b = get_b("Threads_running")
    if threads_a > 0:
        threads_increase_pct = ((threads_b - threads_a) / threads_a) * 100
        if threads_increase_pct > 50 and threads_b > cpu_cores:
            regressions.append({
                "category": "threads",
                "severity": "critical",
                "message": "Thread pressure increased significantly",
                "metric": "Threads_running",
                "section": "global_status"
            })
    
    # =========================================================================
    # 2️⃣ Slow Query Regression
    # Slow_queries delta > 0 AND rate increased (normalized by uptime)
    # =========================================================================
    slow_delta = delta("Slow_queries")
    uptime_a = get_a("Uptime")
    uptime_b = get_b("Uptime")
    if slow_delta > 0 and uptime_a > 0 and uptime_b > 0:
        rate_a = get_a("Slow_queries") / uptime_a
        rate_b = get_b("Slow_queries") / uptime_b
        if rate_b > rate_a:
            regressions.append({
                "category": "queries",
                "severity": "critical",
                "message": "Slow query rate increased",
                "metric": "Slow_queries",
                "section": "global_status"
            })
    
    # =========================================================================
    # 3️⃣ Temp Table Spill Regression
    # Created_tmp_disk_tables delta > 0 AND ratio increased
    # =========================================================================
    disk_tmp_delta = delta("Created_tmp_disk_tables")
    if disk_tmp_delta > 0:
        tmp_tables_a = get_a("Created_tmp_tables")
        tmp_tables_b = get_b("Created_tmp_tables")
        disk_tmp_a = get_a("Created_tmp_disk_tables")
        disk_tmp_b = get_b("Created_tmp_disk_tables")
        
        ratio_a = (disk_tmp_a / tmp_tables_a) if tmp_tables_a > 0 else 0
        ratio_b = (disk_tmp_b / tmp_tables_b) if tmp_tables_b > 0 else 0
        
        if ratio_b > ratio_a:
            regressions.append({
                "category": "temp_tables",
                "severity": "warning",
                "message": "Increased disk temp table usage",
                "metric": "Created_tmp_disk_tables",
                "section": "global_status"
            })
    
    # =========================================================================
    # 4️⃣ Lock Contention Regression
    # Innodb_row_lock_waits delta > 0 AND avg lock time increased
    # =========================================================================
    lock_waits_delta = delta("Innodb_row_lock_waits")
    if lock_waits_delta > 0:
        # Calculate average lock time
        lock_time_a = get_a("Innodb_row_lock_time")
        lock_time_b = get_b("Innodb_row_lock_time")
        lock_waits_a = get_a("Innodb_row_lock_waits")
        lock_waits_b = get_b("Innodb_row_lock_waits")
        
        avg_a = (lock_time_a / lock_waits_a) if lock_waits_a > 0 else 0
        avg_b = (lock_time_b / lock_waits_b) if lock_waits_b > 0 else 0
        
        if avg_b > avg_a:
            regressions.append({
                "category": "locking",
                "severity": "critical",
                "message": "Increased row lock contention",
                "metric": "Innodb_row_lock_waits",
                "section": "global_status"
            })
    
    # =========================================================================
    # 5️⃣ Buffer Pool Efficiency Regression
    # Hit ratio decreased by > 1%
    # =========================================================================
    reads_a = get_a("Innodb_buffer_pool_read_requests")
    reads_b = get_b("Innodb_buffer_pool_read_requests")
    disk_reads_a = get_a("Innodb_buffer_pool_reads")
    disk_reads_b = get_b("Innodb_buffer_pool_reads")
    
    if reads_a > 0 and reads_b > 0:
        hit_ratio_a = ((reads_a - disk_reads_a) / reads_a) * 100 if reads_a > 0 else 100
        hit_ratio_b = ((reads_b - disk_reads_b) / reads_b) * 100 if reads_b > 0 else 100
        
        if (hit_ratio_a - hit_ratio_b) > 1:
            regressions.append({
                "category": "buffer_pool",
                "severity": "warning",
                "message": "Buffer pool efficiency degraded",
                "metric": "Innodb_buffer_pool_read_requests",
                "section": "global_status"
            })
    
    # =========================================================================
    # 6️⃣ Table Cache Regression
    # Table_open_cache_overflows delta > 0 OR misses increased sharply
    # =========================================================================
    cache_overflows_delta = delta("Table_open_cache_overflows")
    cache_misses_a = get_a("Table_open_cache_misses")
    cache_misses_b = get_b("Table_open_cache_misses")
    
    if cache_overflows_delta > 0:
        regressions.append({
            "category": "table_cache",
            "severity": "critical",
            "message": "Table cache pressure increased",
            "metric": "Table_open_cache_overflows",
            "section": "global_status"
        })
    elif cache_misses_a > 0:
        # "Sharply" = more than 50% increase
        misses_increase_pct = ((cache_misses_b - cache_misses_a) / cache_misses_a) * 100
        if misses_increase_pct > 50:
            regressions.append({
                "category": "table_cache",
                "severity": "critical",
                "message": "Table cache pressure increased",
                "metric": "Table_open_cache_misses",
                "section": "global_status"
            })
    
    # =========================================================================
    # 7️⃣ Redo Log Pressure Regression
    # Innodb_log_waits delta > 0
    # =========================================================================
    log_waits_delta = delta("Innodb_log_waits")
    if log_waits_delta > 0:
        regressions.append({
            "category": "redo_log",
            "severity": "critical",
            "message": "Redo log contention detected",
            "metric": "Innodb_log_waits",
            "section": "global_status"
        })
    
    # =========================================================================
    # 8️⃣ Processlist Runtime Regression
    # Queries with Time > 10s increased OR Threads_running up while throughput same
    # =========================================================================
    def count_long_running(pl: List[Dict[str, Any]]) -> int:
        return sum(1 for p in pl if _safe_int(p.get("Time")) > 10)
    
    long_a = count_long_running(processlist_a or [])
    long_b = count_long_running(processlist_b or [])
    
    if long_b > long_a:
        regressions.append({
            "category": "processlist",
            "severity": "warning",
            "message": "More long-running queries detected",
            "metric": "Processlist",
            "section": "processlist"
        })
    else:
        # Check if threads_running increased but throughput unchanged
        threads_delta = delta("Threads_running")
        questions_a = get_a("Questions")
        questions_b = get_b("Questions")
        
        if threads_delta > 0 and uptime_a > 0 and uptime_b > 0:
            qps_a = questions_a / uptime_a
            qps_b = questions_b / uptime_b
            # If QPS didn't increase significantly (< 10%) but threads did
            if qps_a > 0:
                qps_change_pct = ((qps_b - qps_a) / qps_a) * 100
                if qps_change_pct < 10 and threads_delta > 0:
                    regressions.append({
                        "category": "processlist",
                        "severity": "warning",
                        "message": "More long-running queries detected",
                        "metric": "Threads_running",
                        "section": "processlist"
                    })
    
    # Sort: critical first, then warning
    severity_order = {"critical": 0, "warning": 1}
    regressions.sort(key=lambda x: severity_order.get(x["severity"], 2))
    
    return regressions


# =============================================================================
# ROOT-CAUSE HIERARCHY (fixed priority order)
# =============================================================================
ROOT_CAUSE_HIERARCHY = [
    "threads",        # 1. thread_pressure
    "locking",        # 2. locking
    "table_cache",    # 3. table_cache
    "buffer_pool",    # 4. buffer_pool
    "redo_log",       # 5. redo_log
    "temp_tables",    # 6. temp_tables
    "processlist",    # 7. processlist_runtime
]

# Suppression rules: if key exists, suppress values
SUPPRESSION_MAP = {
    "threads": ["processlist", "temp_tables"],
    "locking": ["processlist"],
    "buffer_pool": [],  # Could suppress disk_read if we had that category
}


def _generate_regression_id(reg: Dict[str, Any], host_id: str = "") -> str:
    """Generate a stable ID for a regression."""
    return f"{host_id}:{reg['category']}:{reg['metric']}"


def refine_regressions(
    regressions: List[Dict[str, Any]],
    status_a: Dict[str, Any],
    status_b: Dict[str, Any],
    all_host_regressions: Optional[List[List[Dict[str, Any]]]] = None
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Apply noise reduction and root-cause hierarchy to regressions.
    
    Args:
        regressions: List of raw regressions for a single host
        status_a: Global status from Job A
        status_b: Global status from Job B
        all_host_regressions: Optional list of regressions from all hosts (for cross-host correlation)
    
    Returns:
        Tuple of (visible_regressions, suppressed_regressions)
    """
    if not regressions:
        return [], []
    
    # Add IDs and initialize suppression fields
    processed = []
    for reg in regressions:
        processed.append({
            **reg,
            "id": _generate_regression_id(reg),
            "suppressed": False,
            "suppression_reason": None,
            "original_severity": reg["severity"],
        })
    
    # Get uptime for cold start detection
    uptime_a = _safe_int(status_a.get("Uptime"))
    
    # Track which categories are present
    present_categories = set(r["category"] for r in processed)
    
    # =========================================================================
    # Rule 1 — Root Cause Suppression
    # =========================================================================
    for reg in processed:
        if reg["suppressed"]:
            continue
            
        # Check if this regression should be suppressed by a higher-priority one
        for dominant_cat, suppressed_cats in SUPPRESSION_MAP.items():
            if dominant_cat in present_categories and reg["category"] in suppressed_cats:
                reg["suppressed"] = True
                reg["suppression_reason"] = f"Derived from {dominant_cat.replace('_', ' ')} regression"
                break
    
    # =========================================================================
    # Rule 2 — Low Signal Suppression
    # Suppress if delta < 5% AND absolute value is small
    # =========================================================================
    for reg in processed:
        if reg["suppressed"]:
            continue
        
        metric = reg.get("metric")
        if metric and metric != "Processlist":
            val_a = _safe_int(status_a.get(metric))
            val_b = _safe_int(status_b.get(metric))
            
            if val_a > 0:
                delta_pct = abs((val_b - val_a) / val_a) * 100
                abs_delta = abs(val_b - val_a)
                
                # Small absolute + small percentage = low signal
                if delta_pct < 5 and abs_delta < 10:
                    reg["suppressed"] = True
                    reg["suppression_reason"] = f"Low signal: {delta_pct:.1f}% change, delta={abs_delta}"
    
    # =========================================================================
    # Rule 3 — Cold Start Suppression
    # If job_a.uptime < 600 seconds, suppress buffer_pool and table_cache
    # =========================================================================
    if uptime_a > 0 and uptime_a < 600:
        for reg in processed:
            if reg["suppressed"]:
                continue
            if reg["category"] in ["buffer_pool", "table_cache"]:
                reg["suppressed"] = True
                reg["suppression_reason"] = f"Cold start: Job A uptime was only {uptime_a}s"
    
    # =========================================================================
    # Rule 4 — Single Event Downgrade
    # If triggered by exactly 1 event, downgrade critical → warning
    # =========================================================================
    for reg in processed:
        if reg["suppressed"]:
            continue
        
        metric = reg.get("metric")
        if metric and metric != "Processlist":
            val_a = _safe_int(status_a.get(metric))
            val_b = _safe_int(status_b.get(metric))
            abs_delta = abs(val_b - val_a)
            
            if abs_delta == 1 and reg["severity"] == "critical":
                reg["severity"] = "warning"
                reg["suppression_reason"] = "Downgraded: single event"
    
    # =========================================================================
    # Rule 5 — Cross-Host Correlation
    # If same regression on all hosts, downgrade severity by one level
    # =========================================================================
    if all_host_regressions and len(all_host_regressions) > 1:
        # Count how many hosts have each category
        category_counts = {}
        for host_regs in all_host_regressions:
            host_cats = set(r["category"] for r in host_regs)
            for cat in host_cats:
                category_counts[cat] = category_counts.get(cat, 0) + 1
        
        num_hosts = len(all_host_regressions)
        for reg in processed:
            if reg["suppressed"]:
                continue
            
            cat = reg["category"]
            if category_counts.get(cat, 0) == num_hosts:
                # Present on ALL hosts - downgrade
                if reg["severity"] == "critical":
                    reg["severity"] = "warning"
                    if not reg["suppression_reason"]:
                        reg["suppression_reason"] = "Downgraded: affects all hosts"
    
    # =========================================================================
    # Split into visible and suppressed
    # =========================================================================
    visible = [r for r in processed if not r["suppressed"]]
    suppressed = [r for r in processed if r["suppressed"]]
    
    # Sort visible: severity first, then by root-cause hierarchy
    def sort_key(r):
        severity_order = {"critical": 0, "warning": 1}
        try:
            hierarchy_order = ROOT_CAUSE_HIERARCHY.index(r["category"])
        except ValueError:
            hierarchy_order = 99
        return (severity_order.get(r["severity"], 2), hierarchy_order)
    
    visible.sort(key=sort_key)
    suppressed.sort(key=sort_key)
    
    return visible, suppressed


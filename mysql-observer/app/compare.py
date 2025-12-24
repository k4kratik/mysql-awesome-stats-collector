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


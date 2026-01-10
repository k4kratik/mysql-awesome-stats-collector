"""MySQL diagnostic data collector using PyMySQL."""

import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, Tuple, List, Union
import time
import re
import pymysql
from pymysql import OperationalError, Error

from .utils import (
    HostConfig,
    get_host_by_id,
    ensure_output_dir,
    get_job_dir,
)
from .parser import (
    parse_innodb_status, 
    parse_global_status, 
    parse_processlist, 
    parse_config_variables, 
    parse_replica_status, 
    parse_master_status, 
    calculate_buffer_pool_metrics,
    analyze_innodb_health,
    CONFIG_VARIABLES_ALLOWLIST
)
from .db import get_db_context
from .models import Job, JobHost, JobStatus, HostJobStatus

logger = logging.getLogger("masc.collector")


# Commands to execute - run in parallel for speed
COMMANDS = [
    "SHOW ENGINE INNODB STATUS",
    "SHOW GLOBAL STATUS",
    "SHOW FULL PROCESSLIST",
    "SHOW GLOBAL VARIABLES",
    "SHOW REPLICA STATUS",  # For replica lag (MySQL 8.0.22+)
    "SHOW MASTER STATUS",   # For master binlog position (to compare with replicas)
]


def _timestamp() -> str:
    """Get current timestamp string."""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _update_progress(progress_file: Optional[Path], progress: Dict[str, Any]) -> None:
    """Write progress to file for real-time status updates."""
    if progress_file:
        try:
            with open(progress_file, "w") as f:
                json.dump(progress, f)
        except Exception:
            pass  # Non-critical, don't fail collection


def _create_mysql_connection(host: HostConfig, timeout: int = 120) -> pymysql.Connection:
    """
    Create a PyMySQL connection to the host.
    
    Args:
        host: Host configuration
        timeout: Connection timeout in seconds
    
    Returns:
        PyMySQL connection object
    
    Raises:
        OperationalError: If connection fails
    """
    return pymysql.connect(
        host=host.host,
        port=host.port,
        user=host.user,
        password=host.password,
        connect_timeout=timeout,
        read_timeout=timeout,
        write_timeout=timeout,
        cursorclass=pymysql.cursors.DictCursor
    )


def _format_result_as_text(command: str, result: Union[List[Dict[str, Any]], Dict[str, Any], str], duration: float, cmd_start: str, cmd_end: str) -> str:
    """
    Format PyMySQL result as text output (for backward compatibility with raw.txt).
    
    Args:
        command: The SQL command executed
        result: The result from PyMySQL (list of dicts, single dict, or string)
        duration: Execution duration in seconds
        cmd_start: Start timestamp
        cmd_end: End timestamp
    
    Returns:
        Formatted text output matching CLI format
    """
    output = f"\n{'='*60}\n"
    output += f"-- {command}\n"
    output += f"-- Time: {cmd_start} -> {cmd_end} ({duration:.2f}s)\n"
    output += f"{'='*60}\n"
    
    if isinstance(result, str):
        # For SHOW ENGINE INNODB STATUS, result is the Status column text
        output += result
    elif isinstance(result, dict):
        # Single row result (e.g., SHOW REPLICA STATUS, SHOW MASTER STATUS)
        if result:
            # Format as tab-separated header and row
            headers = list(result.keys())
            values = [str(result.get(h, '')) for h in headers]
            output += "\t".join(headers) + "\n"
            output += "\t".join(values) + "\n"
    elif isinstance(result, list):
        # Multiple rows (e.g., SHOW GLOBAL STATUS, SHOW PROCESSLIST)
        if result:
            headers = list(result[0].keys())
            output += "\t".join(headers) + "\n"
            for row in result:
                values = [str(row.get(h, '')) for h in headers]
                output += "\t".join(values) + "\n"
    
    return output



def get_mysql_version(host: HostConfig) -> Tuple[Optional[str], float]:
    """
    Get MySQL version from host.
    
    Returns:
        Tuple of (version_string, duration_seconds)
    """
    cmd = "SELECT VERSION()"
    _, success, result, _, duration = _run_single_command(host, cmd)
    
    if not success:
        return None, duration
    
    # Result is a string (the version)
    if isinstance(result, str) and result:
        return result, duration
    
    return None, duration


def _parse_version_tuple(version_str: str) -> Tuple[int, int, int]:
    """Parse version string into tuple of ints for comparison."""
    if not version_str:
        return (0, 0, 0)
        
    # Extract just the version number (remove suffixes like -log, -ubuntu, etc)
    # Match pattern: d.d.d
    match = re.search(r'(\d+)\.(\d+)\.(\d+)', version_str)
    if match:
        try:
            return (int(match.group(1)), int(match.group(2)), int(match.group(3)))
        except (ValueError, IndexError):
            pass
            
    # Fallback: simple split
    parts = version_str.split('.')
    try:
        major = int(parts[0]) if len(parts) > 0 else 0
        minor = int(parts[1]) if len(parts) > 1 else 0
        patch = 0 # Ignore patch for now unless strictly needed
        
        # refinement for 8.0.22 check if needed
        if len(parts) > 2:
            p_part = parts[2]
            # remove non-numeric
            p_clean = "".join(c for c in p_part if c.isdigit())
            if p_clean:
                patch = int(p_clean)
                
        return (major, minor, patch)
    except (ValueError, TypeError):
        return (0, 0, 0)


def _run_single_command(
    host: HostConfig, 
    command: str
) -> Tuple[str, bool, Union[List[Dict[str, Any]], Dict[str, Any], str], str, float]:
    """
    Run a single MySQL command using PyMySQL.
    
    Returns:
        Tuple of (command, success, structured_result, formatted_text, duration_seconds)
        structured_result: List of dicts for multi-row results, dict for single row, or str for InnoDB status
        formatted_text: Text formatted output for backward compatibility
    """
    host_label = f"{host.label} ({host.host}:{host.port})"
    start_time = time.time()
    cmd_start = _timestamp()
    
    try:
        conn = _create_mysql_connection(host, timeout=120)
        logger.info(f"[DB CONNECT] {host_label} | {command}")
        
        try:
            with conn.cursor() as cursor:
                cursor.execute(command)
                
                # Handle different command types
                if command.upper().startswith("SHOW ENGINE INNODB STATUS"):
                    # SHOW ENGINE INNODB STATUS returns Type, Name, Status columns
                    # We want the Status column value (the InnoDB monitor text)
                    row = cursor.fetchone()
                    if row and "Status" in row:
                        result = row["Status"]
                    else:
                        result = ""
                elif command.upper().startswith("SHOW REPLICA STATUS") or \
                     command.upper().startswith("SHOW SLAVE STATUS") or \
                     command.upper().startswith("SHOW MASTER STATUS") or \
                     command.upper().startswith("SHOW BINARY LOG STATUS"):
                    # These return a single row or empty
                    row = cursor.fetchone()
                    result = row if row else {}
                elif command.upper().startswith("SELECT VERSION()"):
                    # SELECT VERSION() returns a single row with VERSION() column
                    row = cursor.fetchone()
                    if row:
                        result = row.get("VERSION()", "")
                    else:
                        result = ""
                else:
                    # Other SHOW commands return multiple rows
                    result = cursor.fetchall()
            
            duration = time.time() - start_time
            cmd_end = _timestamp()
            logger.info(f"[DB DISCONNECT] OK ({duration:.2f}s) | {host_label} | {command}")
            
            # Format result as text for backward compatibility
            formatted_text = _format_result_as_text(command, result, duration, cmd_start, cmd_end)
            
            return (command, True, result, formatted_text, duration)
            
        except Error as e:
            duration = time.time() - start_time
            error_msg = str(e)
            logger.warning(f"[DB DISCONNECT] ERROR ({duration:.2f}s) | {host_label} | {command}: {error_msg}")
            formatted_text = _format_result_as_text(command, "", duration, cmd_start, _timestamp())
            return (command, False, error_msg, formatted_text, duration)
        finally:
            conn.close()
            
    except OperationalError as e:
        duration = time.time() - start_time
        error_msg = str(e)
        logger.warning(f"[DB CONNECT] FAILED ({duration:.2f}s) | {host_label} | {command}: {error_msg}")
        formatted_text = _format_result_as_text(command, "", duration, cmd_start, _timestamp())
        return (command, False, error_msg, formatted_text, duration)
    except Exception as e:
        duration = time.time() - start_time
        error_msg = str(e)
        logger.exception(f"[DB] EXCEPTION ({duration:.2f}s) | {host_label} | {command}: {error_msg}")
        formatted_text = _format_result_as_text(command, "", duration, cmd_start, _timestamp())
        return (command, False, error_msg, formatted_text, duration)


def run_mysql_commands_parallel(
    host: HostConfig, 
    progress_file: Optional[Path] = None,
    known_version: Optional[str] = None
) -> Tuple[bool, str, Dict[str, Any], Dict[str, Union[List[Dict[str, Any]], Dict[str, Any], str]]]:
    """
    Run MySQL diagnostic commands in PARALLEL using PyMySQL.
    
    Args:
        host: Host configuration
        progress_file: Optional path to write real-time progress updates
        known_version: Optional pre-fetched MySQL version string
    
    Returns:
        Tuple of (success, combined_output_text, timing_metrics, structured_results)
        structured_results: Dict mapping command -> structured result
    """
    host_label = f"{host.label} ({host.host}:{host.port})"
    collection_start = _timestamp()
    overall_start = time.time()
    
    logger.info(f"[PARALLEL] Starting collection for {host_label}")
    
    # Get MySQL Version first to determine correct commands (or use known version)
    if known_version:
        version_str = known_version
        logger.debug(f"[{host.label}] Using known MySQL version: {version_str}")
    else:
        version_str, v_duration = get_mysql_version(host)
    
    major, minor, patch = _parse_version_tuple(version_str)
    logger.info(f"[{host.label}] Detected MySQL version: {version_str} ({major}.{minor}.{patch})")

    
    # Build dynamic command list
    current_commands = []
    
    # Standard commands
    current_commands.append("SHOW ENGINE INNODB STATUS")
    current_commands.append("SHOW GLOBAL STATUS")
    current_commands.append("SHOW FULL PROCESSLIST")
    current_commands.append("SHOW GLOBAL VARIABLES")
    
    # Version specific commands
    
    # Replica/Slave status
    # MySQL 8.0.22+ deprecated SHOW SLAVE STATUS in favor of SHOW REPLICA STATUS
    if (major > 8) or (major == 8 and minor > 0) or (major == 8 and minor == 0 and patch >= 22):
        current_commands.append("SHOW REPLICA STATUS")
    else:
        current_commands.append("SHOW SLAVE STATUS")
        
    # Master/Binary Log status
    # MySQL 8.4+ removed SHOW MASTER STATUS, replaced with SHOW BINARY LOG STATUS
    if (major > 8) or (major == 8 and minor >= 4):
        current_commands.append("SHOW BINARY LOG STATUS")
    else:
        current_commands.append("SHOW MASTER STATUS")
    
    logger.info(f"[{host.label}] Selected commands: {current_commands}")
    
    # Run all commands in parallel
    results = {}  # command -> (success, structured_result, formatted_text, duration)
    structured_results = {}  # command -> structured_result
    timing = {
        "started_at": collection_start,
        "commands": {}
    }
    
    # Initialize progress tracking
    progress = {
        "phase": "commands",
        "started_at": collection_start,
        "total_commands": len(current_commands),
        "completed_commands": 0,
        "commands": {cmd: {"status": "pending"} for cmd in current_commands}
    }
    _update_progress(progress_file, progress)
    
    with ThreadPoolExecutor(max_workers=len(current_commands)) as executor:
        # Submit all commands
        futures = {
            executor.submit(_run_single_command, host, cmd): cmd 
            for cmd in current_commands
        }
        
        # Collect results as they complete
        for future in as_completed(futures):
            command = futures[future]
            try:
                cmd_name, success, structured_result, formatted_text, duration = future.result()
                results[cmd_name] = (success, formatted_text)
                structured_results[cmd_name] = structured_result
                timing["commands"][cmd_name] = {
                    "duration": round(duration, 3),
                    "success": success
                }
                # Update progress
                progress["completed_commands"] += 1
                progress["commands"][cmd_name] = {
                    "status": "completed" if success else "failed",
                    "duration": round(duration, 3)
                }
                _update_progress(progress_file, progress)
            except Exception as e:
                logger.exception(f"[PARALLEL] Exception for {command}: {e}")
                error_msg = str(e)
                results[command] = (False, error_msg)
                structured_results[command] = error_msg
                timing["commands"][command] = {
                    "duration": 0,
                    "success": False,
                    "error": error_msg
                }
                progress["completed_commands"] += 1
                progress["commands"][command] = {"status": "failed", "error": error_msg}
                _update_progress(progress_file, progress)
    
    overall_duration = time.time() - overall_start
    timing["completed_at"] = _timestamp()
    timing["total_duration"] = round(overall_duration, 3)
    
    # Check if any command failed
    all_success = all(success for success, _ in results.values())
    
    # Build combined output in command order (for raw.txt backward compatibility)
    all_output = []
    all_output.append(f"{'#'*60}")
    all_output.append(f"# MySQL Diagnostic Collection (PARALLEL)")
    all_output.append(f"# Host: {host.host}:{host.port}")
    all_output.append(f"# User: {host.user}")
    all_output.append(f"# Started: {collection_start}")
    all_output.append(f"# Mode: Parallel ({len(current_commands)} concurrent connections)")
    all_output.append(f"# Server Version: {version_str}")
    all_output.append(f"{'#'*60}")
    
    for command in current_commands:
        if command in results:
            success, formatted_text = results[command]
            if success:
                all_output.append(formatted_text)
            else:
                all_output.append(f"\n{'='*60}")
                all_output.append(f"-- {command}")
                all_output.append(f"-- ERROR: {formatted_text}")
                all_output.append(f"{'='*60}")
    
    all_output.append(f"\n{'#'*60}")
    all_output.append(f"# Collection completed: {_timestamp()}")
    all_output.append(f"# Total time: {overall_duration:.2f}s (parallel)")
    all_output.append(f"{'#'*60}")
    
    if all_success:
        logger.info(f"[PARALLEL] Completed {host_label} in {overall_duration:.2f}s")
    else:
        failed = [cmd for cmd, (s, _) in results.items() if not s]
        logger.warning(f"[PARALLEL] Completed {host_label} with errors in {overall_duration:.2f}s - Failed: {failed}")
    
    return all_success, "\n".join(all_output), timing, structured_results


# Keep the old sequential function for fallback
def run_mysql_command(host: HostConfig) -> tuple[bool, str]:
    """
    Run MySQL diagnostic commands via PyMySQL (SEQUENTIAL - legacy).
    
    Args:
        host: Host configuration
    
    Returns:
        Tuple of (success, output/error)
    """
    all_output = []
    collection_start = _timestamp()
    all_output.append(f"{'#'*60}")
    all_output.append(f"# MySQL Diagnostic Collection")
    all_output.append(f"# Host: {host.host}:{host.port}")
    all_output.append(f"# User: {host.user}")
    all_output.append(f"# Started: {collection_start}")
    all_output.append(f"{'#'*60}")
    
    host_label = f"{host.label} ({host.host}:{host.port})"
    
    for command in COMMANDS:
        cmd_name, success, _, formatted_text, _ = _run_single_command(host, command)
        if not success:
            return False, f"[{_timestamp()}] MySQL error: {formatted_text}"
        all_output.append(formatted_text)
    
    all_output.append(f"\n{'#'*60}")
    all_output.append(f"# Collection completed: {_timestamp()}")
    all_output.append(f"{'#'*60}")
    
    return True, "\n".join(all_output)


def collect_host_data(job_id: str, host_id: str, collect_hot_tables: bool = False) -> bool:
    """
    Collect diagnostic data from a single host using PARALLEL command execution.
    
    Args:
        job_id: Job identifier
        host_id: Host identifier
        collect_hot_tables: Whether to query performance_schema for hot tables
    
    Returns:
        True if successful, False otherwise
    """
    # Get host configuration
    host = get_host_by_id(host_id)
    if not host:
        logger.error(f"[{job_id[:8]}] Host {host_id} not found in configuration")
        _update_host_status(job_id, host_id, HostJobStatus.failed, f"Host {host_id} not found")
        return False
    
    logger.info(f"[{job_id[:8]}] Starting PARALLEL collection for {host.label} ({host.host}:{host.port})")
    
    # Fetch MySQL version early to update status
    try:
        version_str, _ = get_mysql_version(host)
    except Exception as e:
        logger.warning(f"[{job_id[:8]}] Failed to fetch version early: {e}")
        version_str = None

    # Update status to running with version info
    if version_str:
         _update_host_status(job_id, host_id, HostJobStatus.running, mysql_version=version_str)
    else:
         _update_host_status(job_id, host_id, HostJobStatus.running)
    
    # Ensure output directory exists FIRST (for progress file)
    output_dir = ensure_output_dir(job_id, host_id)
    progress_file = output_dir / "progress.json"
    
    # Track total collection time (including parsing and hot tables)
    total_start_time = datetime.now()
    
    # Run MySQL commands in PARALLEL (with real-time progress)
    start_time = datetime.now()
    success, output, timing, structured_results = run_mysql_commands_parallel(host, progress_file, known_version=version_str)
    commands_elapsed = (datetime.now() - start_time).total_seconds()
    logger.info(f"[{job_id[:8]}] MySQL commands for {host.label} completed in {commands_elapsed:.1f}s")
    
    # Normalize output - convert literal \n to actual newlines (MySQL escapes them in InnoDB status)
    if '\\n' in output:
        output = output.replace('\\n', '\n')
    
    # Always save raw output (for backward compatibility)
    raw_file = output_dir / "raw.txt"
    with open(raw_file, "w") as f:
        f.write(output)
    
    # Save timing metrics
    timing_file = output_dir / "timing.json"
    with open(timing_file, "w") as f:
        json.dump(timing, f, indent=2)
    
    if not success:
        total_elapsed = (datetime.now() - total_start_time).total_seconds()
        logger.error(f"[{job_id[:8]}] Collection FAILED for {host.label} after {total_elapsed:.1f}s: {output[:100]}")
        _update_progress(progress_file, {"phase": "failed", "error": output[:200]})
        _update_host_status(job_id, host_id, HostJobStatus.failed, output)
        return False
    
    try:
        # Update progress to parsing phase
        _update_progress(progress_file, {"phase": "parsing", "message": "Processing collected data..."})
        parse_start = datetime.now()
        
        # Get InnoDB status text (for parsers that need it)
        innodb_status_text = ""
        if "SHOW ENGINE INNODB STATUS" in structured_results:
            innodb_result = structured_results["SHOW ENGINE INNODB STATUS"]
            if isinstance(innodb_result, str):
                innodb_status_text = innodb_result
        
        # Parse and save InnoDB status (still needs text input)
        innodb_content = parse_innodb_status(output) if innodb_status_text else "InnoDB status not available"
        innodb_file = output_dir / "innodb.txt"
        with open(innodb_file, "w") as f:
            f.write(innodb_content)
        
        # Parse and save Global Status (using structured data)
        global_status_result = structured_results.get("SHOW GLOBAL STATUS", [])
        if isinstance(global_status_result, list):
            global_status = parse_global_status(global_status_result)
        else:
            global_status = {}
        global_status_file = output_dir / "global_status.json"
        with open(global_status_file, "w") as f:
            json.dump(global_status, f, indent=2)
        logger.debug(f"[{job_id[:8]}] Parsed {len(global_status)} global status variables")
        
        # Parse and save Processlist (using structured data)
        processlist_result = structured_results.get("SHOW FULL PROCESSLIST", [])
        if isinstance(processlist_result, list):
            processlist = parse_processlist(processlist_result)
        else:
            processlist = []
        processlist_file = output_dir / "processlist.json"
        with open(processlist_file, "w") as f:
            json.dump(processlist, f, indent=2)
        logger.debug(f"[{job_id[:8]}] Parsed {len(processlist)} processes")
        
        # Parse and save Config Variables (using structured data)
        config_vars_result = structured_results.get("SHOW GLOBAL VARIABLES", [])
        if isinstance(config_vars_result, list):
            config_vars_all = parse_config_variables(config_vars_result, filter_allowlist=False)
        else:
            config_vars_all = {}
        config_vars_file = output_dir / "config_vars.json"
        with open(config_vars_file, "w") as f:
            json.dump(config_vars_all, f, indent=2)
        logger.debug(f"[{job_id[:8]}] Parsed {len(config_vars_all)} config variables")
        
        # Calculate and save Buffer Pool metrics (derived from global_status + config_vars)
        buffer_pool = calculate_buffer_pool_metrics(global_status, config_vars_all)
        buffer_pool_file = output_dir / "buffer_pool.json"
        with open(buffer_pool_file, "w") as f:
            json.dump(buffer_pool, f, indent=2)
        if buffer_pool.get("pool_size_gb"):
            logger.debug(f"[{job_id[:8]}] Buffer pool: {buffer_pool['pool_size_gb']}GB, hit ratio: {buffer_pool.get('hit_ratio')}%")
        
        # Parse and save Replica Status (using structured data)
        replica_result = structured_results.get("SHOW REPLICA STATUS") or structured_results.get("SHOW SLAVE STATUS")
        if isinstance(replica_result, dict):
            replica_status = parse_replica_status(replica_result)
        else:
            replica_status = {"is_replica": False}
        replica_status_file = output_dir / "replica_status.json"
        with open(replica_status_file, "w") as f:
            json.dump(replica_status, f, indent=2)
        if replica_status.get("is_replica"):
            lag = replica_status.get("seconds_behind_master")
            lag_str = f"{lag}s" if lag is not None else "NULL"
            logger.info(f"[{job_id[:8]}] Replica lag for {host.label}: {lag_str}")
        else:
            logger.warning(f"[{job_id[:8]}] {host.label} is not a replica (or status unavailable) - parsed result: {replica_status}")
        
        # Parse and save Master Status (using structured data)
        master_result = structured_results.get("SHOW MASTER STATUS") or structured_results.get("SHOW BINARY LOG STATUS")
        if isinstance(master_result, dict):
            master_status = parse_master_status(master_result)
        else:
            master_status = {"is_master": False}
        master_status_file = output_dir / "master_status.json"
        with open(master_status_file, "w") as f:
            json.dump(master_status, f, indent=2)
        if master_status.get("is_master"):
            logger.info(f"[{job_id[:8]}] Master binlog for {host.label}: {master_status.get('file')}:{master_status.get('position')}")
        else:
            logger.debug(f"[{job_id[:8]}] {host.label} has no master status (binlog disabled or not primary)")
        
        # Analyze InnoDB Health (deadlocks, lock contention, hot indexes, semaphores, redo log)
        # Still needs text output for parsing InnoDB monitor sections
        innodb_health = analyze_innodb_health(output)
        innodb_health_file = output_dir / "innodb_health.json"
        with open(innodb_health_file, "w") as f:
            json.dump(innodb_health, f, indent=2)
        
        # Log any health issues found
        if innodb_health.get("summary", {}).get("has_issues"):
            for issue in innodb_health["summary"]["issues"]:
                if issue["severity"] == "critical":
                    logger.warning(f"[{job_id[:8]}] {host.label} ISSUE: {issue['message']}")
                else:
                    logger.info(f"[{job_id[:8]}] {host.label} issue: {issue['message']}")
        else:
            logger.debug(f"[{job_id[:8]}] {host.label} InnoDB health: No issues detected")
        
        # Log parsing time
        parse_elapsed = (datetime.now() - parse_start).total_seconds()
        logger.info(f"[{job_id[:8]}] Parsing completed for {host.label} in {parse_elapsed:.1f}s")
        
        # Collect Hot Tables (optional - queries performance_schema)
        if collect_hot_tables:
            _update_progress(progress_file, {"phase": "hot_tables", "message": "Querying performance_schema..."})
            logger.info(f"[{job_id[:8]}] Starting hot tables query for {host.label}...")
            ht_start = datetime.now()
            hot_tables = _collect_hot_tables(host, job_id)
            ht_elapsed = (datetime.now() - ht_start).total_seconds()
            hot_tables_file = output_dir / "hot_tables.json"
            with open(hot_tables_file, "w") as f:
                json.dump(hot_tables, f, indent=2)
            if hot_tables.get("tables"):
                logger.info(f"[{job_id[:8]}] Hot tables for {host.label}: {len(hot_tables['tables'])} tables ({ht_elapsed:.1f}s)")
            elif hot_tables.get("error"):
                logger.warning(f"[{job_id[:8]}] Hot tables for {host.label}: {hot_tables['error']} ({ht_elapsed:.1f}s)")
            else:
                logger.debug(f"[{job_id[:8]}] No hot tables data for {host.label} ({ht_elapsed:.1f}s)")
        
        # Update status to completed
        total_elapsed = (datetime.now() - total_start_time).total_seconds()
        _update_progress(progress_file, {"phase": "completed", "total_elapsed": round(total_elapsed, 1)})
        _update_host_status(job_id, host_id, HostJobStatus.completed)
        logger.info(f"[{job_id[:8]}] Collection COMPLETED for {host.label} in {total_elapsed:.1f}s (commands: {commands_elapsed:.1f}s)")
        return True
        
    except Exception as e:
        logger.exception(f"[{job_id[:8]}] Parse error for {host.label}: {e}")
        _update_progress(progress_file, {"phase": "failed", "error": str(e)[:200]})
        _update_host_status(job_id, host_id, HostJobStatus.failed, str(e))
        return False


def _collect_hot_tables(host: HostConfig, job_id: str) -> Dict[str, Any]:
    """
    Query performance_schema for hot tables (most active by I/O operations).
    
    Args:
        host: Host configuration
        job_id: Job ID for logging
        
    Returns:
        Dictionary with tables list or empty if unavailable
    """
    result = {
        "tables": [],
        "error": None,
        "collected_at": _timestamp()
    }
    
    # Query to get top 10 tables by total I/O operations
    query = """
        SELECT 
            OBJECT_SCHEMA AS `schema`,
            OBJECT_NAME AS `table`,
            SUM(COUNT_READ) AS read_ops,
            SUM(COUNT_WRITE) AS write_ops,
            SUM(COUNT_STAR) AS total_ops
        FROM performance_schema.table_io_waits_summary_by_index_usage
        WHERE OBJECT_SCHEMA NOT IN ('mysql', 'performance_schema', 'information_schema', 'sys')
          AND OBJECT_NAME IS NOT NULL
        GROUP BY OBJECT_SCHEMA, OBJECT_NAME
        ORDER BY total_ops DESC
        LIMIT 10
    """
    
    try:
        start_time = time.time()
        conn = _create_mysql_connection(host, timeout=15)
        
        try:
            with conn.cursor() as cursor:
                cursor.execute(query)
                rows = cursor.fetchall()
                
                # Convert rows directly to result format
                for row in rows:
                    result["tables"].append({
                        "schema": row.get("schema", ""),
                        "table": row.get("table", ""),
                        "read_ops": int(row.get("read_ops", 0)) if row.get("read_ops") else 0,
                        "write_ops": int(row.get("write_ops", 0)) if row.get("write_ops") else 0,
                        "total_ops": int(row.get("total_ops", 0)) if row.get("total_ops") else 0
                    })
                
                duration = time.time() - start_time
                logger.debug(f"[{job_id[:8]}] Hot tables query completed in {duration:.2f}s, found {len(result['tables'])} tables")
        finally:
            conn.close()
            
    except OperationalError as e:
        error_msg = str(e)
        # Don't fail the job, just log and return empty
        if "performance_schema" in error_msg.lower() or "doesn't exist" in error_msg.lower() or "Unknown table" in error_msg:
            logger.debug(f"[{job_id[:8]}] performance_schema not available: {error_msg[:100]}")
            result["error"] = "performance_schema not available"
        else:
            logger.warning(f"[{job_id[:8]}] Hot tables query failed: {error_msg[:100]}")
            result["error"] = error_msg[:200]
    except Exception as e:
        error_msg = str(e)
        logger.warning(f"[{job_id[:8]}] Hot tables query error: {error_msg}")
        result["error"] = error_msg[:200]
    
    return result


def _update_host_status(
    job_id: str,
    host_id: str,
    status: HostJobStatus,
    error_message: Optional[str] = None,
    mysql_version: Optional[str] = None
) -> None:
    """Update the status of a job host in the database."""
    with get_db_context() as db:
        job_host = db.query(JobHost).filter(
            JobHost.job_id == job_id,
            JobHost.host_id == host_id
        ).first()
        
        if job_host:
            job_host.status = status
            if status == HostJobStatus.running:
                job_host.started_at = datetime.utcnow()
                if mysql_version:
                    job_host.mysql_version = mysql_version
            elif status in (HostJobStatus.completed, HostJobStatus.failed):
                job_host.completed_at = datetime.utcnow()
            if error_message:
                job_host.error_message = error_message


def run_collection_job(job_id: str, host_ids: list[str], collect_hot_tables: bool = False) -> None:
    """
    Run collection job for multiple hosts (background task).
    
    Args:
        job_id: Job identifier
        host_ids: List of host IDs to collect from
        collect_hot_tables: Whether to query performance_schema for hot tables
    """
    logger.info(f"[{job_id[:8]}] Job STARTED - collecting from {len(host_ids)} host(s)")
    
    # Log host details
    for host_id in host_ids:
        host = get_host_by_id(host_id)
        if host:
            logger.info(f"[{job_id[:8]}] Target host: {host.label} -> {host.host}:{host.port} (user: {host.user})")
    
    # Calculate expected DB connections (3 commands per host)
    expected_connections = len(host_ids) * len(COMMANDS)
    logger.info(f"[{job_id[:8]}] Expected DB connections: {expected_connections} ({len(COMMANDS)} commands × {len(host_ids)} hosts)")
    
    job_start = datetime.now()
    
    # Update job status to running
    with get_db_context() as db:
        job = db.query(Job).filter(Job.id == job_id).first()
        if job:
            job.status = JobStatus.running
    
    # Collect from all hosts IN PARALLEL
    success_count = 0
    logger.info(f"[{job_id[:8]}] Starting PARALLEL collection for {len(host_ids)} hosts")
    
    # Use ThreadPoolExecutor to run host collections in parallel
    with ThreadPoolExecutor(max_workers=min(len(host_ids), 10)) as executor:
        # Submit all host collection tasks
        future_to_host = {
            executor.submit(collect_host_data, job_id, host_id, collect_hot_tables): host_id
            for host_id in host_ids
        }
        
        # Wait for all to complete and count successes
        for future in as_completed(future_to_host):
            host_id = future_to_host[future]
            host = get_host_by_id(host_id)
            host_info = f"{host.label}" if host else host_id
            try:
                success = future.result()
                if success:
                    success_count += 1
                    logger.info(f"[{job_id[:8]}] ✓ {host_info} completed successfully")
                else:
                    logger.warning(f"[{job_id[:8]}] ✗ {host_info} failed")
            except Exception as e:
                logger.exception(f"[{job_id[:8]}] ✗ {host_info} raised exception: {e}")
    
    # Update job status based on results
    job_elapsed = (datetime.now() - job_start).total_seconds()
    failed_count = len(host_ids) - success_count
    
    # Calculate actual connections made
    successful_connections = success_count * len(COMMANDS)
    failed_connections = failed_count * len(COMMANDS)
    
    with get_db_context() as db:
        job = db.query(Job).filter(Job.id == job_id).first()
        if job:
            if failed_count == len(host_ids):
                job.status = JobStatus.failed
                logger.error(f"[{job_id[:8]}] Job FAILED - all {len(host_ids)} hosts failed in {job_elapsed:.1f}s")
                logger.error(f"[{job_id[:8]}] DB Connection Summary: {failed_connections} connections failed")
            elif failed_count > 0:
                job.status = JobStatus.completed  # Partial success
                logger.warning(f"[{job_id[:8]}] Job COMPLETED (partial) - {success_count}/{len(host_ids)} succeeded in {job_elapsed:.1f}s")
                logger.info(f"[{job_id[:8]}] DB Connection Summary: {successful_connections} successful, {failed_connections} failed")
            else:
                job.status = JobStatus.completed
                logger.info(f"[{job_id[:8]}] Job COMPLETED - all {len(host_ids)} hosts succeeded in {job_elapsed:.1f}s")
                logger.info(f"[{job_id[:8]}] DB Connection Summary: {successful_connections} connections completed successfully")


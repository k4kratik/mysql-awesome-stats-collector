"""MySQL diagnostic data collector using CLI."""

import subprocess
import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, Tuple
import time

from .utils import (
    HostConfig,
    get_host_by_id,
    ensure_output_dir,
    get_job_dir,
)
from .parser import parse_innodb_status, parse_global_status, parse_processlist, parse_config_variables, parse_replica_status, parse_master_status, CONFIG_VARIABLES_ALLOWLIST
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


def _run_single_command(
    host: HostConfig, 
    command: str, 
    env: dict
) -> Tuple[str, bool, str, float]:
    """
    Run a single MySQL command.
    
    Returns:
        Tuple of (command, success, output/error, duration_seconds)
    """
    host_label = f"{host.label} ({host.host}:{host.port})"
    cmd = [
        "mysql",
        f"-h{host.host}",
        f"-P{host.port}",
        f"-u{host.user}",
        "-e", command,
    ]
    
    start_time = time.time()
    cmd_start = _timestamp()
    
    try:
        # Use Popen to get PID
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=env
        )
        
        logger.info(f"[DB CONNECT] PID {process.pid} - {host_label} | {command}")
        
        try:
            stdout, stderr = process.communicate(timeout=120)
            returncode = process.returncode
        except subprocess.TimeoutExpired:
            logger.warning(f"[DB DISCONNECT] PID {process.pid} - TIMEOUT | {host_label}")
            process.kill()
            process.communicate()
            duration = time.time() - start_time
            return (command, False, f"Command timed out after 120s", duration)
        
        duration = time.time() - start_time
        cmd_end = _timestamp()
        
        if returncode == 0:
            logger.info(f"[DB DISCONNECT] PID {process.pid} - OK ({duration:.2f}s) | {host_label} | {command}")
        else:
            logger.warning(f"[DB DISCONNECT] PID {process.pid} - ERROR (exit {returncode}) | {host_label}")
        
        if returncode != 0:
            error_msg = stderr or "Unknown error"
            if "Using a password" not in error_msg or "ERROR" in error_msg:
                return (command, False, error_msg, duration)
        
        # Format output with headers
        output = f"\n{'='*60}\n"
        output += f"-- {command}\n"
        output += f"-- Time: {cmd_start} -> {cmd_end} ({duration:.2f}s)\n"
        output += f"{'='*60}\n"
        output += stdout
        
        return (command, True, output, duration)
        
    except FileNotFoundError:
        duration = time.time() - start_time
        return (command, False, "mysql CLI not found", duration)
    except Exception as e:
        duration = time.time() - start_time
        return (command, False, str(e), duration)


def run_mysql_commands_parallel(host: HostConfig) -> Tuple[bool, str, Dict[str, Any]]:
    """
    Run MySQL diagnostic commands in PARALLEL via CLI.
    
    Args:
        host: Host configuration
    
    Returns:
        Tuple of (success, combined_output, timing_metrics)
    """
    env = os.environ.copy()
    env["MYSQL_PWD"] = host.password
    
    host_label = f"{host.label} ({host.host}:{host.port})"
    collection_start = _timestamp()
    overall_start = time.time()
    
    logger.info(f"[PARALLEL] Starting {len(COMMANDS)} commands in parallel for {host_label}")
    
    # Run all commands in parallel
    results = {}
    timing = {
        "started_at": collection_start,
        "commands": {}
    }
    
    with ThreadPoolExecutor(max_workers=len(COMMANDS)) as executor:
        # Submit all commands
        futures = {
            executor.submit(_run_single_command, host, cmd, env): cmd 
            for cmd in COMMANDS
        }
        
        # Collect results as they complete
        for future in as_completed(futures):
            command = futures[future]
            try:
                cmd_name, success, output, duration = future.result()
                results[cmd_name] = (success, output)
                timing["commands"][cmd_name] = {
                    "duration": round(duration, 3),
                    "success": success
                }
            except Exception as e:
                logger.exception(f"[PARALLEL] Exception for {command}: {e}")
                results[command] = (False, str(e))
                timing["commands"][command] = {
                    "duration": 0,
                    "success": False,
                    "error": str(e)
                }
    
    overall_duration = time.time() - overall_start
    timing["completed_at"] = _timestamp()
    timing["total_duration"] = round(overall_duration, 3)
    
    # Check if any command failed
    all_success = all(success for success, _ in results.values())
    
    # Build combined output in command order
    all_output = []
    all_output.append(f"{'#'*60}")
    all_output.append(f"# MySQL Diagnostic Collection (PARALLEL)")
    all_output.append(f"# Host: {host.host}:{host.port}")
    all_output.append(f"# User: {host.user}")
    all_output.append(f"# Started: {collection_start}")
    all_output.append(f"# Mode: Parallel ({len(COMMANDS)} concurrent connections)")
    all_output.append(f"{'#'*60}")
    
    for command in COMMANDS:
        if command in results:
            success, output = results[command]
            if success:
                all_output.append(output)
            else:
                all_output.append(f"\n{'='*60}")
                all_output.append(f"-- {command}")
                all_output.append(f"-- ERROR: {output}")
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
    
    return all_success, "\n".join(all_output), timing


# Keep the old sequential function for fallback
def run_mysql_command(host: HostConfig) -> tuple[bool, str]:
    """
    Run MySQL diagnostic commands via CLI (SEQUENTIAL - legacy).
    
    Args:
        host: Host configuration
    
    Returns:
        Tuple of (success, output/error)
    """
    env = os.environ.copy()
    env["MYSQL_PWD"] = host.password
    
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
        cmd_name, success, output, duration = _run_single_command(host, command, env)
        if not success:
            return False, f"[{_timestamp()}] MySQL error: {output}"
        all_output.append(output)
    
    all_output.append(f"\n{'#'*60}")
    all_output.append(f"# Collection completed: {_timestamp()}")
    all_output.append(f"{'#'*60}")
    
    return True, "\n".join(all_output)


def collect_host_data(job_id: str, host_id: str) -> bool:
    """
    Collect diagnostic data from a single host using PARALLEL command execution.
    
    Args:
        job_id: Job identifier
        host_id: Host identifier
    
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
    
    # Update status to running
    _update_host_status(job_id, host_id, HostJobStatus.running)
    
    # Run MySQL commands in PARALLEL
    start_time = datetime.now()
    success, output, timing = run_mysql_commands_parallel(host)
    elapsed = (datetime.now() - start_time).total_seconds()
    
    # Ensure output directory exists
    output_dir = ensure_output_dir(job_id, host_id)
    
    # Always save raw output
    raw_file = output_dir / "raw.txt"
    with open(raw_file, "w") as f:
        f.write(output)
    
    # Save timing metrics
    timing_file = output_dir / "timing.json"
    with open(timing_file, "w") as f:
        json.dump(timing, f, indent=2)
    
    if not success:
        logger.error(f"[{job_id[:8]}] Collection FAILED for {host.label} after {elapsed:.1f}s: {output[:100]}")
        _update_host_status(job_id, host_id, HostJobStatus.failed, output)
        return False
    
    try:
        # Parse and save InnoDB status
        innodb_content = parse_innodb_status(output)
        innodb_file = output_dir / "innodb.txt"
        with open(innodb_file, "w") as f:
            f.write(innodb_content)
        
        # Parse and save Global Status
        global_status = parse_global_status(output)
        global_status_file = output_dir / "global_status.json"
        with open(global_status_file, "w") as f:
            json.dump(global_status, f, indent=2)
        logger.debug(f"[{job_id[:8]}] Parsed {len(global_status)} global status variables")
        
        # Parse and save Processlist
        processlist = parse_processlist(output)
        processlist_file = output_dir / "processlist.json"
        with open(processlist_file, "w") as f:
            json.dump(processlist, f, indent=2)
        logger.debug(f"[{job_id[:8]}] Parsed {len(processlist)} processes")
        
        # Parse and save Config Variables (all variables, filtering done in UI)
        config_vars_all = parse_config_variables(output, filter_allowlist=False)
        config_vars_file = output_dir / "config_vars.json"
        with open(config_vars_file, "w") as f:
            json.dump(config_vars_all, f, indent=2)
        logger.debug(f"[{job_id[:8]}] Parsed {len(config_vars_all)} config variables")
        
        # Parse and save Replica Status (may be empty for primary/non-replicas)
        replica_status = parse_replica_status(output)
        replica_status_file = output_dir / "replica_status.json"
        with open(replica_status_file, "w") as f:
            json.dump(replica_status, f, indent=2)
        if replica_status.get("is_replica"):
            lag = replica_status.get("seconds_behind_master")
            lag_str = f"{lag}s" if lag is not None else "NULL"
            logger.info(f"[{job_id[:8]}] Replica lag for {host.label}: {lag_str}")
        else:
            logger.warning(f"[{job_id[:8]}] {host.label} is not a replica (or status unavailable) - parsed result: {replica_status}")
        
        # Parse and save Master Status (for primary servers with binlog enabled)
        master_status = parse_master_status(output)
        master_status_file = output_dir / "master_status.json"
        with open(master_status_file, "w") as f:
            json.dump(master_status, f, indent=2)
        if master_status.get("is_master"):
            logger.info(f"[{job_id[:8]}] Master binlog for {host.label}: {master_status.get('file')}:{master_status.get('position')}")
        else:
            logger.debug(f"[{job_id[:8]}] {host.label} has no master status (binlog disabled or not primary)")
        
        # Update status to completed
        _update_host_status(job_id, host_id, HostJobStatus.completed)
        logger.info(f"[{job_id[:8]}] Collection COMPLETED for {host.label} in {elapsed:.1f}s")
        return True
        
    except Exception as e:
        logger.exception(f"[{job_id[:8]}] Parse error for {host.label}: {e}")
        _update_host_status(job_id, host_id, HostJobStatus.failed, str(e))
        return False


def _update_host_status(
    job_id: str,
    host_id: str,
    status: HostJobStatus,
    error_message: Optional[str] = None
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
            elif status in (HostJobStatus.completed, HostJobStatus.failed):
                job_host.completed_at = datetime.utcnow()
            if error_message:
                job_host.error_message = error_message


def run_collection_job(job_id: str, host_ids: list[str]) -> None:
    """
    Run collection job for multiple hosts (background task).
    
    Args:
        job_id: Job identifier
        host_ids: List of host IDs to collect from
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
    
    # Collect from each host
    success_count = 0
    for i, host_id in enumerate(host_ids, 1):
        host = get_host_by_id(host_id)
        host_info = f"{host.label} @ {host.host}:{host.port}" if host else host_id
        logger.info(f"[{job_id[:8]}] Processing host {i}/{len(host_ids)}: {host_info}")
        success = collect_host_data(job_id, host_id)
        if success:
            success_count += 1
    
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


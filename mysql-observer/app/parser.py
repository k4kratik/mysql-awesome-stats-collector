"""Parser for MySQL diagnostic output."""

import re
import json
from typing import Dict, List, Any, Optional


def parse_innodb_status(raw_output: str) -> str:
    """
    Extract InnoDB status section from raw output.
    Returns formatted, readable sections.
    """
    # Look for the INNODB STATUS section marker
    innodb_section = ""
    
    # Find section between the command header and the next section or end
    section_pattern = r"-- SHOW ENGINE INNODB STATUS.*?={60}\n(.*?)(?=\n={60}|$)"
    match = re.search(section_pattern, raw_output, re.DOTALL)
    
    if match:
        innodb_section = match.group(1).strip()
    
    # If not found with header, try to find InnoDB monitor output directly
    if not innodb_section:
        # Look for the InnoDB status output (starts after Type/Name/Status headers)
        pattern = r"Type\tName\tStatus\n\w+\t\w*\t(.*?)(?=\n={60}|$)"
        match = re.search(pattern, raw_output, re.DOTALL)
        if match:
            innodb_section = match.group(1).strip()
    
    # If still not found, look for InnoDB monitor markers
    if not innodb_section:
        pattern = r"=====================================\n.*?INNODB MONITOR OUTPUT.*?END OF INNODB MONITOR OUTPUT"
        match = re.search(pattern, raw_output, re.DOTALL)
        if match:
            innodb_section = match.group(0)
    
    # Handle literal \n in the output (MySQL tabular format stores newlines as literal \n)
    if innodb_section and '\\n' in innodb_section:
        innodb_section = innodb_section.replace('\\n', '\n')
    
    if innodb_section:
        return _format_innodb_sections(innodb_section)
    
    # Return raw output if we can't parse it
    return raw_output if raw_output else "InnoDB status not found in output."


def parse_innodb_status_structured(raw_output: str) -> Dict[str, Any]:
    """
    Parse InnoDB status into structured data for UI display.
    Returns a dictionary with parsed sections and key metrics.
    """
    result = {
        "header": {},
        "background_thread": {},
        "semaphores": {},
        "transactions": {},
        "file_io": {},
        "insert_buffer": {},
        "log": {},
        "buffer_pool": {},
        "row_operations": {},
        "raw_sections": {},
    }
    
    # Extract InnoDB section
    innodb_text = ""
    section_pattern = r"-- SHOW ENGINE INNODB STATUS.*?={60}\n(.*?)(?=\n={60}|$)"
    match = re.search(section_pattern, raw_output, re.DOTALL)
    if match:
        innodb_text = match.group(1).strip()
    
    if not innodb_text:
        pattern = r"Type\tName\tStatus\n\w+\t\w*\t(.*?)(?=\n={60}|$)"
        match = re.search(pattern, raw_output, re.DOTALL)
        if match:
            innodb_text = match.group(1).strip()
    
    if not innodb_text:
        pattern = r"=====================================\n.*?INNODB MONITOR OUTPUT.*?END OF INNODB MONITOR OUTPUT"
        match = re.search(pattern, raw_output, re.DOTALL)
        if match:
            innodb_text = match.group(0)
    
    # Handle literal \n in the output (MySQL tabular format stores newlines as literal \n)
    if '\\n' in innodb_text:
        innodb_text = innodb_text.replace('\\n', '\n')
    
    if not innodb_text:
        return result
    
    # Parse header
    header_match = re.search(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}).*?INNODB MONITOR OUTPUT", innodb_text)
    if header_match:
        result["header"]["timestamp"] = header_match.group(1)
    
    avg_match = re.search(r"Per second averages calculated from the last (\d+) seconds", innodb_text)
    if avg_match:
        result["header"]["avg_interval"] = int(avg_match.group(1))
    
    # Parse Background Thread
    bg_section = _extract_section(innodb_text, "BACKGROUND THREAD")
    if bg_section:
        result["raw_sections"]["background_thread"] = bg_section
        master_match = re.search(r"srv_master_thread loops: (\d+) srv_active, (\d+) srv_shutdown, (\d+) srv_idle", bg_section)
        if master_match:
            result["background_thread"] = {
                "srv_active": int(master_match.group(1)),
                "srv_shutdown": int(master_match.group(2)),
                "srv_idle": int(master_match.group(3)),
            }
    
    # Parse Semaphores
    sem_section = _extract_section(innodb_text, "SEMAPHORES")
    if sem_section:
        result["raw_sections"]["semaphores"] = sem_section
        os_wait = re.findall(r"OS WAIT ARRAY INFO: reservation count (\d+)", sem_section)
        signal_match = re.search(r"signal count (\d+)", sem_section)
        result["semaphores"] = {
            "os_waits": [int(w) for w in os_wait] if os_wait else [],
            "signal_count": int(signal_match.group(1)) if signal_match else 0,
        }
        
        rw_shared = re.search(r"RW-shared spins (\d+), rounds (\d+), OS waits (\d+)", sem_section)
        if rw_shared:
            result["semaphores"]["rw_shared"] = {
                "spins": int(rw_shared.group(1)),
                "rounds": int(rw_shared.group(2)),
                "os_waits": int(rw_shared.group(3)),
            }
    
    # Parse Transactions
    trx_section = _extract_section(innodb_text, "TRANSACTIONS")
    if trx_section:
        result["raw_sections"]["transactions"] = trx_section
        trx_id = re.search(r"Trx id counter (\d+)", trx_section)
        purge_match = re.search(r"Purge done for trx's n:o < (\d+)", trx_section)
        history_match = re.search(r"History list length (\d+)", trx_section)
        
        active_trx = re.findall(r"---TRANSACTION (\d+)", trx_section)
        not_started = len(re.findall(r"not started", trx_section))
        
        result["transactions"] = {
            "trx_id_counter": int(trx_id.group(1)) if trx_id else 0,
            "purge_trx_id": int(purge_match.group(1)) if purge_match else 0,
            "history_list_length": int(history_match.group(1)) if history_match else 0,
            "total_transactions": len(active_trx),
            "not_started": not_started,
            "active": len(active_trx) - not_started,
        }
    
    # Parse File I/O
    io_section = _extract_section(innodb_text, "FILE I/O")
    if io_section:
        result["raw_sections"]["file_io"] = io_section
        reads_match = re.search(r"(\d+) OS file reads", io_section)
        writes_match = re.search(r"(\d+) OS file writes", io_section)
        fsyncs_match = re.search(r"(\d+) OS fsyncs", io_section)
        
        reads_s = re.search(r"([\d.]+) reads/s", io_section)
        writes_s = re.search(r"([\d.]+) writes/s", io_section)
        fsyncs_s = re.search(r"([\d.]+) fsyncs/s", io_section)
        
        pending_reads = re.search(r"Pending normal aio reads: \[([\d, ]+)\]", io_section)
        pending_writes = re.search(r"aio writes: \[([\d, ]+)\]", io_section)
        
        io_threads = re.findall(r"I/O thread \d+ state: (.*?) \((.*?)\)", io_section)
        
        result["file_io"] = {
            "os_file_reads": int(reads_match.group(1)) if reads_match else 0,
            "os_file_writes": int(writes_match.group(1)) if writes_match else 0,
            "os_fsyncs": int(fsyncs_match.group(1)) if fsyncs_match else 0,
            "reads_per_sec": float(reads_s.group(1)) if reads_s else 0,
            "writes_per_sec": float(writes_s.group(1)) if writes_s else 0,
            "fsyncs_per_sec": float(fsyncs_s.group(1)) if fsyncs_s else 0,
            "io_threads_count": len(io_threads),
            "read_threads": len([t for t in io_threads if "read" in t[1]]),
            "write_threads": len([t for t in io_threads if "write" in t[1]]),
        }
    
    # Parse Insert Buffer and Adaptive Hash Index
    ibuf_section = _extract_section(innodb_text, "INSERT BUFFER AND ADAPTIVE HASH INDEX")
    if ibuf_section:
        result["raw_sections"]["insert_buffer"] = ibuf_section
        ibuf_match = re.search(r"Ibuf: size (\d+), free list len (\d+), seg size (\d+), (\d+) merges", ibuf_section)
        hash_table = re.findall(r"Hash table size (\d+), node heap has (\d+) buffer", ibuf_section)
        hash_search = re.search(r"([\d.]+) hash searches/s, ([\d.]+) non-hash searches/s", ibuf_section)
        
        result["insert_buffer"] = {
            "ibuf_size": int(ibuf_match.group(1)) if ibuf_match else 0,
            "ibuf_free_list": int(ibuf_match.group(2)) if ibuf_match else 0,
            "ibuf_seg_size": int(ibuf_match.group(3)) if ibuf_match else 0,
            "ibuf_merges": int(ibuf_match.group(4)) if ibuf_match else 0,
            "hash_table_size": int(hash_table[0][0]) if hash_table else 0,
            "hash_table_buffers": sum(int(h[1]) for h in hash_table) if hash_table else 0,
            "hash_searches_per_sec": float(hash_search.group(1)) if hash_search else 0,
            "non_hash_searches_per_sec": float(hash_search.group(2)) if hash_search else 0,
        }
    
    # Parse Log
    log_section = _extract_section(innodb_text, "LOG")
    if log_section:
        result["raw_sections"]["log"] = log_section
        lsn = re.search(r"Log sequence number\s+(\d+)", log_section)
        flushed = re.search(r"Log flushed up to\s+(\d+)", log_section)
        checkpoint = re.search(r"Last checkpoint at\s+(\d+)", log_section)
        log_ios = re.search(r"(\d+) log i/o's done", log_section)
        
        result["log"] = {
            "log_sequence_number": int(lsn.group(1)) if lsn else 0,
            "log_flushed_up_to": int(flushed.group(1)) if flushed else 0,
            "last_checkpoint": int(checkpoint.group(1)) if checkpoint else 0,
            "log_ios_done": int(log_ios.group(1)) if log_ios else 0,
        }
        
        # Calculate checkpoint age
        if result["log"]["log_sequence_number"] and result["log"]["last_checkpoint"]:
            result["log"]["checkpoint_age"] = result["log"]["log_sequence_number"] - result["log"]["last_checkpoint"]
    
    # Parse Buffer Pool and Memory
    bp_section = _extract_section(innodb_text, "BUFFER POOL AND MEMORY")
    if bp_section:
        result["raw_sections"]["buffer_pool"] = bp_section
        pool_size = re.search(r"Buffer pool size\s+(\d+)", bp_section)
        free_buffers = re.search(r"Free buffers\s+(\d+)", bp_section)
        db_pages = re.search(r"Database pages\s+(\d+)", bp_section)
        modified = re.search(r"Modified db pages\s+(\d+)", bp_section)
        pending_reads = re.search(r"Pending reads\s+(\d+)", bp_section)
        hit_rate = re.search(r"Buffer pool hit rate (\d+) / (\d+)", bp_section)
        
        pages_read = re.search(r"Pages read (\d+), created (\d+), written (\d+)", bp_section)
        pages_made_young = re.search(r"Pages made young (\d+), not young (\d+)", bp_section)
        
        result["buffer_pool"] = {
            "pool_size": int(pool_size.group(1)) if pool_size else 0,
            "free_buffers": int(free_buffers.group(1)) if free_buffers else 0,
            "database_pages": int(db_pages.group(1)) if db_pages else 0,
            "modified_pages": int(modified.group(1)) if modified else 0,
            "pending_reads": int(pending_reads.group(1)) if pending_reads else 0,
            "hit_rate_num": int(hit_rate.group(1)) if hit_rate else 0,
            "hit_rate_denom": int(hit_rate.group(2)) if hit_rate else 1000,
            "pages_read": int(pages_read.group(1)) if pages_read else 0,
            "pages_created": int(pages_read.group(2)) if pages_read else 0,
            "pages_written": int(pages_read.group(3)) if pages_read else 0,
            "pages_made_young": int(pages_made_young.group(1)) if pages_made_young else 0,
            "pages_not_made_young": int(pages_made_young.group(2)) if pages_made_young else 0,
        }
        
        # Calculate utilization
        if result["buffer_pool"]["pool_size"]:
            result["buffer_pool"]["utilization_pct"] = round(
                (result["buffer_pool"]["database_pages"] / result["buffer_pool"]["pool_size"]) * 100, 2
            )
            result["buffer_pool"]["dirty_pct"] = round(
                (result["buffer_pool"]["modified_pages"] / result["buffer_pool"]["pool_size"]) * 100, 2
            )
    
    # Parse Row Operations
    row_section = _extract_section(innodb_text, "ROW OPERATIONS")
    if row_section:
        result["raw_sections"]["row_operations"] = row_section
        queries = re.search(r"(\d+) queries inside InnoDB, (\d+) queries in queue", row_section)
        read_views = re.search(r"(\d+) read views open inside InnoDB", row_section)
        
        rows_total = re.search(r"Number of rows inserted (\d+), updated (\d+), deleted (\d+), read (\d+)", row_section)
        rows_per_sec = re.search(r"([\d.]+) inserts/s, ([\d.]+) updates/s, ([\d.]+) deletes/s, ([\d.]+) reads/s", row_section)
        
        result["row_operations"] = {
            "queries_inside": int(queries.group(1)) if queries else 0,
            "queries_in_queue": int(queries.group(2)) if queries else 0,
            "read_views_open": int(read_views.group(1)) if read_views else 0,
            "rows_inserted": int(rows_total.group(1)) if rows_total else 0,
            "rows_updated": int(rows_total.group(2)) if rows_total else 0,
            "rows_deleted": int(rows_total.group(3)) if rows_total else 0,
            "rows_read": int(rows_total.group(4)) if rows_total else 0,
            "inserts_per_sec": float(rows_per_sec.group(1)) if rows_per_sec else 0,
            "updates_per_sec": float(rows_per_sec.group(2)) if rows_per_sec else 0,
            "deletes_per_sec": float(rows_per_sec.group(3)) if rows_per_sec else 0,
            "reads_per_sec": float(rows_per_sec.group(4)) if rows_per_sec else 0,
        }
    
    return result


def _extract_section(innodb_text: str, section_name: str) -> Optional[str]:
    """Extract a specific section from InnoDB status text."""
    pattern = rf"-+\n{section_name}\n-+\n(.*?)(?=-{{5,}}|\Z)"
    match = re.search(pattern, innodb_text, re.DOTALL)
    if match:
        return match.group(1).strip()
    return None


def _format_innodb_sections(innodb_text: str) -> str:
    """Format InnoDB status into readable sections."""
    # Define known section headers
    sections = [
        "BACKGROUND THREAD",
        "SEMAPHORES",
        "LATEST FOREIGN KEY ERROR",
        "LATEST DETECTED DEADLOCK",
        "TRANSACTIONS",
        "FILE I/O",
        "INSERT BUFFER AND ADAPTIVE HASH INDEX",
        "LOG",
        "BUFFER POOL AND MEMORY",
        "INDIVIDUAL BUFFER POOL INFO",
        "ROW OPERATIONS",
    ]
    
    formatted = []
    formatted.append("=" * 60)
    formatted.append("INNODB ENGINE STATUS")
    formatted.append("=" * 60)
    formatted.append("")
    
    # Check if we have section markers in the text
    has_sections = any(section in innodb_text for section in sections)
    
    if has_sections:
        # Extract each section
        for section in sections:
            pattern = rf"-+\n{section}\n-+\n(.*?)(?=-{{5,}}|\Z)"
            match = re.search(pattern, innodb_text, re.DOTALL)
            if match:
                formatted.append(f"### {section}")
                formatted.append("-" * 40)
                content = match.group(1).strip()
                if content:
                    formatted.append(content)
                else:
                    formatted.append("(empty)")
                formatted.append("")
        
        return "\n".join(formatted) if len(formatted) > 4 else innodb_text
    else:
        # Return raw text with header
        formatted.append(innodb_text)
        return "\n".join(formatted)


def parse_global_status(raw_output: str) -> Dict[str, Any]:
    """
    Parse SHOW GLOBAL STATUS output into key-value dictionary.
    Handles tabular format output.
    """
    result = {}
    
    # Find the GLOBAL STATUS section
    section_pattern = r"-- SHOW GLOBAL STATUS.*?={60}\n(.*?)(?=\n={60}|$)"
    match = re.search(section_pattern, raw_output, re.DOTALL)
    
    section_text = match.group(1) if match else raw_output
    
    # Parse tabular format (Variable_name\tValue)
    lines = section_text.strip().split("\n")
    
    for line in lines:
        # Skip header line
        if line.startswith("Variable_name") or not line.strip():
            continue
        
        # Split by tab
        parts = line.split("\t")
        if len(parts) >= 2:
            name = parts[0].strip()
            value = parts[1].strip()
            
            # Skip invalid entries
            if not name or name.startswith("+") or name.startswith("|") or name.startswith("="):
                continue
            
            # Try to convert to number
            try:
                if "." in value:
                    result[name] = float(value)
                else:
                    result[name] = int(value)
            except ValueError:
                result[name] = value
    
    return result


def parse_processlist(raw_output: str) -> List[Dict[str, Any]]:
    """
    Parse SHOW FULL PROCESSLIST output into list of dictionaries.
    Handles tabular format output.
    """
    processes = []
    
    # Find the PROCESSLIST section
    section_pattern = r"-- SHOW FULL PROCESSLIST.*?={60}\n(.*?)(?=\n={60}|$)"
    match = re.search(section_pattern, raw_output, re.DOTALL)
    
    section_text = match.group(1) if match else raw_output
    
    lines = section_text.strip().split("\n")
    
    if not lines:
        return processes
    
    # First line should be headers
    headers = []
    header_line = lines[0] if lines else ""
    
    if "\t" in header_line:
        headers = [h.strip().lower() for h in header_line.split("\t")]
    else:
        # Default headers for processlist
        headers = ["id", "user", "host", "db", "command", "time", "state", "info"]
    
    # Parse data rows
    for line in lines[1:]:
        if not line.strip():
            continue
        
        parts = line.split("\t")
        if len(parts) < 2:
            continue
        
        process = {}
        for i, header in enumerate(headers):
            if i < len(parts):
                value = parts[i].strip()
                
                # Handle NULL values
                if value == "NULL" or value == "\\N":
                    value = None
                
                # Map to standard field names
                field_name = header.lower()
                if field_name in ["id", "user", "host", "db", "command", "time", "state", "info"]:
                    # Convert numeric fields
                    if field_name == "time" and value is not None:
                        try:
                            process[field_name] = int(value)
                        except ValueError:
                            process[field_name] = 0
                    elif field_name == "id" and value is not None:
                        try:
                            process[field_name] = int(value)
                        except ValueError:
                            process[field_name] = value
                    else:
                        process[field_name] = value
        
        if process:
            processes.append(process)
    
    return processes


def extract_section(raw_output: str, command: str) -> str:
    """Extract output for a specific command from the raw output."""
    # Pattern to find command output with our header format
    pattern = rf"-- {re.escape(command)}.*?={60}\n(.*?)(?=\n={60}|$)"
    match = re.search(pattern, raw_output, re.DOTALL | re.IGNORECASE)
    
    if match:
        return match.group(1).strip()
    
    return raw_output


def filter_processlist(
    processes: List[Dict[str, Any]],
    user: Optional[str] = None,
    state: Optional[str] = None,
    min_time: Optional[int] = None,
    query: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Filter processlist by criteria.
    
    Args:
        processes: List of process dictionaries
        user: Filter by user name (case-insensitive substring)
        state: Filter by state (case-insensitive substring)
        min_time: Filter by minimum time in seconds
        query: Filter by query/info content (case-insensitive substring)
    
    Returns:
        Filtered list of processes
    """
    result = processes
    
    if user:
        result = [p for p in result if p.get("user") and user.lower() in p["user"].lower()]
    
    if state:
        result = [p for p in result if p.get("state") and state.lower() in p["state"].lower()]
    
    if min_time is not None:
        result = [p for p in result if p.get("time", 0) >= min_time]
    
    if query:
        result = [p for p in result if p.get("info") and query.lower() in p["info"].lower()]
    
    return result


def get_key_metrics(global_status: Dict[str, Any]) -> Dict[str, Any]:
    """Extract key metrics for charting from global status."""
    metrics = {}
    
    # Connection metrics
    metrics["connections"] = {
        "current": global_status.get("Threads_connected", 0),
        "running": global_status.get("Threads_running", 0),
        "created": global_status.get("Threads_created", 0),
        "cached": global_status.get("Threads_cached", 0),
    }
    
    # Query metrics
    metrics["queries"] = {
        "questions": global_status.get("Questions", 0),
        "slow_queries": global_status.get("Slow_queries", 0),
        "select": global_status.get("Com_select", 0),
        "insert": global_status.get("Com_insert", 0),
        "update": global_status.get("Com_update", 0),
        "delete": global_status.get("Com_delete", 0),
    }
    
    # InnoDB metrics
    metrics["innodb"] = {
        "buffer_pool_reads": global_status.get("Innodb_buffer_pool_reads", 0),
        "buffer_pool_read_requests": global_status.get("Innodb_buffer_pool_read_requests", 0),
        "row_lock_waits": global_status.get("Innodb_row_lock_waits", 0),
        "rows_read": global_status.get("Innodb_rows_read", 0),
        "rows_inserted": global_status.get("Innodb_rows_inserted", 0),
        "rows_updated": global_status.get("Innodb_rows_updated", 0),
        "rows_deleted": global_status.get("Innodb_rows_deleted", 0),
    }
    
    # Bytes metrics
    metrics["bytes"] = {
        "received": global_status.get("Bytes_received", 0),
        "sent": global_status.get("Bytes_sent", 0),
    }
    
    # Table metrics
    metrics["tables"] = {
        "open_tables": global_status.get("Open_tables", 0),
        "opened_tables": global_status.get("Opened_tables", 0),
        "table_locks_waited": global_status.get("Table_locks_waited", 0),
    }
    
    return metrics


# Allowlist of important config variables to display
CONFIG_VARIABLES_ALLOWLIST = [
    # Memory & Buffer Pool
    "innodb_buffer_pool_size",
    "innodb_buffer_pool_instances",
    "innodb_log_buffer_size",
    "tmp_table_size",
    "max_heap_table_size",
    # Connections & Threading
    "max_connections",
    "thread_cache_size",
    "wait_timeout",
    "interactive_timeout",
    "max_prepared_stmt_count",
    # Table & Metadata Cache
    "table_open_cache",
    "table_definition_cache",
    "open_files_limit",
    # InnoDB Redo Log
    "innodb_log_file_size",
    "innodb_log_files_in_group",
    "innodb_flush_log_at_trx_commit",
    # InnoDB I/O
    "innodb_io_capacity",
    "innodb_io_capacity_max",
    "innodb_read_io_threads",
    "innodb_write_io_threads",
    "innodb_sync_array_size",
    "innodb_change_buffering",
    # Replication
    "sync_binlog",
    "binlog_format",
    "binlog_group_commit_sync_delay",
    "slave_parallel_workers",
    "slave_preserve_commit_order",
    # Read-Only Mode
    "read_only",
    "super_read_only",
    # Transaction
    "transaction_isolation",
]


def parse_config_variables(raw_output: str, filter_allowlist: bool = True) -> Dict[str, Any]:
    """
    Parse SHOW GLOBAL VARIABLES output.
    
    Args:
        raw_output: Raw MySQL output containing SHOW GLOBAL VARIABLES
        filter_allowlist: If True, only return allowlisted variables
    
    Returns:
        Dictionary of variable_name -> value
    """
    result = {}
    
    # Find the GLOBAL VARIABLES section
    section_pattern = r"-- SHOW GLOBAL VARIABLES.*?={60}\n(.*?)(?=\n={60}|$)"
    match = re.search(section_pattern, raw_output, re.DOTALL)
    
    section_text = match.group(1) if match else raw_output
    
    # Parse tabular format (Variable_name\tValue)
    lines = section_text.strip().split("\n")
    
    for line in lines:
        # Skip header line
        if line.startswith("Variable_name") or not line.strip():
            continue
        
        # Split by tab
        parts = line.split("\t")
        if len(parts) >= 2:
            name = parts[0].strip().lower()
            value = parts[1].strip()
            
            # Filter to allowlist if requested
            if filter_allowlist:
                if name in CONFIG_VARIABLES_ALLOWLIST:
                    result[name] = value
            else:
                result[name] = value
    
    return result


def parse_all_config_variables(raw_output: str) -> Dict[str, Any]:
    """
    Parse all SHOW GLOBAL VARIABLES output (no filtering).
    
    Args:
        raw_output: Raw MySQL output containing SHOW GLOBAL VARIABLES
    
    Returns:
        Dictionary of all variable_name -> value pairs
    """
    return parse_config_variables(raw_output, filter_allowlist=False)


def evaluate_config_health(
    config_vars: Dict[str, Any],
    global_status: Dict[str, Any],
    system_info: Optional[Dict[str, Any]] = None
) -> Dict[str, Dict[str, str]]:
    """
    Evaluate health indicators for config variables.
    
    Args:
        config_vars: Configuration variables from SHOW GLOBAL VARIABLES
        global_status: Status counters from SHOW GLOBAL STATUS
        system_info: Optional system info (e.g., total RAM)
    
    Returns:
        Dictionary of variable_name -> {value, health, reason}
        health is one of: 'healthy', 'warning', 'critical'
    """
    result = {}
    system_info = system_info or {}
    
    def get_int(d: dict, key: str, default: int = 0) -> int:
        """Safely get integer value from dict."""
        try:
            return int(d.get(key, default))
        except (ValueError, TypeError):
            return default
    
    # Helper to add health entry
    def add_health(var: str, health: str, reason: str):
        if var in config_vars:
            result[var] = {
                "value": config_vars[var],
                "health": health,
                "reason": reason
            }
    
    # ===== Memory & Core Limits =====
    
    # innodb_buffer_pool_size - compared to RAM
    if "innodb_buffer_pool_size" in config_vars:
        pool_size = get_int(config_vars, "innodb_buffer_pool_size")
        total_ram = get_int(system_info, "total_ram", 0)
        
        if total_ram > 0:
            pct = (pool_size / total_ram) * 100
            if pct > 60:
                add_health("innodb_buffer_pool_size", "healthy", f"{pct:.0f}% of RAM")
            elif pct >= 30:
                add_health("innodb_buffer_pool_size", "warning", f"{pct:.0f}% of RAM (30-60%)")
            else:
                add_health("innodb_buffer_pool_size", "critical", f"Only {pct:.0f}% of RAM")
        else:
            # No RAM info available, just show the value without indicator
            result["innodb_buffer_pool_size"] = {
                "value": config_vars["innodb_buffer_pool_size"],
                "health": None,
                "reason": "System RAM unknown"
            }
    
    # max_connections - compared to current usage
    if "max_connections" in config_vars:
        max_conn = get_int(config_vars, "max_connections")
        current_conn = get_int(global_status, "Threads_connected", 0)
        
        if max_conn > 0:
            usage_pct = (current_conn / max_conn) * 100
            if usage_pct > 95:
                add_health("max_connections", "critical", f"{usage_pct:.0f}% used ({current_conn}/{max_conn})")
            elif usage_pct >= 80:
                add_health("max_connections", "warning", f"{usage_pct:.0f}% used ({current_conn}/{max_conn})")
            else:
                add_health("max_connections", "healthy", f"{usage_pct:.0f}% used ({current_conn}/{max_conn})")
    
    # tmp_table_size
    if "tmp_table_size" in config_vars:
        tmp_size = get_int(config_vars, "tmp_table_size")
        mb_64 = 64 * 1024 * 1024
        mb_16 = 16 * 1024 * 1024
        
        if tmp_size >= mb_64:
            add_health("tmp_table_size", "healthy", "≥ 64MB")
        elif tmp_size >= mb_16:
            add_health("tmp_table_size", "warning", "16-64MB range")
        else:
            add_health("tmp_table_size", "critical", "< 16MB")
    
    # max_heap_table_size - compared to tmp_table_size
    if "max_heap_table_size" in config_vars:
        heap_size = get_int(config_vars, "max_heap_table_size")
        tmp_size = get_int(config_vars, "tmp_table_size", 0)
        
        if tmp_size > 0:
            if heap_size >= tmp_size:
                add_health("max_heap_table_size", "healthy", "≥ tmp_table_size")
            else:
                add_health("max_heap_table_size", "warning", "< tmp_table_size (limits temp tables)")
    
    # ===== Table & Metadata Cache =====
    
    # table_open_cache
    if "table_open_cache" in config_vars:
        cache = get_int(config_vars, "table_open_cache")
        open_tables = get_int(global_status, "Open_tables", 0)
        opened_tables = get_int(global_status, "Opened_tables", 0)
        table_open_cache_overflows = get_int(global_status, "Table_open_cache_overflows", 0)
        
        if table_open_cache_overflows > 0:
            add_health("table_open_cache", "critical", f"{table_open_cache_overflows:,} overflows")
        elif cache >= open_tables:
            add_health("table_open_cache", "healthy", f"Cache ({cache:,}) ≥ Open ({open_tables:,})")
        else:
            add_health("table_open_cache", "warning", f"Cache ({cache:,}) < Open ({open_tables:,})")
    
    # table_definition_cache
    if "table_definition_cache" in config_vars:
        cache = get_int(config_vars, "table_definition_cache")
        open_defs = get_int(global_status, "Open_table_definitions", 0)
        
        if open_defs > 0:
            if cache >= open_defs:
                add_health("table_definition_cache", "healthy", f"Cache ({cache:,}) ≥ Open defs ({open_defs:,})")
            else:
                add_health("table_definition_cache", "warning", f"Cache ({cache:,}) < Open defs ({open_defs:,})")
    
    # open_files_limit
    if "open_files_limit" in config_vars:
        limit = get_int(config_vars, "open_files_limit")
        table_cache = get_int(config_vars, "table_open_cache", 0)
        
        if table_cache > 0:
            if limit >= table_cache * 2:
                add_health("open_files_limit", "healthy", f"≥ 2× table_open_cache")
            else:
                add_health("open_files_limit", "warning", f"< 2× table_open_cache ({table_cache * 2:,})")
    
    # ===== Threading =====
    
    # thread_cache_size
    if "thread_cache_size" in config_vars:
        cache = get_int(config_vars, "thread_cache_size")
        if cache > 0:
            add_health("thread_cache_size", "healthy", "Thread caching enabled")
        else:
            add_health("thread_cache_size", "warning", "Thread caching disabled")
    
    # wait_timeout
    if "wait_timeout" in config_vars:
        timeout = get_int(config_vars, "wait_timeout")
        if timeout >= 300:
            add_health("wait_timeout", "healthy", f"≥ 300s ({timeout}s)")
        elif timeout >= 60:
            add_health("wait_timeout", "warning", f"60-300s range ({timeout}s)")
        else:
            add_health("wait_timeout", "critical", f"< 60s ({timeout}s)")
    
    # ===== Redo / Durability =====
    
    # innodb_log_file_size
    if "innodb_log_file_size" in config_vars:
        size = get_int(config_vars, "innodb_log_file_size")
        mb_512 = 512 * 1024 * 1024
        mb_128 = 128 * 1024 * 1024
        
        if size >= mb_512:
            add_health("innodb_log_file_size", "healthy", "≥ 512MB")
        elif size >= mb_128:
            add_health("innodb_log_file_size", "warning", "128-512MB range")
        else:
            add_health("innodb_log_file_size", "critical", "< 128MB")
    
    # innodb_flush_log_at_trx_commit
    if "innodb_flush_log_at_trx_commit" in config_vars:
        val = get_int(config_vars, "innodb_flush_log_at_trx_commit")
        if val == 1:
            add_health("innodb_flush_log_at_trx_commit", "healthy", "Full ACID compliance")
        elif val == 2:
            add_health("innodb_flush_log_at_trx_commit", "warning", "Flush to OS only (risk on crash)")
        else:
            add_health("innodb_flush_log_at_trx_commit", "critical", "No flush (data loss risk)")
    
    # sync_binlog
    if "sync_binlog" in config_vars:
        val = get_int(config_vars, "sync_binlog")
        if val == 1:
            add_health("sync_binlog", "healthy", "Sync after each transaction")
        elif val == 0:
            add_health("sync_binlog", "warning", "No sync (OS-dependent)")
        else:
            add_health("sync_binlog", "healthy", f"Sync every {val} transactions")
    
    # ===== I/O Threads =====
    
    # innodb_read_io_threads
    if "innodb_read_io_threads" in config_vars:
        val = get_int(config_vars, "innodb_read_io_threads")
        if val >= 4:
            add_health("innodb_read_io_threads", "healthy", f"{val} threads")
        elif val > 0:
            add_health("innodb_read_io_threads", "warning", f"Only {val} thread(s)")
        else:
            add_health("innodb_read_io_threads", "critical", "No read threads")
    
    # innodb_write_io_threads
    if "innodb_write_io_threads" in config_vars:
        val = get_int(config_vars, "innodb_write_io_threads")
        if val >= 4:
            add_health("innodb_write_io_threads", "healthy", f"{val} threads")
        elif val > 0:
            add_health("innodb_write_io_threads", "warning", f"Only {val} thread(s)")
        else:
            add_health("innodb_write_io_threads", "critical", "No write threads")
    
    # Add remaining important vars without health indicators
    for var in CONFIG_VARIABLES_ALLOWLIST:
        if var in config_vars and var not in result:
            result[var] = {
                "value": config_vars[var],
                "health": None,
                "reason": ""
            }
    
    return result

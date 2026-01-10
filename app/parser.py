"""Parser for MySQL diagnostic output."""

import re
import json
from typing import Dict, List, Any, Optional

# Pre-compiled regex patterns for performance (compiled once at module load)
_RE_TIMESTAMP = re.compile(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})")
_RE_TABLE_INDEX = re.compile(r"table `([^`]+)`\.`([^`]+)`")
_RE_INDEX_NAME = re.compile(r"index [`']?(\w+)[`']?", re.IGNORECASE)
_RE_TRX_ID = re.compile(r"TRANSACTION (\d+),\s*ACTIVE (\d+) sec\s*(\w+)?")
_RE_TABLES_LOCKED = re.compile(r"tables in use (\d+),\s*locked (\d+)")
_RE_LOCK_STRUCTS = re.compile(r"(\d+) lock struct\(s\).*?(\d+) row lock\(s\)(?:.*?undo log entries (\d+))?")
_RE_THREAD_INFO = re.compile(r"MySQL thread id (\d+).*?query id (\d+)\s+(\S+)\s+(\S+)")
_RE_HISTORY_LIST = re.compile(r"History list length (\d+)")
_RE_WAIT_SEC = re.compile(r"(\d+) sec")


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
    """
    Extract a specific section from InnoDB status text.
    
    OPTIMIZED: Uses string find() instead of regex with .*? DOTALL.
    """
    # Find section header (format: "---\nSECTION_NAME\n---")
    section_markers = [
        f"\n{section_name}\n---",  # Most common
        f"\n{section_name}\n-",    # Variable dashes
    ]
    
    start_idx = -1
    for marker in section_markers:
        idx = innodb_text.find(marker)
        if idx != -1:
            # Find the actual content start (after the dashes line)
            newline_after = innodb_text.find('\n', idx + len(marker))
            if newline_after != -1:
                start_idx = newline_after + 1
                break
            else:
                start_idx = idx + len(marker)
                break
    
    if start_idx == -1:
        return None
    
    # Find end of section (next section header: line of dashes followed by section name)
    # Look for pattern: "\n---" which starts the next section
    end_idx = len(innodb_text)
    
    # Search for next section marker starting from our content
    search_start = start_idx
    while True:
        dash_idx = innodb_text.find('\n---', search_start)
        if dash_idx == -1:
            break
        # Check if this looks like a section header (followed by newline and text)
        # Skip if it's part of our current section content
        if dash_idx > start_idx + 10:  # Must be at least 10 chars into section
            end_idx = dash_idx
            break
        search_start = dash_idx + 4
    
    section_content = innodb_text[start_idx:end_idx].strip()
    return section_content if section_content else None


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


def parse_global_status(result_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Parse SHOW GLOBAL STATUS output into key-value dictionary.
    
    Args:
        result_rows: List of dicts from PyMySQL (each dict has Variable_name and Value keys)
    
    Returns:
        Dictionary of variable_name -> value
    """
    result = {}
    
    for row in result_rows:
        if isinstance(row, dict):
            var_name = row.get("Variable_name", "")
            value = row.get("Value", "")
            
            if not var_name:
                continue
            
            # Try to convert to number
            try:
                if isinstance(value, (int, float)):
                    result[var_name] = value
                elif "." in str(value):
                    result[var_name] = float(value)
                else:
                    result[var_name] = int(value)
            except (ValueError, TypeError):
                result[var_name] = value
    
    return result


def parse_processlist(result_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Parse SHOW FULL PROCESSLIST output into list of dictionaries.
    
    Args:
        result_rows: List of dicts from PyMySQL (each dict has processlist column names as keys)
    
    Returns:
        List of process dictionaries
    """
    processes = []
    
    for row in result_rows:
        if isinstance(row, dict):
            process = {}
            # Map column names to lowercase field names
            for key, value in row.items():
                field_name = key.lower()
                if field_name in ["id", "user", "host", "db", "command", "time", "state", "info"]:
                    # Handle NULL values
                    if value is None or value == "NULL" or value == "\\N":
                        process[field_name] = None
                    # Convert numeric fields
                    elif field_name == "time":
                        try:
                            process[field_name] = int(value) if value else 0
                        except (ValueError, TypeError):
                            process[field_name] = 0
                    elif field_name == "id":
                        try:
                            process[field_name] = int(value) if value else None
                        except (ValueError, TypeError):
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


def parse_config_variables(result_rows: List[Dict[str, Any]], filter_allowlist: bool = True) -> Dict[str, Any]:
    """
    Parse SHOW GLOBAL VARIABLES output.
    
    Args:
        result_rows: List of dicts from PyMySQL (each dict has Variable_name and Value keys)
        filter_allowlist: If True, only return allowlisted variables
    
    Returns:
        Dictionary of variable_name -> value
    """
    result = {}
    
    for row in result_rows:
        if isinstance(row, dict):
            var_name = row.get("Variable_name", "").lower()
            value = row.get("Value", "")
            
            if not var_name:
                continue
            
            # Filter to allowlist if requested
            if filter_allowlist:
                if var_name in CONFIG_VARIABLES_ALLOWLIST:
                    result[var_name] = value
            else:
                result[var_name] = value
    
    return result


def parse_all_config_variables(result_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Parse all SHOW GLOBAL VARIABLES output (no filtering).
    
    Args:
        result_rows: List of dicts from PyMySQL (each dict has Variable_name and Value keys)
    
    Returns:
        Dictionary of all variable_name -> value pairs
    """
    return parse_config_variables(result_rows, filter_allowlist=False)


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


def parse_replica_status(result_row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Parse SHOW REPLICA STATUS (or SHOW SLAVE STATUS) output.
    
    Args:
        result_row: Dict from PyMySQL with replica status columns
    
    Returns:
        Dictionary with key replication metrics.
        Returns {"is_replica": False} if not a replica or status unavailable.
    """
    result = {
        "is_replica": False,
        "seconds_behind_master": None,
        "slave_io_running": None,
        "slave_sql_running": None,
        "slave_io_state": None,
        "slave_sql_state": None,
        "master_host": None,
        "master_port": None,
        "master_user": None,
        "last_error": None,
        "last_errno": None,
        "last_io_error": None,
        "last_io_errno": None,
        "last_sql_error": None,
        "last_sql_errno": None,
        "relay_log_space": None,
        # Binlog positions
        "master_log_file": None,
        "read_master_log_pos": None,
        "relay_master_log_file": None,
        "exec_master_log_pos": None,
        "relay_log_file": None,
        "relay_log_pos": None,
        # GTID
        "retrieved_gtid_set": None,
        "executed_gtid_set": None,
        "auto_position": None,
        "channel_name": None,
        # Additional useful info
        "until_condition": None,
        "replicate_do_db": None,
        "replicate_ignore_db": None,
        "skip_counter": None,
        "connect_retry": None,
        "master_server_id": None,
        "master_uuid": None,
        "sql_delay": None,
        "sql_remaining_delay": None,
    }
    
    if not result_row:
        return result
    
    # Map column names to result fields
    result["is_replica"] = True
    
    # Seconds behind master - check for key existence explicitly (0 is a valid value!)
    if "Seconds_Behind_Source" in result_row:
        seconds_behind = result_row["Seconds_Behind_Source"]
    elif "Seconds_Behind_Master" in result_row:
        seconds_behind = result_row["Seconds_Behind_Master"]
    else:
        seconds_behind = None
    
    # Convert to int, handling None, empty string, and 0
    if seconds_behind is not None and seconds_behind != "":
        try:
            result["seconds_behind_master"] = int(seconds_behind)
        except (ValueError, TypeError):
            result["seconds_behind_master"] = None
    
    # IO/SQL running status
    io_running = result_row.get("Replica_IO_Running") or result_row.get("Slave_IO_Running")
    sql_running = result_row.get("Replica_SQL_Running") or result_row.get("Slave_SQL_Running")
    result["slave_io_running"] = io_running if io_running else None
    result["slave_sql_running"] = sql_running if sql_running else None
    
    # IO/SQL state
    result["slave_io_state"] = result_row.get("Replica_IO_State") or result_row.get("Slave_IO_State")
    result["slave_sql_state"] = result_row.get("Replica_SQL_Running_State") or result_row.get("Slave_SQL_Running_State")
    
    # Master connection info
    result["master_host"] = result_row.get("Source_Host") or result_row.get("Master_Host")
    master_port = result_row.get("Source_Port") or result_row.get("Master_Port")
    if master_port:
        try:
            result["master_port"] = int(master_port)
        except (ValueError, TypeError):
            pass
    result["master_user"] = result_row.get("Source_User") or result_row.get("Master_User")
    
    # Errors
    result["last_error"] = result_row.get("Last_Error")
    result["last_errno"] = result_row.get("Last_Errno")
    result["last_io_error"] = result_row.get("Last_IO_Error")
    result["last_io_errno"] = result_row.get("Last_IO_Errno")
    result["last_sql_error"] = result_row.get("Last_SQL_Error")
    result["last_sql_errno"] = result_row.get("Last_SQL_Errno")
    
    # Binlog positions
    result["master_log_file"] = result_row.get("Source_Log_File") or result_row.get("Master_Log_File")
    read_pos = result_row.get("Read_Source_Log_Pos") or result_row.get("Read_Master_Log_Pos")
    if read_pos:
        try:
            result["read_master_log_pos"] = int(read_pos)
        except (ValueError, TypeError):
            pass
    result["relay_master_log_file"] = result_row.get("Relay_Source_Log_File") or result_row.get("Relay_Master_Log_File")
    exec_pos = result_row.get("Exec_Source_Log_Pos") or result_row.get("Exec_Master_Log_Pos")
    if exec_pos:
        try:
            result["exec_master_log_pos"] = int(exec_pos)
        except (ValueError, TypeError):
            pass
    result["relay_log_file"] = result_row.get("Relay_Log_File")
    relay_pos = result_row.get("Relay_Log_Pos")
    if relay_pos:
        try:
            result["relay_log_pos"] = int(relay_pos)
        except (ValueError, TypeError):
            pass
    
    # GTID
    result["retrieved_gtid_set"] = result_row.get("Retrieved_Gtid_Set")
    result["executed_gtid_set"] = result_row.get("Executed_Gtid_Set")
    auto_pos = result_row.get("Auto_Position")
    if auto_pos:
        result["auto_position"] = auto_pos == "1" or auto_pos == 1
    
    # Other fields
    result["channel_name"] = result_row.get("Channel_Name")
    result["until_condition"] = result_row.get("Until_Condition")
    result["replicate_do_db"] = result_row.get("Replicate_Do_DB")
    result["replicate_ignore_db"] = result_row.get("Replicate_Ignore_DB")
    # Skip counter - 0 is a valid value
    skip_counter = result_row.get("Skip_Counter")
    if skip_counter is not None and skip_counter != "":
        try:
            result["skip_counter"] = int(skip_counter)
        except (ValueError, TypeError):
            result["skip_counter"] = None
    
    # Connect retry - 0 is not valid, so 'or' is fine
    connect_retry = result_row.get("Connect_Retry")
    if connect_retry:
        try:
            result["connect_retry"] = int(connect_retry)
        except (ValueError, TypeError):
            pass
    
    # Master server ID - check for key existence (0 is not valid, but be explicit)
    if "Source_Server_Id" in result_row:
        master_server_id = result_row["Source_Server_Id"]
    elif "Master_Server_Id" in result_row:
        master_server_id = result_row["Master_Server_Id"]
    else:
        master_server_id = None
    
    if master_server_id:
        try:
            result["master_server_id"] = int(master_server_id)
        except (ValueError, TypeError):
            pass
    
    result["master_uuid"] = result_row.get("Source_UUID") or result_row.get("Master_UUID")
    
    # SQL delay - 0 is a valid value (no delay)
    sql_delay = result_row.get("SQL_Delay")
    if sql_delay is not None and sql_delay != "":
        try:
            result["sql_delay"] = int(sql_delay)
        except (ValueError, TypeError):
            result["sql_delay"] = None
    sql_remaining = result_row.get("SQL_Remaining_Delay")
    if sql_remaining and str(sql_remaining).lower() != "null":
        try:
            result["sql_remaining_delay"] = int(sql_remaining)
        except (ValueError, TypeError):
            pass
    relay_space = result_row.get("Relay_Log_Space")
    if relay_space:
        try:
            result["relay_log_space"] = int(relay_space)
        except (ValueError, TypeError):
            pass
    
    return result


def parse_master_status(result_row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Parse SHOW MASTER STATUS (or SHOW BINARY LOG STATUS) output.
    
    Args:
        result_row: Dict from PyMySQL with master status columns
    
    Returns:
        Dictionary with current master binlog position.
        Returns {"is_master": False} if not configured as master or binlog disabled.
    """
    result = {
        "is_master": False,
        "file": None,
        "position": None,
        "binlog_do_db": None,
        "binlog_ignore_db": None,
        "executed_gtid_set": None,
    }
    
    if not result_row:
        return result
    
    result["is_master"] = True
    result["file"] = result_row.get("File") or result_row.get("Binlog_File")
    position = result_row.get("Position") or result_row.get("Binlog_Position")
    if position:
        try:
            result["position"] = int(position)
        except (ValueError, TypeError):
            pass
    result["binlog_do_db"] = result_row.get("Binlog_Do_DB")
    result["binlog_ignore_db"] = result_row.get("Binlog_Ignore_DB")
    result["executed_gtid_set"] = result_row.get("Executed_Gtid_Set")
    
    return result


def calculate_buffer_pool_metrics(
    global_status: Dict[str, Any],
    config_vars: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Calculate InnoDB Buffer Pool metrics from existing parsed data.
    
    Uses ONLY data from SHOW GLOBAL STATUS and SHOW GLOBAL VARIABLES.
    No additional queries required.
    
    Args:
        global_status: Parsed output from SHOW GLOBAL STATUS
        config_vars: Parsed output from SHOW GLOBAL VARIABLES
        
    Returns:
        Dictionary with derived buffer pool metrics
    """
    result = {
        "pool_size_gb": None,
        "used_gb": None,
        "free_gb": None,
        "used_percent": None,
        "free_percent": None,
        "dirty_percent": None,
        "hit_ratio": None,
        "wait_free": None,
        "health": "unknown",
        "health_reason": None,
    }
    
    try:
        # Get config values
        pool_size_bytes = _safe_int(config_vars.get("innodb_buffer_pool_size"))
        page_size = _safe_int(config_vars.get("innodb_page_size", 16384))  # Default 16KB
        
        # Get status values
        pages_total = _safe_int(global_status.get("Innodb_buffer_pool_pages_total"))
        pages_free = _safe_int(global_status.get("Innodb_buffer_pool_pages_free"))
        pages_dirty = _safe_int(global_status.get("Innodb_buffer_pool_pages_dirty"))
        buffer_reads = _safe_int(global_status.get("Innodb_buffer_pool_reads"))
        read_requests = _safe_int(global_status.get("Innodb_buffer_pool_read_requests"))
        wait_free = _safe_int(global_status.get("Innodb_buffer_pool_wait_free"))
        
        # Calculate derived metrics
        if pool_size_bytes and pool_size_bytes > 0:
            result["pool_size_gb"] = round(pool_size_bytes / (1024 ** 3), 1)
        
        if pages_total and page_size and pages_total > 0:
            # Free and Used bytes
            free_bytes = (pages_free or 0) * page_size
            used_pages = pages_total - (pages_free or 0)
            used_bytes = used_pages * page_size
            
            result["free_gb"] = round(free_bytes / (1024 ** 3), 1)
            result["used_gb"] = round(used_bytes / (1024 ** 3), 1)
            
            # Percentages (based on pool_size_bytes for accuracy)
            if pool_size_bytes and pool_size_bytes > 0:
                result["used_percent"] = round((used_bytes / pool_size_bytes) * 100, 1)
                result["free_percent"] = round(100 - result["used_percent"], 1)
            
            # Dirty pages percentage
            if pages_dirty is not None:
                result["dirty_percent"] = round((pages_dirty / pages_total) * 100, 1)
        
        # Hit ratio calculation
        if read_requests is not None and read_requests > 0:
            # Hit ratio = 1 - (physical reads / logical read requests)
            hit_ratio = (1 - (buffer_reads / read_requests)) * 100
            result["hit_ratio"] = round(max(0, min(100, hit_ratio)), 1)
        elif read_requests == 0:
            # No read requests, treat as 100% hit ratio
            result["hit_ratio"] = 100.0
        
        # Wait free count
        result["wait_free"] = wait_free if wait_free is not None else 0
        
        # Health badge calculation
        hit = result["hit_ratio"]
        wait = result["wait_free"] or 0
        
        if hit is not None:
            if hit >= 99 and wait == 0:
                result["health"] = "healthy"
                result["health_reason"] = "Excellent hit ratio with no wait events"
            elif hit >= 97:
                result["health"] = "mild"
                result["health_reason"] = "Good hit ratio, minor optimization possible"
            else:
                result["health"] = "pressure"
                result["health_reason"] = "Low hit ratio indicates memory pressure"
        
        if wait > 0:
            result["health"] = "pressure"
            result["health_reason"] = f"Wait free events ({wait}) indicate buffer pool exhaustion"
        
    except Exception:
        # Return partial results on error
        pass
    
    return result


def _safe_int(value: Any, default: int = 0) -> Optional[int]:
    """Safely convert a value to int, returning default if not possible."""
    if value is None:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return default


# =============================================================================
# InnoDB Health Analysis Functions
# =============================================================================

def parse_deadlock_info(raw_output: str) -> Dict[str, Any]:
    """
    Extract deadlock information from SHOW ENGINE INNODB STATUS.
    
    Returns:
        {
            "has_deadlock": bool,
            "timestamp": str or None,
            "transactions": [
                {
                    "trx_id": str,
                    "table": str,
                    "index": str,
                    "operation": str,  # INSERT/UPDATE/DELETE
                    "was_rolled_back": bool
                }
            ],
            "victim_trx_id": str or None
        }
    """
    result = {
        "has_deadlock": False,
        "timestamp": None,
        "transactions": [],
        "victim_trx_id": None,
        "raw_section": None,
    }
    
    # Handle literal \n in output
    if '\\n' in raw_output:
        raw_output = raw_output.replace('\\n', '\n')
    
    # Extract LATEST DETECTED DEADLOCK section
    deadlock_section = _extract_section(raw_output, "LATEST DETECTED DEADLOCK")
    if not deadlock_section:
        return result
    
    result["raw_section"] = deadlock_section
    result["has_deadlock"] = True
    
    # Extract timestamp - could be in section content OR on the header line itself
    # First try to find it in the section content
    timestamp_match = _RE_TIMESTAMP.search(deadlock_section)
    if timestamp_match:
        result["timestamp"] = timestamp_match.group(1)
    else:
        # Try to find it on the header line (format: "LATEST DETECTED DEADLOCK ------- 2025-10-28 13:20:17")
        header_match = _RE_TIMESTAMP.search(raw_output[raw_output.find("LATEST DETECTED DEADLOCK"):raw_output.find("LATEST DETECTED DEADLOCK")+100] if "LATEST DETECTED DEADLOCK" in raw_output else "")
        if header_match:
            result["timestamp"] = header_match.group(1)
    
    # Extract transactions involved in deadlock
    # Pattern: *** (1) TRANSACTION: or *** (2) TRANSACTION:
    trx_blocks = re.split(r'\*\*\* \(\d+\) TRANSACTION:', deadlock_section)
    
    for i, block in enumerate(trx_blocks[1:], 1):  # Skip first empty split
        trx_info = {
            "trx_id": None,
            "thread_id": None,
            "user": None,
            "host": None,
            "query_id": None,
            "active_time": None,
            "state": None,
            "tables_in_use": None,
            "tables_locked": None,
            "lock_structs": None,
            "row_locks": None,
            "undo_log_entries": None,
            "table": None,
            "index": None,
            "operation": None,
            "query": None,
            "was_rolled_back": False,
            "lock_mode": None,
        }
        
        # Extract transaction ID and active time
        # Pattern: "TRANSACTION 3918278554, ACTIVE 49 sec inserting"
        trx_match = _RE_TRX_ID.search(block)
        if trx_match:
            trx_info["trx_id"] = trx_match.group(1)
            trx_info["active_time"] = int(trx_match.group(2))
            trx_info["state"] = trx_match.group(3)  # e.g., "inserting", "updating"
        
        # Extract tables in use/locked
        # Pattern: "mysql tables in use 1, locked 1"
        tables_match = _RE_TABLES_LOCKED.search(block)
        if tables_match:
            trx_info["tables_in_use"] = int(tables_match.group(1))
            trx_info["tables_locked"] = int(tables_match.group(2))
        
        # Extract lock structs, row locks, undo log entries
        # Pattern: "4 lock struct(s), heap size 1128, 2 row lock(s), undo log entries 1"
        locks_match = _RE_LOCK_STRUCTS.search(block)
        if locks_match:
            trx_info["lock_structs"] = int(locks_match.group(1))
            trx_info["row_locks"] = int(locks_match.group(2))
            if locks_match.group(3):
                trx_info["undo_log_entries"] = int(locks_match.group(3))
        
        # Extract MySQL thread id, query id, host and user
        # Pattern: "MySQL thread id 19357609, OS thread handle 70425783342416, query id 35878558901 172.20.61.93 polo-worker update"
        thread_match = _RE_THREAD_INFO.search(block)
        if thread_match:
            trx_info["thread_id"] = thread_match.group(1)
            trx_info["query_id"] = thread_match.group(2)
            trx_info["host"] = thread_match.group(3)
            trx_info["user"] = thread_match.group(4)
        
        # Extract the actual SQL query (starts with INSERT/UPDATE/DELETE/SELECT/REPLACE)
        # The query appears after the "MySQL thread id ... user state" line
        # Match from the SQL keyword to end of block or next section marker
        query_match = re.search(
            r"(?:^|\n)((?:INSERT|UPDATE|DELETE|SELECT|REPLACE)\s+(?:INTO\s+)?[`\w].*?)(?:\n\*\*\*|\nRECORD LOCKS|\n---|\n$|$)", 
            block, 
            re.IGNORECASE | re.DOTALL
        )
        if query_match:
            query = query_match.group(1).strip()
            # Clean up any trailing whitespace/newlines
            query = ' '.join(query.split())
            # Truncate very long queries
            if len(query) > 500:
                query = query[:500] + "..."
            trx_info["query"] = query
        
        # Extract table name from query or lock info
        table_match = _RE_TABLE_INDEX.search(block)
        if table_match:
            trx_info["table"] = f"{table_match.group(1)}.{table_match.group(2)}"
        elif not trx_info["table"] and trx_info["query"]:
            # Try to extract table from query
            query_table = re.search(r"(?:INTO|FROM|UPDATE)\s+(\w+)", trx_info["query"], re.IGNORECASE)
            if query_table:
                trx_info["table"] = query_table.group(1)
        
        # Extract index name
        index_match = _RE_INDEX_NAME.search(block)
        if index_match:
            trx_info["index"] = index_match.group(1)
        
        # Detect operation type from state or query
        state_upper = (trx_info["state"] or "").upper()
        if "INSERT" in state_upper or (trx_info["query"] and "INSERT" in trx_info["query"].upper()):
            trx_info["operation"] = "INSERT"
        elif "UPDATE" in state_upper or "UPDATING" in state_upper or (trx_info["query"] and "UPDATE" in trx_info["query"].upper()):
            trx_info["operation"] = "UPDATE"
        elif "DELETE" in state_upper or (trx_info["query"] and "DELETE" in trx_info["query"].upper()):
            trx_info["operation"] = "DELETE"
        elif "SELECT" in state_upper or (trx_info["query"] and "SELECT" in trx_info["query"].upper()):
            trx_info["operation"] = "SELECT"
        
        # Extract lock mode
        lock_match = re.search(r"(RECORD LOCKS|GAP|X|S) lock", block, re.IGNORECASE)
        if lock_match:
            trx_info["lock_mode"] = lock_match.group(1).upper()
        
        result["transactions"].append(trx_info)
    
    # Find the rolled back transaction (victim)
    victim_match = re.search(r"WE ROLL BACK TRANSACTION \((\d+)\)", deadlock_section)
    if victim_match:
        victim_num = int(victim_match.group(1))
        result["victim_trx_id"] = result["transactions"][victim_num - 1]["trx_id"] if victim_num <= len(result["transactions"]) else None
        if victim_num <= len(result["transactions"]):
            result["transactions"][victim_num - 1]["was_rolled_back"] = True
    
    return result


def parse_lock_contention(raw_output: str) -> Dict[str, Any]:
    """
    Detect lock contention from TRANSACTIONS section.
    
    OPTIMIZED: Uses string operations instead of expensive regex with .*? and DOTALL.
    
    Returns:
        {
            "lock_waiting_transactions": int,
            "has_contention": bool,
            "lock_wait_details": [...]
        }
    """
    result = {
        "lock_waiting_transactions": 0,
        "has_contention": False,
        "lock_wait_details": [],
        "history_list_length": 0,
    }
    
    # Handle literal \n in output
    if '\\n' in raw_output:
        raw_output = raw_output.replace('\\n', '\n')
    
    # Extract TRANSACTIONS section
    trx_section = _extract_section(raw_output, "TRANSACTIONS")
    if not trx_section:
        return result
    
    # FAST: Count "LOCK WAIT" occurrences using string method (O(n) single pass)
    lock_wait_count = trx_section.count("LOCK WAIT")
    result["lock_waiting_transactions"] = lock_wait_count
    result["has_contention"] = lock_wait_count > 0
    
    # Extract history list length (simple single-line pattern)
    history_match = _RE_HISTORY_LIST.search(trx_section)
    if history_match:
        result["history_list_length"] = int(history_match.group(1))
    
    # Only parse details if there's contention (limit expensive parsing)
    if lock_wait_count > 0:
        # Split by transaction markers and process only those with LOCK WAIT
        trx_blocks = trx_section.split("---TRANSACTION")
        details_found = 0
        
        for block in trx_blocks[1:]:  # Skip first empty split
            if details_found >= 5:
                break
            if "LOCK WAIT" not in block:
                continue
            
            detail = {"trx_id": None, "wait_seconds": 0, "table": None, "index": None}
            
            # Extract transaction ID (first number after split point)
            trx_match = re.match(r"\s*(\d+)", block)
            if trx_match:
                detail["trx_id"] = trx_match.group(1)
            
            # Extract wait time (look for "X sec" pattern)
            wait_match = _RE_WAIT_SEC.search(block)
            if wait_match:
                detail["wait_seconds"] = int(wait_match.group(1))
            
            # Extract table (simple pattern, no DOTALL needed)
            table_match = _RE_TABLE_INDEX.search(block)
            if table_match:
                detail["table"] = f"{table_match.group(1)}.{table_match.group(2)}"
            
            # Extract index
            index_match = _RE_INDEX_NAME.search(block)
            if index_match:
                detail["index"] = index_match.group(1)
            
            result["lock_wait_details"].append(detail)
            details_found += 1
    
    return result


def parse_hot_indexes(raw_output: str) -> Dict[str, Any]:
    """
    Identify hot indexes from deadlock and lock wait sections.
    
    Returns:
        {
            "hot_indexes": [
                {
                    "table": str,
                    "index": str,
                    "contention_count": int,
                    "lock_types": [str]
                }
            ]
        }
    """
    result = {
        "hot_indexes": [],
    }
    
    # Handle literal \n in output
    if '\\n' in raw_output:
        raw_output = raw_output.replace('\\n', '\n')
    
    # Track index contention
    index_stats = {}  # key: "table.index" -> {"count": N, "lock_types": set()}
    
    # Parse from deadlock section
    deadlock_section = _extract_section(raw_output, "LATEST DETECTED DEADLOCK")
    if deadlock_section:
        _extract_index_locks(deadlock_section, index_stats)
    
    # Parse from transactions section
    trx_section = _extract_section(raw_output, "TRANSACTIONS")
    if trx_section:
        _extract_index_locks(trx_section, index_stats)
    
    # Convert to list and sort by contention count
    for key, stats in index_stats.items():
        parts = key.split(".", 1)
        result["hot_indexes"].append({
            "table": parts[0] if len(parts) > 1 else "unknown",
            "index": parts[-1],
            "contention_count": stats["count"],
            "lock_types": list(stats["lock_types"]),
        })
    
    # Sort by contention and limit to top 3
    result["hot_indexes"].sort(key=lambda x: x["contention_count"], reverse=True)
    result["hot_indexes"] = result["hot_indexes"][:3]
    
    return result


def _extract_index_locks(section: str, index_stats: dict) -> None:
    """
    Helper to extract index lock information from a section.
    
    OPTIMIZED: Process line-by-line instead of regex with .*? DOTALL.
    """
    # Process line-by-line for speed (avoids catastrophic backtracking)
    lines = section.split('\n')
    current_table = None
    current_index = None
    
    for i, line in enumerate(lines):
        # Look for RECORD LOCKS lines
        if "RECORD LOCKS" in line:
            # Extract table and index from this line or next few lines
            context = ' '.join(lines[i:min(i+3, len(lines))])  # Look at 3 lines
            
            table_match = _RE_TABLE_INDEX.search(context)
            index_match = _RE_INDEX_NAME.search(context)
            
            if table_match and index_match:
                table = f"{table_match.group(1)}.{table_match.group(2)}"
                index = index_match.group(1)
                key = f"{table}.{index}"
                
                if key not in index_stats:
                    index_stats[key] = {"count": 0, "lock_types": set()}
                index_stats[key]["count"] += 1
                
                # Detect lock type from context
                context_lower = context.lower()
                if "gap" in context_lower:
                    index_stats[key]["lock_types"].add("GAP")
                if "rec but not gap" in context_lower:
                    index_stats[key]["lock_types"].add("RECORD")
                if " x " in context_lower or "exclusive" in context_lower:
                    index_stats[key]["lock_types"].add("X")
                if " s " in context_lower or "shared" in context_lower:
                    index_stats[key]["lock_types"].add("S")


def parse_semaphore_health(raw_output: str) -> Dict[str, Any]:
    """
    Parse semaphore/mutex health from SEMAPHORES section.
    
    Returns:
        {
            "has_mutex_contention": bool,
            "rw_shared_os_waits": int,
            "rw_excl_os_waits": int,
            "rw_sx_os_waits": int,
            "total_os_waits": int,
            "spin_rounds_per_wait": {
                "rw_shared": float,
                "rw_excl": float,
                "rw_sx": float
            }
        }
    """
    result = {
        "has_mutex_contention": False,
        "rw_shared_os_waits": 0,
        "rw_excl_os_waits": 0,
        "rw_sx_os_waits": 0,
        "total_os_waits": 0,
        "spin_rounds_per_wait": {},
    }
    
    # Handle literal \n in output
    if '\\n' in raw_output:
        raw_output = raw_output.replace('\\n', '\n')
    
    # Extract SEMAPHORES section
    sem_section = _extract_section(raw_output, "SEMAPHORES")
    if not sem_section:
        return result
    
    # Parse RW-shared
    rw_shared = re.search(r"RW-shared spins (\d+), rounds (\d+), OS waits (\d+)", sem_section)
    if rw_shared:
        result["rw_shared_os_waits"] = int(rw_shared.group(3))
    
    # Parse RW-excl
    rw_excl = re.search(r"RW-excl spins (\d+), rounds (\d+), OS waits (\d+)", sem_section)
    if rw_excl:
        result["rw_excl_os_waits"] = int(rw_excl.group(3))
    
    # Parse RW-sx
    rw_sx = re.search(r"RW-sx spins (\d+), rounds (\d+), OS waits (\d+)", sem_section)
    if rw_sx:
        result["rw_sx_os_waits"] = int(rw_sx.group(3))
    
    # Calculate total
    result["total_os_waits"] = (
        result["rw_shared_os_waits"] + 
        result["rw_excl_os_waits"] + 
        result["rw_sx_os_waits"]
    )
    
    # Determine if there's contention (non-zero OS waits)
    result["has_mutex_contention"] = result["total_os_waits"] > 0
    
    # Parse spin rounds per wait
    spin_match = re.search(r"Spin rounds per wait: ([\d.]+) RW-shared, ([\d.]+) RW-excl, ([\d.]+) RW-sx", sem_section)
    if spin_match:
        result["spin_rounds_per_wait"] = {
            "rw_shared": float(spin_match.group(1)),
            "rw_excl": float(spin_match.group(2)),
            "rw_sx": float(spin_match.group(3)),
        }
    
    return result


def parse_redo_log_health(raw_output: str, previous_checkpoint_age: Optional[int] = None) -> Dict[str, Any]:
    """
    Parse redo log health from LOG section.
    
    Args:
        raw_output: InnoDB status output
        previous_checkpoint_age: Checkpoint age from previous snapshot (for trend)
    
    Returns:
        {
            "log_sequence_number": int,
            "last_checkpoint": int,
            "checkpoint_age_bytes": int,
            "checkpoint_age_mb": float,
            "log_ios_done": int,
            "log_ios_per_sec": float,
            "trend": "stable" | "growing" | "shrinking",
            "health": "healthy" | "warning" | "critical"
        }
    """
    result = {
        "log_sequence_number": 0,
        "last_checkpoint": 0,
        "checkpoint_age_bytes": 0,
        "checkpoint_age_mb": 0.0,
        "log_ios_done": 0,
        "log_ios_per_sec": 0.0,
        "trend": "stable",
        "health": "healthy",
    }
    
    # Handle literal \n in output
    if '\\n' in raw_output:
        raw_output = raw_output.replace('\\n', '\n')
    
    # Extract LOG section
    log_section = _extract_section(raw_output, "LOG")
    if not log_section:
        return result
    
    # Parse log sequence number
    lsn_match = re.search(r"Log sequence number\s+(\d+)", log_section)
    if lsn_match:
        result["log_sequence_number"] = int(lsn_match.group(1))
    
    # Parse last checkpoint
    checkpoint_match = re.search(r"Last checkpoint at\s+(\d+)", log_section)
    if checkpoint_match:
        result["last_checkpoint"] = int(checkpoint_match.group(1))
    
    # Calculate checkpoint age
    if result["log_sequence_number"] and result["last_checkpoint"]:
        result["checkpoint_age_bytes"] = result["log_sequence_number"] - result["last_checkpoint"]
        result["checkpoint_age_mb"] = round(result["checkpoint_age_bytes"] / (1024 * 1024), 2)
    
    # Parse log I/O
    log_io_match = re.search(r"(\d+) log i/o's done", log_section)
    if log_io_match:
        result["log_ios_done"] = int(log_io_match.group(1))
    
    log_io_rate = re.search(r"([\d.]+) log i/o's/second", log_section)
    if log_io_rate:
        result["log_ios_per_sec"] = float(log_io_rate.group(1))
    
    # Determine trend (if previous data available)
    if previous_checkpoint_age is not None:
        age_diff = result["checkpoint_age_bytes"] - previous_checkpoint_age
        threshold = 1024 * 1024  # 1MB threshold for significance
        if age_diff > threshold:
            result["trend"] = "growing"
        elif age_diff < -threshold:
            result["trend"] = "shrinking"
        else:
            result["trend"] = "stable"
    
    # Determine health (rough heuristics)
    # Checkpoint age > 1GB is usually concerning
    if result["checkpoint_age_mb"] > 1024:
        result["health"] = "critical"
    elif result["checkpoint_age_mb"] > 512:
        result["health"] = "warning"
    else:
        result["health"] = "healthy"
    
    return result


def analyze_innodb_health(raw_output: str, previous_snapshot: Optional[Dict] = None) -> Dict[str, Any]:
    """
    Comprehensive InnoDB health analysis combining all metrics.
    
    Args:
        raw_output: Full SHOW ENGINE INNODB STATUS output
        previous_snapshot: Previous analysis result (for trend comparison)
    
    Returns:
        Combined analysis with all health metrics
    """
    prev_checkpoint_age = None
    if previous_snapshot and "redo_log" in previous_snapshot:
        prev_checkpoint_age = previous_snapshot["redo_log"].get("checkpoint_age_bytes")
    
    result = {
        "deadlock": parse_deadlock_info(raw_output),
        "lock_contention": parse_lock_contention(raw_output),
        "hot_indexes": parse_hot_indexes(raw_output),
        "semaphore": parse_semaphore_health(raw_output),
        "redo_log": parse_redo_log_health(raw_output, prev_checkpoint_age),
        "summary": {
            "has_issues": False,
            "issues": [],
        }
    }
    
    # Build summary
    issues = []
    
    if result["deadlock"]["has_deadlock"]:
        issues.append({
            "category": "deadlock",
            "severity": "critical",
            "message": f"Deadlock detected at {result['deadlock']['timestamp']}",
        })
    
    if result["lock_contention"]["has_contention"]:
        count = result["lock_contention"]["lock_waiting_transactions"]
        severity = "critical" if count > 5 else "warning"
        issues.append({
            "category": "lock_contention",
            "severity": severity,
            "message": f"{count} transaction(s) waiting for locks",
        })
    
    if result["hot_indexes"]["hot_indexes"]:
        top_index = result["hot_indexes"]["hot_indexes"][0]
        issues.append({
            "category": "hot_index",
            "severity": "warning",
            "message": f"Index contention on {top_index['table']}.{top_index['index']}",
        })
    
    if result["semaphore"]["has_mutex_contention"]:
        total = result["semaphore"]["total_os_waits"]
        if total > 1000:
            issues.append({
                "category": "semaphore",
                "severity": "warning",
                "message": f"High mutex contention ({total} OS waits)",
            })
    
    if result["redo_log"]["health"] != "healthy":
        issues.append({
            "category": "redo_log",
            "severity": result["redo_log"]["health"],
            "message": f"Redo log pressure ({result['redo_log']['checkpoint_age_mb']:.1f} MB checkpoint age)",
        })
    
    result["summary"]["has_issues"] = len(issues) > 0
    result["summary"]["issues"] = issues
    
    return result

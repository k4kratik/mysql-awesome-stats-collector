"""FastAPI main application for MySQL Awesome Stats Collector (MASC)."""

import logging
import sys
import uuid
from datetime import datetime

from fastapi import FastAPI, Request, Depends, Form, BackgroundTasks, Query
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from sqlalchemy.orm import Session
from typing import List, Optional
from pathlib import Path
import json

from .db import init_db, get_db, get_db_context
from .models import Job, JobHost, JobStatus, HostJobStatus, CronJob, DBHost, DBGroup
from .utils import (
    load_hosts,
    load_all_hosts,
    get_host_by_id,
    generate_job_id,
    generate_job_host_id,
    get_host_output_dir,
    get_job_dir,
    read_file_safe,
    read_json_safe,
)
from .collector import run_collection_job
from .parser import get_key_metrics, parse_innodb_status_structured, CONFIG_VARIABLES_ALLOWLIST, evaluate_config_health
from . import __version__

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("masc")

# Initialize FastAPI app
app = FastAPI(
    title="MySQL Awesome Stats Collector",
    description="Collect and visualize MySQL diagnostics from multiple hosts",
    version="1.0.0"
)

# Log startup configuration
from .utils import HOSTS_FILE
logger.info(f"Hosts file: {HOSTS_FILE}")

# Setup templates and static files
BASE_DIR = Path(__file__).parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


# Custom Jinja2 filters
def format_bytes(value, precision=2):
    """Format bytes to human-readable format (KB, MB, GB, TB, PB)."""
    if value is None or value == 0:
        return "0 B"
    try:
        value = float(value)
    except (ValueError, TypeError):
        return str(value)
    
    units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
    unit_index = 0
    while abs(value) >= 1024 and unit_index < len(units) - 1:
        value /= 1024
        unit_index += 1
    return f"{value:.{precision}f} {units[unit_index]}"


def format_number(value, precision=1):
    """Format large numbers to human-readable format (K, M, B, T)."""
    if value is None:
        return "0"
    try:
        value = float(value)
    except (ValueError, TypeError):
        return str(value)
    
    if abs(value) < 1000:
        return f"{int(value)}" if value == int(value) else f"{value:.{precision}f}"
    
    units = ['', 'K', 'M', 'B', 'T']
    unit_index = 0
    while abs(value) >= 1000 and unit_index < len(units) - 1:
        value /= 1000
        unit_index += 1
    return f"{value:.{precision}f}{units[unit_index]}"


def format_uptime(seconds):
    """Format seconds to human-readable uptime (e.g., 7d 12h, 3h 45m)."""
    if seconds is None:
        return "N/A"
    try:
        seconds = int(seconds)
    except (ValueError, TypeError):
        return str(seconds)
    
    days = seconds // 86400
    hours = (seconds % 86400) // 3600
    mins = (seconds % 3600) // 60
    
    if days > 0:
        return f"{days}d {hours}h"
    elif hours > 0:
        return f"{hours}h {mins}m"
    else:
        return f"{mins}m"


# Register custom filters
templates.env.filters["format_bytes"] = format_bytes
templates.env.filters["format_number"] = format_number
templates.env.filters["format_uptime"] = format_uptime

# Add version to template globals (available in all templates)
templates.env.globals["app_version"] = __version__


# Mount static files - use app/static for package compatibility
STATIC_DIR = BASE_DIR / "static"
STATIC_DIR.mkdir(exist_ok=True)
(STATIC_DIR / "css").mkdir(exist_ok=True)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.on_event("startup")
async def startup_event():
    """Initialize database and scheduler on startup."""
    logger.info("Starting MASC...")
    init_db()
    hosts = load_hosts()
    logger.info(f"Loaded {len(hosts)} host(s) from configuration:")
    for h in hosts:
        logger.info(f"  - {h.id}: {h.label} ({h.host}:{h.port}, user={h.user})")
    
    # Start cron scheduler
    from .scheduler import start_scheduler
    start_scheduler()
    logger.info("Cron scheduler started")
    
    logger.info("MASC ready")


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown."""
    from .scheduler import stop_scheduler
    stop_scheduler()
    logger.info("MASC shutdown complete")


# =============================================================================
# HOME PAGE - Host Selection
# =============================================================================

@app.get("/", response_class=HTMLResponse)
async def index(request: Request, db: Session = Depends(get_db)):
    """Home page with host selection."""
    hosts = load_hosts()
    
    # Get all groups for organized display
    groups = db.query(DBGroup).order_by(DBGroup.name).all()
    
    # Get all hosts from DB to map group_id
    db_hosts = db.query(DBHost).filter(DBHost.enabled == True).all()
    host_group_map = {h.id: h.group_id for h in db_hosts}
    
    # Organize hosts by group
    grouped_hosts = {}
    ungrouped_hosts = []
    for host in hosts:
        group_id = host_group_map.get(host.id)
        if group_id:
            if group_id not in grouped_hosts:
                grouped_hosts[group_id] = []
            grouped_hosts[group_id].append(host)
        else:
            ungrouped_hosts.append(host)
    
    return templates.TemplateResponse("index.html", {
        "request": request,
        "hosts": hosts,
        "groups": groups,
        "grouped_hosts": grouped_hosts,
        "ungrouped_hosts": ungrouped_hosts,
        "page_title": "MASC"
    })


# =============================================================================
# JOB CREATION
# =============================================================================

@app.post("/jobs/create")
async def create_job(
    request: Request,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """Create a new collection job."""
    # Get form data
    form = await request.form()
    selected_hosts = form.getlist("hosts")
    job_name = form.get("job_name", "").strip() or None  # Empty string -> None
    collect_hot_tables = form.get("collect_hot_tables") == "1"  # Checkbox value
    
    if not selected_hosts:
        logger.warning("Job creation attempted with no hosts selected")
        # Redirect back with error
        hosts = load_hosts()
        return templates.TemplateResponse("index.html", {
            "request": request,
            "hosts": hosts,
            "page_title": "MASC",
            "error": "Please select at least one host"
        })
    
    # Create job
    job_id = generate_job_id()
    job_display = f"'{job_name}' ({job_id[:8]})" if job_name else job_id[:8]
    logger.info(f"Creating job {job_display} for {len(selected_hosts)} host(s)")
    all_hosts = load_hosts()
    hosts_map = {h.id: h for h in all_hosts}
    for host_id in selected_hosts:
        h = hosts_map.get(host_id)
        if h:
            logger.info(f"  - {h.id}: {h.label} ({h.host}:{h.port})")
        else:
            logger.warning(f"  - {host_id}: (unknown host)")
    job = Job(id=job_id, name=job_name, status=JobStatus.pending)
    db.add(job)
    
    # Create job hosts
    for host_id in selected_hosts:
        job_host = JobHost(
            id=generate_job_host_id(),
            job_id=job_id,
            host_id=host_id,
            status=HostJobStatus.pending
        )
        db.add(job_host)
    
    db.commit()
    
    # Start background collection (with optional hot tables)
    background_tasks.add_task(run_collection_job, job_id, list(selected_hosts), collect_hot_tables)
    
    if collect_hot_tables:
        logger.info(f"  Hot Tables collection: ENABLED")
    
    # Redirect to job detail page
    return RedirectResponse(url=f"/jobs/{job_id}", status_code=303)


# =============================================================================
# JOBS LIST
# =============================================================================

@app.get("/jobs", response_class=HTMLResponse)
async def list_jobs(
    request: Request, 
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=10, le=200),
    db: Session = Depends(get_db)
):
    """List all jobs with pagination."""
    # Pre-load all host configs once (instead of per-job lookup)
    all_hosts = load_hosts()
    host_label_map = {h.id: (h.label or h.host) for h in all_hosts}
    
    # Count total jobs for pagination
    total_jobs = db.query(Job).count()
    total_pages = (total_jobs + per_page - 1) // per_page
    
    # Fetch paginated jobs (hosts are eager-loaded via relationship)
    jobs = db.query(Job).order_by(Job.created_at.desc()).offset((page - 1) * per_page).limit(per_page).all()
    
    # Enrich with host counts and labels
    jobs_data = []
    for job in jobs:
        host_count = len(job.hosts)
        completed_count = sum(1 for h in job.hosts if h.status == HostJobStatus.completed)
        failed_count = sum(1 for h in job.hosts if h.status == HostJobStatus.failed)
        
        # Get host labels using pre-loaded map (fast!)
        host_labels = [host_label_map.get(jh.host_id, jh.host_id) for jh in job.hosts]
        
        jobs_data.append({
            "job": job,
            "host_count": host_count,
            "completed_count": completed_count,
            "failed_count": failed_count,
            "host_labels": host_labels
        })
    
    return templates.TemplateResponse("jobs.html", {
        "request": request,
        "jobs": jobs_data,
        "page_title": "Jobs",
        "page": page,
        "per_page": per_page,
        "total_jobs": total_jobs,
        "total_pages": total_pages,
    })


# =============================================================================
# JOB DETAIL
# =============================================================================

@app.get("/jobs/{job_id}", response_class=HTMLResponse)
async def job_detail(request: Request, job_id: str, db: Session = Depends(get_db)):
    """Job detail page."""
    job = db.query(Job).filter(Job.id == job_id).first()
    
    if not job:
        return templates.TemplateResponse("error.html", {
            "request": request,
            "error": "Job not found",
            "page_title": "Error"
        }, status_code=404)
    
    # Enrich hosts with labels and replica status
    hosts_data = []
    all_hosts = load_hosts()
    hosts_map = {h.id: h for h in all_hosts}
    
    for job_host in job.hosts:
        host_config = hosts_map.get(job_host.host_id)
        # Load replica status and buffer pool for this host
        output_dir = get_host_output_dir(job_id, job_host.host_id)
        replica_status = read_json_safe(output_dir / "replica_status.json") or {}
        buffer_pool = read_json_safe(output_dir / "buffer_pool.json") or {}
        
        hosts_data.append({
            "job_host": job_host,
            "label": host_config.label if host_config else job_host.host_id,
            "host": host_config.host if host_config else "unknown",
            "port": host_config.port if host_config else 0,
            "replica_status": replica_status,
            "buffer_pool": buffer_pool
        })
    
    return templates.TemplateResponse("job_detail.html", {
        "request": request,
        "job": job,
        "hosts": hosts_data,
        "page_title": f"Job {job_id[:8]}..."
    })


@app.post("/jobs/{job_id}/rerun")
async def rerun_job(
    job_id: str,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """Re-run a job with the same hosts and settings."""
    from fastapi.responses import RedirectResponse
    
    # Get original job
    original_job = db.query(Job).filter(Job.id == job_id).first()
    if not original_job:
        return RedirectResponse(url="/jobs", status_code=303)
    
    # Get host IDs from original job
    host_ids = [jh.host_id for jh in original_job.hosts]
    
    if not host_ids:
        return RedirectResponse(url=f"/jobs/{job_id}", status_code=303)
    
    # Check if hot_tables was collected by checking if any host has hot_tables.json
    collect_hot_tables = False
    for host_id in host_ids:
        hot_tables_file = get_job_dir(job_id) / host_id / "hot_tables.json"
        if hot_tables_file.exists():
            collect_hot_tables = True
            break
    
    # Create new job (no name to avoid "Re-run of Re-run of..." chains)
    new_job_id = str(uuid.uuid4())
    
    new_job = Job(id=new_job_id, name=None, status=JobStatus.pending)
    db.add(new_job)
    
    # Create job_host entries
    for host_id in host_ids:
        job_host = JobHost(
            id=str(uuid.uuid4()),
            job_id=new_job_id,
            host_id=host_id,
            status=HostJobStatus.pending
        )
        db.add(job_host)
    
    db.commit()
    
    # Start collection in background
    background_tasks.add_task(run_collection_job, new_job_id, host_ids, collect_hot_tables)
    
    return RedirectResponse(url=f"/jobs/{new_job_id}", status_code=303)


# =============================================================================
# HOST OUTPUT VIEW
# =============================================================================

@app.get("/jobs/{job_id}/hosts/{host_id}", response_class=HTMLResponse)
async def host_detail(
    request: Request,
    job_id: str,
    host_id: str,
    tab: str = "innodb",
    user_filter: Optional[str] = None,
    state_filter: Optional[str] = None,
    min_time: Optional[str] = None,
    query_filter: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """Host output detail page with tabs."""
    # Convert min_time to int if provided and not empty
    min_time_int: Optional[int] = None
    if min_time and min_time.strip():
        try:
            min_time_int = int(min_time)
        except ValueError:
            min_time_int = None
    
    # Verify job and host exist
    job = db.query(Job).filter(Job.id == job_id).first()
    if not job:
        return templates.TemplateResponse("error.html", {
            "request": request,
            "error": "Job not found",
            "page_title": "Error"
        }, status_code=404)
    
    job_host = db.query(JobHost).filter(
        JobHost.job_id == job_id,
        JobHost.host_id == host_id
    ).first()
    
    if not job_host:
        return templates.TemplateResponse("error.html", {
            "request": request,
            "error": "Host not found in this job",
            "page_title": "Error"
        }, status_code=404)
    
    # Get host config
    host_config = get_host_by_id(host_id)
    host_label = host_config.label if host_config else host_id
    
    # Get all hosts in this job for the dropdown navigation
    job_hosts_list = []
    all_host_configs = load_hosts()
    host_config_map = {h.id: h for h in all_host_configs}
    for jh in job.hosts:
        hc = host_config_map.get(jh.host_id)
        job_hosts_list.append({
            "host_id": jh.host_id,
            "label": hc.label if hc else jh.host_id,
            "status": jh.status.value,
        })
    
    # Get output directory
    output_dir = get_host_output_dir(job_id, host_id)
    
    # Load timing data (always available)
    timing_data = read_json_safe(output_dir / "timing.json") or {}
    
    # Load replica status (always, for header display)
    replica_status = read_json_safe(output_dir / "replica_status.json") or {}
    
    # Load master status (for master binlog position)
    master_status = read_json_safe(output_dir / "master_status.json") or {}
    
    # Load buffer pool metrics (always, for summary card)
    buffer_pool = read_json_safe(output_dir / "buffer_pool.json") or {}
    
    # Load hot tables (optional, only if collected)
    hot_tables = read_json_safe(output_dir / "hot_tables.json") or {}
    
    # Load InnoDB health analysis (deadlocks, lock contention, hot indexes, etc.)
    innodb_health = read_json_safe(output_dir / "innodb_health.json") or {}
    
    # Load ALL tab data upfront for client-side tab switching (no page refresh)
    master_info = None  # Info about the master if this is a replica
    
    # Raw output - convert literal \n to actual newlines (MySQL escapes them in InnoDB status)
    raw_output = read_file_safe(output_dir / "raw.txt") or "No raw output available"
    if raw_output and '\\n' in raw_output:
        raw_output = raw_output.replace('\\n', '\n')
    
    # InnoDB
    innodb_output = read_file_safe(output_dir / "innodb.txt") or "No InnoDB output available"
    innodb_structured = parse_innodb_status_structured(raw_output)
    
    # Global Status
    global_status = read_json_safe(output_dir / "global_status.json") or {}
    key_metrics = get_key_metrics(global_status)
    
    # Processlist (all data - filtering is done client-side for better UX)
    processlist = read_json_safe(output_dir / "processlist.json") or []
    
    # Config
    config_vars = read_json_safe(output_dir / "config_vars.json") or {}
    important_vars = {k: v for k, v in config_vars.items() if k in CONFIG_VARIABLES_ALLOWLIST}
    config_health = evaluate_config_health(important_vars, global_status)
    
    # Replication - find master info if this is a replica
    if replica_status.get("is_replica") and replica_status.get("master_host"):
            master_host_addr = replica_status.get("master_host")
            master_port = replica_status.get("master_port", 3306)
            
            # Look through other hosts in this job to find the master
            for job_host_entry in job.hosts:
                other_host_config = get_host_by_id(job_host_entry.host_id)
                if other_host_config:
                    # Check if this host matches the master address
                    if (other_host_config.host == master_host_addr or 
                        master_host_addr in other_host_config.host):
                        if other_host_config.port == master_port:
                            # Found the master! Load its master_status
                            master_output_dir = get_host_output_dir(job_id, job_host_entry.host_id)
                            master_master_status = read_json_safe(master_output_dir / "master_status.json") or {}
                            if master_master_status.get("is_master"):
                                master_info = {
                                    "host_id": job_host_entry.host_id,
                                    "label": other_host_config.label,
                                    "host": other_host_config.host,
                                    "port": other_host_config.port,
                                    "binlog_file": master_master_status.get("file"),
                                    "binlog_position": master_master_status.get("position"),
                                    "executed_gtid_set": master_master_status.get("executed_gtid_set"),
                                }
                            break
    
    return templates.TemplateResponse("host_detail.html", {
        "request": request,
        "job": job,
        "job_host": job_host,
        "host_id": host_id,
        "host_label": host_label,
        "job_hosts_list": job_hosts_list,
        "tab": tab,
        "raw_output": raw_output,
        "innodb_output": innodb_output,
        "innodb_structured": innodb_structured,
        "global_status": global_status,
        "key_metrics": json.dumps(key_metrics) if key_metrics else "{}",
        "processlist": processlist,
        "config_vars": config_vars,
        "config_health": config_health,
        "config_allowlist": CONFIG_VARIABLES_ALLOWLIST,
        "user_filter": user_filter or "",
        "state_filter": state_filter or "",
        "min_time": min_time_int if min_time_int is not None else "",
        "query_filter": query_filter or "",
        "timing_data": timing_data,
        "replica_status": replica_status,
        "master_status": master_status,
        "master_info": master_info,
        "buffer_pool": buffer_pool,
        "hot_tables": hot_tables,
        "innodb_health": innodb_health,
        "page_title": f"{host_label} Output"
    })


# =============================================================================
# API ENDPOINTS (for AJAX refreshing)
# =============================================================================

@app.get("/api/jobs/{job_id}/status")
async def get_job_status(job_id: str, db: Session = Depends(get_db)):
    """Get current job status with real-time command progress (for polling)."""
    job = db.query(Job).filter(Job.id == job_id).first()
    
    if not job:
        return {"error": "Job not found"}
    
    hosts_status = []
    for job_host in job.hosts:
        host_info = {
            "host_id": job_host.host_id,
            "status": job_host.status.value,
            "error_message": job_host.error_message
        }
        
        # Read real-time progress for running hosts
        if job_host.status.value == "running":
            progress_file = get_job_dir(job_id) / job_host.host_id / "progress.json"
            if progress_file.exists():
                try:
                    with open(progress_file) as f:
                        host_info["progress"] = json.load(f)
                except Exception:
                    pass
        
        hosts_status.append(host_info)
    
    return {
        "job_id": job_id,
        "status": job.status.value,
        "hosts": hosts_status
    }


@app.get("/api/jobs/{job_id}/raw-outputs")
async def get_all_raw_outputs(job_id: str, db: Session = Depends(get_db)):
    """Get all raw outputs for all hosts in a job (for batch download)."""
    job = db.query(Job).filter(Job.id == job_id).first()
    
    if not job:
        return {"error": "Job not found"}
    
    outputs = []
    for job_host in job.hosts:
        host = get_host_by_id(job_host.host_id)
        if not host:
            continue
        
        raw_file = get_job_dir(job_id) / job_host.host_id / "raw.txt"
        if raw_file.exists():
            try:
                with open(raw_file, 'r') as f:
                    content = f.read()
                outputs.append({
                    "host_id": job_host.host_id,
                    "label": host.label,
                    "content": content
                })
            except Exception:
                pass
    
    return {"outputs": outputs}


# =============================================================================
# COMPARE ROUTES
# =============================================================================

@app.get("/compare", response_class=HTMLResponse)
async def compare_page(request: Request, db: Session = Depends(get_db)):
    """Compare entry page - select two jobs to compare."""
    # Get only completed jobs
    jobs = db.query(Job).filter(Job.status == JobStatus.completed).order_by(Job.created_at.desc()).all()
    
    return templates.TemplateResponse("compare.html", {
        "request": request,
        "page_title": "Compare Jobs",
        "jobs": jobs,
    })


@app.get("/compare/result", response_class=HTMLResponse)
async def compare_result(
    request: Request,
    job_a: str = Query(..., description="Job A ID"),
    job_b: str = Query(..., description="Job B ID"),
    db: Session = Depends(get_db)
):
    """Compare two jobs and show results."""
    from .compare import (
        compare_global_status,
        compare_processlist,
        compare_config,
        compare_innodb_text,
        compare_buffer_pool,
        find_common_hosts,
        detect_regressions,
        refine_regressions,
    )
    
    # Validate same job not selected
    if job_a == job_b:
        jobs = db.query(Job).filter(Job.status == JobStatus.completed).order_by(Job.created_at.desc()).all()
        return templates.TemplateResponse("compare.html", {
            "request": request,
            "page_title": "Compare Jobs",
            "jobs": jobs,
            "error": "Cannot compare a job with itself. Please select two different jobs.",
        })
    
    # Validate jobs exist
    job_a_obj = db.query(Job).filter(Job.id == job_a).first()
    job_b_obj = db.query(Job).filter(Job.id == job_b).first()
    
    if not job_a_obj or not job_b_obj:
        jobs = db.query(Job).filter(Job.status == JobStatus.completed).order_by(Job.created_at.desc()).all()
        return templates.TemplateResponse("compare.html", {
            "request": request,
            "page_title": "Compare Jobs",
            "jobs": jobs,
            "error": "One or both jobs not found.",
        })
    
    # Get hosts from each job
    hosts_a = [jh.host_id for jh in job_a_obj.hosts if jh.status == HostJobStatus.completed]
    hosts_b = [jh.host_id for jh in job_b_obj.hosts if jh.status == HostJobStatus.completed]
    
    # Find common hosts
    common_hosts = find_common_hosts(hosts_a, hosts_b)
    
    # Load host labels
    all_hosts = load_hosts()
    host_labels = {h.id: h.label for h in all_hosts}
    
    # Build comparisons for each common host
    comparisons = {}
    for host_id in common_hosts:
        # Load data from filesystem
        dir_a = get_host_output_dir(job_a, host_id)
        dir_b = get_host_output_dir(job_b, host_id)
        
        # Global Status
        gs_a = read_json_safe(dir_a / "global_status.json") or {}
        gs_b = read_json_safe(dir_b / "global_status.json") or {}
        
        # Processlist
        pl_a = read_json_safe(dir_a / "processlist.json") or []
        pl_b = read_json_safe(dir_b / "processlist.json") or []
        
        # Config
        cfg_a = read_json_safe(dir_a / "config_vars.json") or {}
        cfg_b = read_json_safe(dir_b / "config_vars.json") or {}
        
        # InnoDB (raw text)
        innodb_a = read_file_safe(dir_a / "innodb.txt") or ""
        innodb_b = read_file_safe(dir_b / "innodb.txt") or ""
        
        # Buffer Pool
        bp_a = read_json_safe(dir_a / "buffer_pool.json") or {}
        bp_b = read_json_safe(dir_b / "buffer_pool.json") or {}
        
        # System info for regression detection (use config if available)
        system_info = {
            "cpu_cores": cfg_b.get("innodb_read_io_threads", 4),  # Approximate from config
        }
        
        # Detect regressions (raw)
        raw_regressions = detect_regressions(gs_a, gs_b, pl_a, pl_b, system_info)
        
        comparisons[host_id] = {
            "global_status": compare_global_status(gs_a, gs_b),
            "processlist": compare_processlist(pl_a, pl_b),
            "config": compare_config(cfg_a, cfg_b),
            "innodb": compare_innodb_text(innodb_a, innodb_b),
            "buffer_pool": compare_buffer_pool(bp_a, bp_b),
            "raw_regressions": raw_regressions,
            "gs_a": gs_a,
            "gs_b": gs_b,
        }
    
    # Collect all raw regressions for cross-host correlation
    all_host_regressions = [comparisons[h]["raw_regressions"] for h in common_hosts]
    
    # Apply refinement to each host's regressions
    for host_id in common_hosts:
        visible, suppressed = refine_regressions(
            comparisons[host_id]["raw_regressions"],
            comparisons[host_id]["gs_a"],
            comparisons[host_id]["gs_b"],
            all_host_regressions
        )
        comparisons[host_id]["visible_regressions"] = visible
        comparisons[host_id]["suppressed_regressions"] = suppressed
        # Clean up temp data
        del comparisons[host_id]["raw_regressions"]
        del comparisons[host_id]["gs_a"]
        del comparisons[host_id]["gs_b"]
    
    # Aggregate all regressions across hosts for summary
    visible_regressions = []
    suppressed_regressions = []
    
    for host_id in common_hosts:
        for reg in comparisons[host_id].get("visible_regressions", []):
            visible_regressions.append({
                **reg,
                "host_id": host_id,
                "host_label": host_labels.get(host_id, host_id),
            })
        for reg in comparisons[host_id].get("suppressed_regressions", []):
            suppressed_regressions.append({
                **reg,
                "host_id": host_id,
                "host_label": host_labels.get(host_id, host_id),
            })
    
    # Sort: severity first, then by root-cause hierarchy
    def sort_regressions(regs):
        severity_order = {"critical": 0, "warning": 1}
        regs.sort(key=lambda x: (severity_order.get(x["severity"], 2), x.get("category", "")))
    
    sort_regressions(visible_regressions)
    sort_regressions(suppressed_regressions)
    
    return templates.TemplateResponse("compare_result.html", {
        "request": request,
        "page_title": "Comparison Results",
        "job_a": job_a_obj,
        "job_b": job_b_obj,
        "common_hosts": common_hosts,
        "host_labels": host_labels,
        "comparisons": comparisons,
        "visible_regressions": visible_regressions,
        "suppressed_regressions": suppressed_regressions,
    })


# =============================================================================
# CRON JOBS ROUTES
# =============================================================================

@app.get("/crons", response_class=HTMLResponse)
async def list_crons(request: Request, db: Session = Depends(get_db)):
    """List all scheduled cron jobs."""
    crons = db.query(CronJob).order_by(CronJob.created_at.desc()).all()
    hosts = load_hosts()
    host_map = {h.id: h for h in hosts}
    
    # Enrich crons with host labels
    cron_data = []
    for cron in crons:
        host_ids = json.loads(cron.host_ids) if cron.host_ids else []
        host_labels = [host_map[hid].label if hid in host_map else hid for hid in host_ids]
        cron_data.append({
            "cron": cron,
            "host_labels": host_labels,
            "host_count": len(host_ids),
        })
    
    return templates.TemplateResponse("crons.html", {
        "request": request,
        "page_title": "Scheduled Jobs",
        "crons": cron_data,
        "hosts": hosts,
    })


@app.post("/crons/create")
async def create_cron(
    request: Request,
    name: str = Form(...),
    host_ids: List[str] = Form(...),
    interval_minutes: int = Form(60),
    collect_hot_tables: bool = Form(False),
    db: Session = Depends(get_db)
):
    """Create a new cron job."""
    from datetime import timedelta
    
    cron_id = str(uuid.uuid4())
    now = datetime.utcnow()
    
    cron = CronJob(
        id=cron_id,
        name=name,
        host_ids=json.dumps(host_ids),
        interval_minutes=interval_minutes,
        collect_hot_tables=collect_hot_tables,
        enabled=True,
        next_run_at=now + timedelta(minutes=interval_minutes),
        created_at=now,
    )
    
    db.add(cron)
    db.commit()
    
    logger.info(f"Created cron job: {name} (ID: {cron_id[:8]}) - every {interval_minutes} min")
    
    return RedirectResponse(url="/crons", status_code=302)


@app.post("/crons/{cron_id}/toggle")
async def toggle_cron(cron_id: str, db: Session = Depends(get_db)):
    """Enable or disable a cron job."""
    from datetime import timedelta
    
    cron = db.query(CronJob).filter(CronJob.id == cron_id).first()
    if not cron:
        return RedirectResponse(url="/crons", status_code=302)
    
    cron.enabled = not cron.enabled
    
    # If enabling, set next run time
    if cron.enabled:
        cron.next_run_at = datetime.utcnow() + timedelta(minutes=cron.interval_minutes)
    else:
        cron.next_run_at = None
    
    db.commit()
    
    status = "enabled" if cron.enabled else "disabled"
    logger.info(f"Cron '{cron.name}' (ID: {cron_id[:8]}) {status}")
    
    return RedirectResponse(url="/crons", status_code=302)


@app.post("/crons/{cron_id}/delete")
async def delete_cron(cron_id: str, db: Session = Depends(get_db)):
    """Delete a cron job."""
    cron = db.query(CronJob).filter(CronJob.id == cron_id).first()
    if cron:
        name = cron.name
        db.delete(cron)
        db.commit()
        logger.info(f"Deleted cron job: {name} (ID: {cron_id[:8]})")
    
    return RedirectResponse(url="/crons", status_code=302)


@app.post("/crons/{cron_id}/run-now")
async def run_cron_now(
    cron_id: str,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """Manually trigger a cron job to run immediately."""
    cron = db.query(CronJob).filter(CronJob.id == cron_id).first()
    if not cron:
        return RedirectResponse(url="/crons", status_code=302)
    
    # Parse host IDs
    host_ids = json.loads(cron.host_ids)
    
    # Create a new collection job
    job_id = str(uuid.uuid4())
    job_name = f"[Cron] {cron.name} (Manual)"
    
    new_job = Job(
        id=job_id,
        name=job_name,
        status=JobStatus.pending
    )
    db.add(new_job)
    
    for host_id in host_ids:
        job_host = JobHost(
            id=str(uuid.uuid4()),
            job_id=job_id,
            host_id=host_id,
            status=HostJobStatus.pending
        )
        db.add(job_host)
    
    # Update cron metadata
    cron.last_run_at = datetime.utcnow()
    cron.last_job_id = job_id
    cron.run_count = (cron.run_count or 0) + 1
    
    db.commit()
    
    # Run collection in background
    background_tasks.add_task(run_collection_job, job_id, host_ids, cron.collect_hot_tables)
    
    logger.info(f"Manually triggered cron '{cron.name}' - job {job_id[:8]}")
    
    return RedirectResponse(url=f"/jobs/{job_id}", status_code=302)


@app.post("/crons/{cron_id}/update")
async def update_cron(
    cron_id: str,
    name: str = Form(...),
    host_ids: List[str] = Form(...),
    interval_minutes: int = Form(60),
    collect_hot_tables: bool = Form(False),
    db: Session = Depends(get_db)
):
    """Update a cron job configuration."""
    from datetime import timedelta
    
    cron = db.query(CronJob).filter(CronJob.id == cron_id).first()
    if not cron:
        return RedirectResponse(url="/crons", status_code=302)
    
    cron.name = name
    cron.host_ids = json.dumps(host_ids)
    cron.interval_minutes = interval_minutes
    cron.collect_hot_tables = collect_hot_tables
    cron.updated_at = datetime.utcnow()
    
    # Update next run time if enabled
    if cron.enabled:
        cron.next_run_at = datetime.utcnow() + timedelta(minutes=interval_minutes)
    
    db.commit()
    
    logger.info(f"Updated cron job: {name} (ID: {cron_id[:8]})")
    
    return RedirectResponse(url="/crons", status_code=302)


# =============================================================================
# ABOUT PAGE
# =============================================================================

@app.get("/about", response_class=HTMLResponse)
async def about_page(request: Request):
    """About page with author info and project links."""
    return templates.TemplateResponse("about.html", {
        "request": request,
        "page_title": "About",
    })


@app.get("/version")
async def get_version():
    """Return the current application version."""
    return {"version": __version__}


# =============================================================================
# HOST MANAGEMENT ROUTES
# =============================================================================

@app.get("/hosts", response_class=HTMLResponse)
async def list_hosts_page(request: Request, db: Session = Depends(get_db)):
    """Host management page - list, add, edit, delete hosts."""
    # Get all groups
    groups = db.query(DBGroup).order_by(DBGroup.name).all()
    
    # Get all hosts from DB (including disabled), ordered by group then label
    db_hosts = db.query(DBHost).order_by(DBHost.group_id, DBHost.label).all()
    
    # Organize hosts by group
    grouped_hosts = {}
    ungrouped_hosts = []
    for host in db_hosts:
        if host.group_id:
            if host.group_id not in grouped_hosts:
                grouped_hosts[host.group_id] = []
            grouped_hosts[host.group_id].append(host)
        else:
            ungrouped_hosts.append(host)
    
    return templates.TemplateResponse("hosts.html", {
        "request": request,
        "page_title": "Manage Hosts",
        "hosts": db_hosts,
        "groups": groups,
        "grouped_hosts": grouped_hosts,
        "ungrouped_hosts": ungrouped_hosts,
    })


@app.post("/hosts/groups/create")
async def create_group(
    request: Request,
    name: str = Form(...),
    description: str = Form(""),
    color: str = Form("ocean"),
    db: Session = Depends(get_db)
):
    """Create a new host group."""
    import re
    
    # Auto-generate ID from name (slug format)
    group_id = re.sub(r'[^a-z0-9]+', '-', name.lower()).strip('-')
    
    # Ensure unique ID by appending number if needed
    base_id = group_id
    counter = 1
    while db.query(DBGroup).filter(DBGroup.id == group_id).first():
        group_id = f"{base_id}-{counter}"
        counter += 1
    
    new_group = DBGroup(
        id=group_id,
        name=name,
        description=description if description else None,
        color=color,
    )
    
    db.add(new_group)
    db.commit()
    
    logger.info(f"Created group: {name} (ID: {group_id})")
    
    return RedirectResponse(url="/hosts", status_code=302)


@app.post("/hosts/groups/{group_id}/update")
async def update_group(
    group_id: str,
    name: str = Form(...),
    description: str = Form(""),
    color: str = Form("ocean"),
    db: Session = Depends(get_db)
):
    """Update an existing group."""
    group = db.query(DBGroup).filter(DBGroup.id == group_id).first()
    if not group:
        return RedirectResponse(url="/hosts", status_code=302)
    
    group.name = name
    group.description = description if description else None
    group.color = color
    group.updated_at = datetime.utcnow()
    
    db.commit()
    
    logger.info(f"Updated group: {name}")
    
    return RedirectResponse(url="/hosts", status_code=302)


@app.post("/hosts/groups/{group_id}/delete")
async def delete_group(group_id: str, db: Session = Depends(get_db)):
    """Delete a group (hosts become ungrouped)."""
    group = db.query(DBGroup).filter(DBGroup.id == group_id).first()
    if group:
        # Ungroup all hosts in this group
        db.query(DBHost).filter(DBHost.group_id == group_id).update({"group_id": None})
        
        name = group.name
        db.delete(group)
        db.commit()
        logger.info(f"Deleted group: {name}")
    
    return RedirectResponse(url="/hosts", status_code=302)


@app.post("/hosts/create")
async def create_host(
    request: Request,
    label: str = Form(...),
    host: str = Form(...),
    port: int = Form(3306),
    user: str = Form(...),
    password: str = Form(...),
    group_id: str = Form(""),
    notes: str = Form(""),
    db: Session = Depends(get_db)
):
    """Create a new host."""
    import re
    
    # Auto-generate ID from label (slug format)
    host_id = re.sub(r'[^a-z0-9]+', '-', label.lower()).strip('-')
    
    # Ensure unique ID by appending number if needed
    base_id = host_id
    counter = 1
    while db.query(DBHost).filter(DBHost.id == host_id).first():
        host_id = f"{base_id}-{counter}"
        counter += 1
    
    new_host = DBHost(
        id=host_id,
        label=label,
        host=host,
        port=port,
        user=user,
        password=password,
        group_id=group_id if group_id else None,
        notes=notes if notes else None,
        enabled=True,
    )
    
    db.add(new_host)
    db.commit()
    
    logger.info(f"Created host: {label} ({host}:{port}) with ID '{host_id}'")
    
    return RedirectResponse(url="/hosts", status_code=302)


@app.post("/hosts/{host_id}/update")
async def update_host(
    host_id: str,
    label: str = Form(...),
    host: str = Form(...),
    port: int = Form(3306),
    user: str = Form(...),
    password: str = Form(""),
    group_id: str = Form(""),
    notes: str = Form(""),
    db: Session = Depends(get_db)
):
    """Update an existing host."""
    db_host = db.query(DBHost).filter(DBHost.id == host_id).first()
    if not db_host:
        return RedirectResponse(url="/hosts", status_code=302)
    
    db_host.label = label
    db_host.host = host
    db_host.port = port
    db_host.user = user
    # Only update password if provided (non-empty)
    if password:
        db_host.password = password
    db_host.group_id = group_id if group_id else None
    db_host.notes = notes if notes else None
    db_host.updated_at = datetime.utcnow()
    
    db.commit()
    
    logger.info(f"Updated host: {label} ({host}:{port})")
    
    return RedirectResponse(url="/hosts", status_code=302)


@app.post("/hosts/{host_id}/toggle")
async def toggle_host(host_id: str, db: Session = Depends(get_db)):
    """Enable or disable a host."""
    db_host = db.query(DBHost).filter(DBHost.id == host_id).first()
    if not db_host:
        return RedirectResponse(url="/hosts", status_code=302)
    
    db_host.enabled = not db_host.enabled
    db_host.updated_at = datetime.utcnow()
    db.commit()
    
    status = "enabled" if db_host.enabled else "disabled"
    logger.info(f"Host '{db_host.label}' {status}")
    
    return RedirectResponse(url="/hosts", status_code=302)


@app.post("/hosts/{host_id}/delete")
async def delete_host(host_id: str, db: Session = Depends(get_db)):
    """Delete a host."""
    db_host = db.query(DBHost).filter(DBHost.id == host_id).first()
    if db_host:
        label = db_host.label
        db.delete(db_host)
        db.commit()
        logger.info(f"Deleted host: {label}")
    
    return RedirectResponse(url="/hosts", status_code=302)


@app.post("/hosts/{host_id}/test")
async def test_host_connection(host_id: str, db: Session = Depends(get_db)):
    """Test connection to a host."""
    import subprocess
    import os
    
    db_host = db.query(DBHost).filter(DBHost.id == host_id).first()
    if not db_host:
        return {"success": False, "error": "Host not found"}
    
    # Simple connection test using mysql CLI
    env = os.environ.copy()
    env["MYSQL_PWD"] = db_host.password
    
    try:
        result = subprocess.run(
            [
                "mysql",
                f"-h{db_host.host}",
                f"-P{db_host.port}",
                f"-u{db_host.user}",
                "-e", "SELECT 1 AS test"
            ],
            capture_output=True,
            text=True,
            timeout=10,
            env=env
        )
        
        success = result.returncode == 0
        
        # Update test status in DB
        db_host.last_test_at = datetime.utcnow()
        db_host.last_test_success = success
        db.commit()
        
        if success:
            return {"success": True, "message": "Connection successful!"}
        else:
            error_msg = result.stderr.strip() if result.stderr else "Connection failed"
            # Clean up common MySQL warning about password on command line
            if "Using a password" in error_msg:
                error_msg = error_msg.split("\n")[-1] if "\n" in error_msg else error_msg
            return {"success": False, "error": error_msg}
            
    except subprocess.TimeoutExpired:
        db_host.last_test_at = datetime.utcnow()
        db_host.last_test_success = False
        db.commit()
        return {"success": False, "error": "Connection timed out (10s)"}
    except FileNotFoundError:
        return {"success": False, "error": "mysql CLI not found. Please install MySQL client."}
    except Exception as e:
        db_host.last_test_at = datetime.utcnow()
        db_host.last_test_success = False
        db.commit()
        return {"success": False, "error": str(e)}


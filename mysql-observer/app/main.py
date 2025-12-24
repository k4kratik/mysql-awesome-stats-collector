"""FastAPI main application for MySQL Observer."""

import logging
import sys
from datetime import datetime

from fastapi import FastAPI, Request, Depends, Form, BackgroundTasks, Query
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from sqlalchemy.orm import Session
from typing import List, Optional
from pathlib import Path
import json

from .db import init_db, get_db
from .models import Job, JobHost, JobStatus, HostJobStatus
from .utils import (
    load_hosts,
    get_host_by_id,
    generate_job_id,
    generate_job_host_id,
    get_host_output_dir,
    read_file_safe,
    read_json_safe,
)
from .collector import run_collection_job
from .parser import filter_processlist, get_key_metrics, parse_innodb_status_structured, CONFIG_VARIABLES_ALLOWLIST, evaluate_config_health

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("mysql-observer")

# Initialize FastAPI app
app = FastAPI(
    title="MySQL Observer",
    description="Internal DevOps tool for MySQL diagnostics",
    version="1.0.0"
)

# Setup templates and static files
BASE_DIR = Path(__file__).parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

# Mount static files (create directory if needed)
STATIC_DIR = BASE_DIR.parent / "static"
STATIC_DIR.mkdir(exist_ok=True)
(STATIC_DIR / "css").mkdir(exist_ok=True)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.on_event("startup")
async def startup_event():
    """Initialize database on startup."""
    logger.info("Starting MySQL Observer...")
    init_db()
    hosts = load_hosts()
    logger.info(f"Loaded {len(hosts)} host(s) from configuration:")
    for h in hosts:
        logger.info(f"  - {h.id}: {h.label} ({h.host}:{h.port}, user={h.user})")
    logger.info("MySQL Observer ready")


# =============================================================================
# HOME PAGE - Host Selection
# =============================================================================

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """Home page with host selection."""
    hosts = load_hosts()
    return templates.TemplateResponse("index.html", {
        "request": request,
        "hosts": hosts,
        "page_title": "MySQL Observer"
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
    
    if not selected_hosts:
        logger.warning("Job creation attempted with no hosts selected")
        # Redirect back with error
        hosts = load_hosts()
        return templates.TemplateResponse("index.html", {
            "request": request,
            "hosts": hosts,
            "page_title": "MySQL Observer",
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
    
    # Start background collection
    background_tasks.add_task(run_collection_job, job_id, list(selected_hosts))
    
    # Redirect to job detail page
    return RedirectResponse(url=f"/jobs/{job_id}", status_code=303)


# =============================================================================
# JOBS LIST
# =============================================================================

@app.get("/jobs", response_class=HTMLResponse)
async def list_jobs(request: Request, db: Session = Depends(get_db)):
    """List all jobs."""
    jobs = db.query(Job).order_by(Job.created_at.desc()).all()
    
    # Enrich with host counts
    jobs_data = []
    for job in jobs:
        host_count = len(job.hosts)
        completed_count = sum(1 for h in job.hosts if h.status == HostJobStatus.completed)
        failed_count = sum(1 for h in job.hosts if h.status == HostJobStatus.failed)
        
        jobs_data.append({
            "job": job,
            "host_count": host_count,
            "completed_count": completed_count,
            "failed_count": failed_count
        })
    
    return templates.TemplateResponse("jobs.html", {
        "request": request,
        "jobs": jobs_data,
        "page_title": "Jobs"
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
    
    # Enrich hosts with labels
    hosts_data = []
    all_hosts = load_hosts()
    hosts_map = {h.id: h for h in all_hosts}
    
    for job_host in job.hosts:
        host_config = hosts_map.get(job_host.host_id)
        hosts_data.append({
            "job_host": job_host,
            "label": host_config.label if host_config else job_host.host_id,
            "host": host_config.host if host_config else "unknown",
            "port": host_config.port if host_config else 0
        })
    
    return templates.TemplateResponse("job_detail.html", {
        "request": request,
        "job": job,
        "hosts": hosts_data,
        "page_title": f"Job {job_id[:8]}..."
    })


# =============================================================================
# HOST OUTPUT VIEW
# =============================================================================

@app.get("/jobs/{job_id}/hosts/{host_id}", response_class=HTMLResponse)
async def host_detail(
    request: Request,
    job_id: str,
    host_id: str,
    tab: str = "raw",
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
    
    # Get output directory
    output_dir = get_host_output_dir(job_id, host_id)
    
    # Load timing data (always available)
    timing_data = read_json_safe(output_dir / "timing.json") or {}
    
    # Load data based on tab
    raw_output = None
    innodb_output = None
    innodb_structured = None
    global_status = None
    processlist = []  # Always a list for JSON serialization
    key_metrics = None
    config_vars = None
    config_health = None
    
    if tab == "raw":
        raw_output = read_file_safe(output_dir / "raw.txt") or "No raw output available"
    elif tab == "innodb":
        innodb_output = read_file_safe(output_dir / "innodb.txt") or "No InnoDB output available"
        # Also load raw output for structured parsing
        raw_for_innodb = read_file_safe(output_dir / "raw.txt") or ""
        innodb_structured = parse_innodb_status_structured(raw_for_innodb)
    elif tab == "global":
        global_status = read_json_safe(output_dir / "global_status.json") or {}
        key_metrics = get_key_metrics(global_status)
    elif tab == "processlist":
        processlist = read_json_safe(output_dir / "processlist.json") or []
        # Apply filters
        processlist = filter_processlist(
            processlist,
            user=user_filter,
            state=state_filter,
            min_time=min_time_int,
            query=query_filter
        )
    elif tab == "config":
        # Load all config vars (needed for "Show All" toggle)
        config_vars = read_json_safe(output_dir / "config_vars.json") or {}
        # Load global_status for health evaluation (only important vars need this)
        global_status_for_health = read_json_safe(output_dir / "global_status.json") or {}
        # Only evaluate health for important variables (not all 500+)
        important_vars = {k: v for k, v in config_vars.items() if k in CONFIG_VARIABLES_ALLOWLIST}
        config_health = evaluate_config_health(important_vars, global_status_for_health)
    
    return templates.TemplateResponse("host_detail.html", {
        "request": request,
        "job": job,
        "job_host": job_host,
        "host_id": host_id,
        "host_label": host_label,
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
        "page_title": f"{host_label} Output"
    })


# =============================================================================
# API ENDPOINTS (for AJAX refreshing)
# =============================================================================

@app.get("/api/jobs/{job_id}/status")
async def get_job_status(job_id: str, db: Session = Depends(get_db)):
    """Get current job status (for polling)."""
    job = db.query(Job).filter(Job.id == job_id).first()
    
    if not job:
        return {"error": "Job not found"}
    
    hosts_status = []
    for job_host in job.hosts:
        hosts_status.append({
            "host_id": job_host.host_id,
            "status": job_host.status.value,
            "error_message": job_host.error_message
        })
    
    return {
        "job_id": job_id,
        "status": job.status.value,
        "hosts": hosts_status
    }


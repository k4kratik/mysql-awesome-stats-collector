"""Utility functions for the application."""

import os
import uuid
import yaml
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional
from dataclasses import dataclass

logger = logging.getLogger("masc.utils")


@dataclass
class HostConfig:
    """Host configuration data class."""
    id: str
    label: str
    host: str
    port: int
    user: str
    password: str
    enabled: bool = True
    notes: Optional[str] = None
    group_id: Optional[str] = None


# Project paths - use current working directory for data files
# This allows the package to work when installed via pip
# Users run the tool from their project directory where hosts.yaml and runs/ live
CWD = Path.cwd()

# Runs directory - stores job outputs in current working directory
# Can be overridden via MASC_RUNS_DIR environment variable
_runs_dir_env = os.environ.get("MASC_RUNS_DIR")
if _runs_dir_env:
    RUNS_DIR = Path(_runs_dir_env)
else:
    RUNS_DIR = CWD / "runs"

# Hosts file can be overridden via environment variable
# Usage: export MASC_HOSTS_FILE=/path/to/custom/hosts.yaml
_hosts_file_env = os.environ.get("MASC_HOSTS_FILE")
if _hosts_file_env:
    HOSTS_FILE = Path(_hosts_file_env)
else:
    HOSTS_FILE = CWD / "hosts.yaml"


def generate_job_id() -> str:
    """Generate a unique job ID."""
    return str(uuid.uuid4())


def generate_job_host_id() -> str:
    """Generate a unique job-host ID."""
    return str(uuid.uuid4())


def _load_hosts_from_yaml() -> List[HostConfig]:
    """Load hosts from YAML file (legacy support)."""
    if not HOSTS_FILE.exists():
        return []
    
    logger.debug(f"Loading hosts from YAML: {HOSTS_FILE}")
    with open(HOSTS_FILE, "r") as f:
        data = yaml.safe_load(f)
    
    hosts = []
    for h in data.get("hosts", []):
        hosts.append(HostConfig(
            id=h["id"],
            label=h["label"],
            host=h["host"],
            port=h["port"],
            user=h["user"],
            password=h["password"],
            enabled=h.get("enabled", True),
            notes=h.get("notes"),
            group_id=h.get("group_id"),
        ))
    return hosts


def _load_hosts_from_db() -> List[HostConfig]:
    """Load hosts from database."""
    try:
        from .db import get_db_context
        from .models import DBHost
        
        with get_db_context() as db:
            db_hosts = db.query(DBHost).filter(DBHost.enabled == True).all()
            hosts = []
            for h in db_hosts:
                hosts.append(HostConfig(
                    id=h.id,
                    label=h.label,
                    host=h.host,
                    port=h.port,
                    user=h.user,
                    password=h.password,
                    enabled=h.enabled,
                    notes=h.notes,
                ))
            return hosts
    except Exception as e:
        logger.warning(f"Could not load hosts from DB: {e}")
        return []


def load_hosts(include_disabled: bool = False) -> List[HostConfig]:
    """
    Load hosts from database, falling back to YAML if DB is empty.
    
    Priority:
    1. If DB has hosts -> use DB hosts
    2. If DB is empty but YAML exists -> import YAML to DB, then use DB
    3. If both empty -> return empty list
    
    Args:
        include_disabled: If True, include disabled hosts in the result
    """
    try:
        from .db import get_db_context
        from .models import DBHost
        
        with get_db_context() as db:
            # Check if DB has any hosts
            query = db.query(DBHost)
            if not include_disabled:
                query = query.filter(DBHost.enabled == True)
            db_hosts = query.all()
            
            if db_hosts:
                # DB has hosts - use them
                hosts = []
                for h in db_hosts:
                    hosts.append(HostConfig(
                        id=h.id,
                        label=h.label,
                        host=h.host,
                        port=h.port,
                        user=h.user,
                        password=h.password,
                        enabled=h.enabled,
                        notes=h.notes,
                        group_id=h.group_id,
                    ))
                logger.debug(f"Loaded {len(hosts)} hosts from database")
                return hosts
            
            # DB is empty - check YAML for migration
            yaml_hosts = _load_hosts_from_yaml()
            if yaml_hosts:
                logger.info(f"Migrating {len(yaml_hosts)} hosts from YAML to database...")
                for h in yaml_hosts:
                    db_host = DBHost(
                        id=h.id,
                        label=h.label,
                        host=h.host,
                        port=h.port,
                        user=h.user,
                        password=h.password,
                        enabled=h.enabled,
                        notes=h.notes,
                        group_id=h.group_id,
                    )
                    db.add(db_host)
                db.commit()
                logger.info(f"Successfully migrated {len(yaml_hosts)} hosts to database")
                
                # Return the migrated hosts
                if include_disabled:
                    return yaml_hosts
                return [h for h in yaml_hosts if h.enabled]
            
            return []
    except Exception as e:
        logger.warning(f"Error loading hosts from DB, falling back to YAML: {e}")
        yaml_hosts = _load_hosts_from_yaml()
        if include_disabled:
            return yaml_hosts
        return [h for h in yaml_hosts if h.enabled]


def load_all_hosts() -> List[HostConfig]:
    """Load ALL hosts including disabled ones (for admin UI)."""
    return load_hosts(include_disabled=True)


def get_host_by_id(host_id: str) -> Optional[HostConfig]:
    """Get a specific host by ID (including disabled hosts)."""
    hosts = load_hosts(include_disabled=True)
    for h in hosts:
        if h.id == host_id:
            return h
    return None


def get_job_dir(job_id: str) -> Path:
    """Get the directory path for a job."""
    return RUNS_DIR / f"job_{job_id}"


def get_host_output_dir(job_id: str, host_id: str) -> Path:
    """Get the directory path for a host's output within a job."""
    return get_job_dir(job_id) / host_id


def ensure_output_dir(job_id: str, host_id: str) -> Path:
    """Ensure the output directory exists and return its path."""
    output_dir = get_host_output_dir(job_id, host_id)
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir


def read_file_safe(file_path: Path) -> Optional[str]:
    """Safely read a file, returning None if it doesn't exist."""
    if file_path.exists():
        with open(file_path, "r") as f:
            return f.read()
    return None


def read_json_safe(file_path: Path) -> Optional[Any]:
    """Safely read a JSON file, returning None if it doesn't exist."""
    import json
    if file_path.exists():
        with open(file_path, "r") as f:
            return json.load(f)
    return None


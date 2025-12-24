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


def load_hosts() -> List[HostConfig]:
    """
    Load hosts from configuration file.
    
    The hosts file can be overridden via MYSQL_OBSERVER_HOSTS_FILE env var.
    Default: hosts.yaml in project root.
    """
    if not HOSTS_FILE.exists():
        logger.warning(f"Hosts file not found: {HOSTS_FILE}")
        return []
    
    logger.debug(f"Loading hosts from: {HOSTS_FILE}")
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
            password=h["password"]
        ))
    return hosts


def get_host_by_id(host_id: str) -> Optional[HostConfig]:
    """Get a specific host by ID."""
    hosts = load_hosts()
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


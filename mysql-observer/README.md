# MySQL Observer

Internal DevOps tool for collecting MySQL diagnostic information from multiple hosts.

## Features

- **Multi-Host Support**: Configure multiple MySQL hosts in `hosts.yaml`
- **Background Collection**: Jobs run asynchronously using FastAPI BackgroundTasks
- **Diagnostic Commands**: 
  - `SHOW ENGINE INNODB STATUS\G`
  - `SHOW GLOBAL STATUS\G`
  - `SHOW FULL PROCESSLIST\G`
- **Rich UI**: View raw output, parsed InnoDB status, charts, and filterable processlists
- **Local Storage**: All output stored on filesystem under `runs/`

## Quick Start

### 1. Install Dependencies (using uv)

```bash
cd mysql-observer

# Create virtual environment and install dependencies
uv venv
uv pip install -e .
```

Or with pip (alternative):

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

### 2. Configure Hosts

Edit `hosts.yaml` with your MySQL hosts:

```yaml
hosts:
  - id: "db-primary"
    label: "Primary Database"
    host: "localhost"
    port: 3306
    user: "diagnostic_user"
    password: "your_password"
```

### 3. Create MySQL Diagnostic User (Recommended)

```sql
CREATE USER 'diagnostic_user'@'%' IDENTIFIED BY 'your_password';
GRANT PROCESS, REPLICATION CLIENT ON *.* TO 'diagnostic_user'@'%';
FLUSH PRIVILEGES;
```

### 4. Run the Application

```bash
# Using uv (recommended)
uv run uvicorn app.main:app --reload --host 0.0.0.0 --port 8000

# Or with activated venv
source .venv/bin/activate
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

Visit: http://localhost:8000

## Project Structure

```
mysql-observer/
├── app/
│   ├── main.py          # FastAPI routes
│   ├── db.py            # SQLite setup
│   ├── models.py        # SQLAlchemy models
│   ├── collector.py     # MySQL CLI execution
│   ├── parser.py        # Output parsing
│   ├── utils.py         # Utility functions
│   └── templates/       # Jinja2 templates
├── hosts.yaml           # Host configuration
├── runs/                # Job output storage
├── observer.db          # SQLite database (auto-created)
└── requirements.txt
```

## Output Storage

Each job creates a directory structure:

```
runs/job_<uuid>/
└── <host_id>/
    ├── raw.txt              # Complete raw output
    ├── innodb.txt           # Parsed InnoDB sections
    ├── global_status.json   # Key-value status
    └── processlist.json     # Process list as JSON
```

## UI Pages

- `/` - Host selection (start new collection)
- `/jobs` - List all jobs
- `/jobs/{job_id}` - Job details with per-host status
- `/jobs/{job_id}/hosts/{host_id}` - Host output with tabs:
  - Raw Output
  - InnoDB Status
  - Global Status (with charts)
  - Processlist (filterable)

## Requirements

- Python 3.11+
- MySQL client (`mysql` CLI) installed on the server
- Network access to MySQL hosts

## Notes

- This is an internal tool - no authentication is implemented
- Passwords are stored in plain text in `hosts.yaml`
- Only read-only MySQL commands are executed
- No data is written to MySQL databases


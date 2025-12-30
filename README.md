# MySQL Awesome Stats Collector (MASC)

<div align="center">

![MASC](https://img.shields.io/badge/MySQL-Awesome_Stats_Collector-06b6d4?style=for-the-badge&logo=mysql&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.10+-3776ab?style=for-the-badge&logo=python&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-009688?style=for-the-badge&logo=fastapi&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-green?style=for-the-badge)

**A lightweight, self-hosted MySQL diagnostics tool for DevOps teams.**

Collect, visualize, and compare MySQL diagnostic data across multiple hosts — without agents, cloud dependencies, or complex setup.

[Features](#-features) • [Quick Start](#-quick-start) • [Configuration](#-configuration) • [Usage](#-usage) • [Screenshots](#-screenshots)

</div>

---

## ✨ Features

### 📊 **Collect Diagnostics**

Run diagnostic commands across multiple MySQL hosts in parallel:

- `SHOW ENGINE INNODB STATUS` — Buffer pool, transactions, locks, I/O
- `SHOW GLOBAL STATUS` — Server metrics and counters
- `SHOW FULL PROCESSLIST` — Active queries and connections
- `SHOW GLOBAL VARIABLES` — Configuration values

### 🔍 **Rich Visualization**

- **InnoDB Status** — Parsed sections with key metrics dashboard (hit rate, dirty pages, transactions)
- **Global Status** — Searchable table with human-readable formatting (GB, millions, etc.)
- **Processlist** — Filterable, sortable table with query search
- **Config Variables** — Important settings with health indicators (🟢🟡🔴)

### ⚡ **Compare Jobs**

Compare two collection runs side-by-side:

- Numeric counter diffs (threads, locks, temp tables)
- Buffer pool comparison (size, used, hit ratio)
- Processlist summary changes
- Configuration changes highlighted
- InnoDB text diff with +/- lines

### ⏰ **Scheduled Collections (Crons)**

Automate diagnostic collection:

- Create scheduled jobs at specified intervals (15m, 30m, 1h, 6h, 24h, custom)
- Select which hosts to include per schedule
- Pause/resume schedules anytime
- Run schedules manually on-demand
- Track run history and next scheduled time

### 📊 **Connection Analysis**

Detailed connection breakdown:

- **By User** — Connections grouped by MySQL user
- **By IP** — Connections grouped by source IP address
- **By IP + User** — Combined view with sortable columns
- Active (Query), Sleeping, Other counts per group
- Click to filter processlist by user

### 🎯 **DevOps-Friendly**

- **No agents** — Uses MySQL CLI via subprocess
- **No cloud** — 100% self-hosted, runs anywhere
- **No database writes** — Read-only MySQL access
- **Job-based** — Track collections over time with optional naming
- **Parallel execution** — Fast collection across hosts

---

## 🚀 Quick Start

### Prerequisites

- Python 3.10+
- MySQL client (`mysql` CLI) installed
- Read-only MySQL user on target hosts

### Installation

#### Option 1: Install from PyPI (Recommended)

```bash
# Install the package
pip install mysql-awesome-stats-collector

# Create a project directory
mkdir my-masc-project && cd my-masc-project

# Create hosts configuration
cat > hosts.yaml << 'EOF'
hosts:
  - id: "primary"
    label: "Primary DB"
    host: "your-db-host.example.com"
    port: 3306
    user: "observer"
    password: "your-password"
EOF

# Run the server
masc --port 8000
```

#### Option 2: Install from Source

```bash
# Clone the repository
git clone https://github.com/k4kratik/mysql-awesome-stats-collector.git
cd mysql-awesome-stats-collector

# Install uv (if not already installed)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Create virtual environment and install dependencies
uv venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
uv pip install -e .

# Configure your hosts
cp hosts.yaml.example hosts.yaml
# Edit hosts.yaml with your MySQL hosts

# Run the server
masc --host 0.0.0.0 --port 8000
```

Open <http://localhost:8000> in your browser.

### CLI Usage

```bash
# Start server on default port (8000)
masc

# Start on a custom port
masc --port 9000

# Listen on all interfaces
masc --host 0.0.0.0

# Enable auto-reload for development
masc --reload

# Use a custom hosts file
masc --hosts-file /path/to/hosts.yaml

# Show help
masc --help

# Show version
masc --version
```

### Environment Variables

| Variable          | Description               | Default        |
| ----------------- | ------------------------- | -------------- |
| `MASC_HOSTS_FILE` | Path to hosts.yaml        | `./hosts.yaml` |
| `MASC_RUNS_DIR`   | Directory for job outputs | `./runs`       |

### Running as a Daemon (Production)

For production use on a remote server:

```bash
# Using nohup (simple)
nohup masc --host 0.0.0.0 --port 8000 > masc.log 2>&1 &

# Using screen (interactive)
screen -S masc
masc --host 0.0.0.0 --port 8000
# Press Ctrl+A, D to detach
# screen -r masc to reattach

# Using systemd (recommended for production)
# Create /etc/systemd/system/masc.service:
# [Unit]
# Description=MySQL Awesome Stats Collector
# After=network.target
#
# [Service]
# Type=simple
# User=your-user
# WorkingDirectory=/path/to/masc
# ExecStart=/path/to/venv/bin/masc --host 0.0.0.0 --port 8000
# Restart=always
#
# [Install]
# WantedBy=multi-user.target

sudo systemctl daemon-reload
sudo systemctl enable masc
sudo systemctl start masc
```

---

## ⚙️ Configuration

### hosts.yaml

Define your MySQL hosts in `hosts.yaml`:

```yaml
hosts:
  - id: "primary"
    label: "Production Primary"
    host: "db-primary.example.com"
    port: 3306
    user: "observer"
    password: "your-password"

  - id: "replica-1"
    label: "Read Replica 1"
    host: "db-replica-1.example.com"
    port: 3306
    user: "observer"
    password: "your-password"

  - id: "replica-2"
    label: "Read Replica 2"
    host: "db-replica-2.example.com"
    port: 3306
    user: "observer"
    password: "your-password"
```

### MySQL User Permissions

Create a read-only user for MASC:

```sql
-- Create the monitoring user
CREATE USER 'masc_monitor'@'%' IDENTIFIED BY 'secure-password';

-- For SHOW ENGINE INNODB STATUS, SHOW PROCESSLIST, SHOW GLOBAL STATUS/VARIABLES
GRANT PROCESS ON *.* TO 'masc_monitor'@'%';

-- For SHOW REPLICA STATUS / SHOW SLAVE STATUS
GRANT REPLICATION CLIENT ON *.* TO 'masc_monitor'@'%';

-- For reading information_schema tables (hot tables, table sizes)
GRANT SELECT ON information_schema.* TO 'masc_monitor'@'%';

-- For performance_schema access (optional, for hot tables feature)
GRANT SELECT ON performance_schema.* TO 'masc_monitor'@'%';

FLUSH PRIVILEGES;
```

| Privilege | Purpose |
|-----------|---------|
| `PROCESS` | InnoDB status, processlist, global status/variables |
| `REPLICATION CLIENT` | Replica/slave status |
| `SELECT on information_schema` | Table stats, hot tables analysis |
| `SELECT on performance_schema` | Hot tables feature (optional) |

> ⚠️ **Security Note**: Never use a user with write permissions. MASC only needs read access.

---

## 📖 Usage

### 1. Run a Collection

1. Go to the **Home** page
2. Optionally enter a **Job Name** (e.g., "Before deployment")
3. Select one or more hosts
4. Click **Run Collection**

The job runs in the background. You'll be redirected to the job detail page.

### 2. View Results

Each host shows tabs for:

- **Raw Output** — Complete command output with copy/download buttons
- **InnoDB Status** — Parsed sections with metrics dashboard
- **Global Status** — Searchable metrics with charts
- **Processlist** — Filterable query list with connection summary
- **Config** — Important variables with health indicators
- **Replication** — Replica lag and master status
- **Health** — InnoDB health analysis (deadlocks, waits)

### 3. Compare Jobs

1. Go to **Compare** in the navigation
2. Select **Job A** (baseline) and **Job B** (after)
3. Click **Compare**

See what changed between runs:

- 🟢 Green = Decrease (usually good)
- 🔴 Red = Increase (watch out)
- Changed config values highlighted

---

## 📁 Project Structure

```
mysql-awesome-stats-collector/
├── app/
│   ├── main.py          # FastAPI routes
│   ├── cli.py           # CLI entry point
│   ├── db.py            # SQLite setup
│   ├── models.py        # SQLAlchemy models (Job, JobHost, CronJob)
│   ├── collector.py     # MySQL command execution
│   ├── parser.py        # Output parsing
│   ├── compare.py       # Job comparison logic
│   ├── scheduler.py     # Cron job scheduler
│   ├── utils.py         # Helper functions
│   └── templates/       # Jinja2 HTML templates
├── docs/
│   └── PUBLISHING.md    # PyPI publishing guide
├── runs/                # Job output storage (gitignored)
├── hosts.yaml           # Host configuration (gitignored)
├── hosts.yaml.example   # Example configuration
├── observer.db          # SQLite metadata (gitignored)
├── pyproject.toml       # Dependencies & package config
├── CHANGELOG.txt        # Version history
├── LICENSE              # MIT License
└── README.md
```

### Data Storage

- **SQLite** (`observer.db`) — Job metadata, cron schedules (IDs, timestamps, status)
- **Filesystem** (`runs/`) — All command outputs stored as files:

  ```
  runs/job_<uuid>/<host_id>/
  ├── raw.txt              # Full command output
  ├── innodb.txt           # Extracted InnoDB status
  ├── global_status.json   # Parsed key/value pairs
  ├── processlist.json     # Parsed process list
  ├── config_vars.json     # Parsed variables
  └── timing.json          # Per-command timing
  ```

---

## 📸 Screenshots

### Homepage

Select hosts and run diagnostics in parallel.

![MASC Homepage](screenshots/masc-home.png)

### Job Detail - Host Overview

### Single Host Homepage

![Single Host](screenshots/masc-8.png)

**View all hosts in a job with status and timing.**

![hosts](screenshots/masc-10.png)

#### Hot Tables

![Job Detail](screenshots/masc-1.png)

### InnoDB Status

Parsed InnoDB sections with key metrics dashboard.

![InnoDBStatus](screenshots/masc-7.png)
![InnoDB Status](screenshots/masc-2.png)

### Global Status

Searchable metrics with charts and human-readable formatting.

![Global Status](screenshots/masc-3.png)

### Processlist

Filterable, sortable active queries with pagination.

![Processlist](screenshots/masc-4.png)

### Config Variables

Important settings with health indicators (🟢🟡🔴).

![Config Variables](screenshots/masc-5.png)

### Replication Status

Replica lag monitoring with master comparison.

![Replica](screenshots/masc-6.png)
![Replica](screenshots/masc-9.png)

### Job Comparison

Compare two runs side-by-side with delta highlighting.

todo

---

## 🛠️ Tech Stack

| Component       | Technology                                    |
| --------------- | --------------------------------------------- |
| Backend         | [FastAPI](https://fastapi.tiangolo.com/)      |
| Database        | SQLite + SQLAlchemy                           |
| Scheduler       | [APScheduler](https://apscheduler.readthedocs.io/) |
| Templates       | Jinja2                                        |
| Styling         | [TailwindCSS](https://tailwindcss.com/) (CDN) |
| Charts          | [Chart.js](https://www.chartjs.org/)          |
| Interactivity   | [Alpine.js](https://alpinejs.dev/)            |
| Package Manager | [uv](https://github.com/astral-sh/uv)         |

---

## 🔒 Security Considerations

- **Passwords** are stored in plain text in `hosts.yaml` — keep this file secure
- **Never commit** `hosts.yaml` to version control (it's gitignored by default)
- Use a **read-only MySQL user** with minimal permissions
- Passwords are passed via `MYSQL_PWD` environment variable (not command line)
- No credentials are logged or exposed in the UI

---

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

---

## 📋 Roadmap

- [x] Environment variable support for hosts file
- [x] Replication monitoring (replica lag, master comparison)
- [x] PyPI package (`pip install mysql-awesome-stats-collector`)
- [x] Scheduled collections (Cron jobs)
- [x] Buffer pool comparison between jobs
- [x] Job re-run feature
- [x] Connection summary by User/IP
- [x] Hot tables analysis
- [ ] Environment variable support for passwords
- [ ] Export comparison reports (PDF/HTML)
- [ ] Alerting thresholds
- [ ] Query analysis tools
- [ ] Docker support

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## 🙏 Acknowledgments

Built for DevOps teams who need quick MySQL diagnostics without the overhead of complex monitoring solutions.

---

<div align="center">

**[⬆ Back to Top](#mysql-awesome-stats-collector-masc)**

Made with ❤️ for the MySQL community

</div>

# MySQL Observer

<div align="center">

![MySQL Observer](https://img.shields.io/badge/MySQL-Observer-06b6d4?style=for-the-badge&logo=mysql&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.11+-3776ab?style=for-the-badge&logo=python&logoColor=white)
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
- Processlist summary changes
- Configuration changes highlighted
- InnoDB text diff with +/- lines

### 🎯 **DevOps-Friendly**
- **No agents** — Uses MySQL CLI via subprocess
- **No cloud** — 100% self-hosted, runs anywhere
- **No database writes** — Read-only MySQL access
- **Job-based** — Track collections over time with optional naming
- **Parallel execution** — Fast collection across hosts

---

## 🚀 Quick Start

### Prerequisites
- Python 3.11+
- MySQL client (`mysql` CLI) installed
- Read-only MySQL user on target hosts

### Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/mysql-observer.git
cd mysql-observer

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
uv run uvicorn app.main:app --host 0.0.0.0 --port 8000
```

Open http://localhost:8000 in your browser.

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

Create a read-only user for MySQL Observer:

```sql
CREATE USER 'observer'@'%' IDENTIFIED BY 'secure-password';
GRANT PROCESS, REPLICATION CLIENT ON *.* TO 'observer'@'%';
FLUSH PRIVILEGES;
```

> ⚠️ **Security Note**: Never use a user with write permissions. The observer only needs read access.

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
- **Raw Output** — Complete command output with copy button
- **InnoDB Status** — Parsed sections with metrics dashboard
- **Global Status** — Searchable metrics with charts
- **Processlist** — Filterable query list
- **Config** — Important variables with health indicators

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
mysql-observer/
├── app/
│   ├── main.py          # FastAPI routes
│   ├── db.py            # SQLite setup
│   ├── models.py        # SQLAlchemy models
│   ├── collector.py     # MySQL command execution
│   ├── parser.py        # Output parsing
│   ├── compare.py       # Job comparison logic
│   ├── utils.py         # Helper functions
│   └── templates/       # Jinja2 HTML templates
├── runs/                # Job output storage (gitignored)
├── hosts.yaml           # Host configuration (gitignored)
├── observer.db          # SQLite metadata (gitignored)
├── pyproject.toml       # Dependencies
└── README.md
```

### Data Storage

- **SQLite** (`observer.db`) — Job metadata only (IDs, timestamps, status)
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

## 🛠️ Tech Stack

| Component | Technology |
|-----------|------------|
| Backend | [FastAPI](https://fastapi.tiangolo.com/) |
| Database | SQLite + SQLAlchemy |
| Templates | Jinja2 |
| Styling | [TailwindCSS](https://tailwindcss.com/) (CDN) |
| Charts | [Chart.js](https://www.chartjs.org/) |
| Interactivity | [Alpine.js](https://alpinejs.dev/) |
| Package Manager | [uv](https://github.com/astral-sh/uv) |

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

- [ ] Environment variable support for passwords
- [ ] Export comparison reports (PDF/HTML)
- [ ] Scheduled collections
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

**[⬆ Back to Top](#mysql-observer)**

Made with ❤️ for the MySQL community

</div>

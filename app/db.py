"""Database connection and session management."""

import logging
from pathlib import Path
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.orm import sessionmaker, Session
from contextlib import contextmanager
from typing import Generator

from .models import Base

logger = logging.getLogger("masc.db")

# Database file path (relative to project root)
DB_PATH = Path(__file__).parent.parent / "observer.db"
DATABASE_URL = f"sqlite:///{DB_PATH}"

# Create engine with check_same_thread=False for SQLite
engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False},
    echo=False
)

# Session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def _run_migrations() -> None:
    """Run database migrations for schema changes."""
    inspector = inspect(engine)
    existing_tables = inspector.get_table_names()
    
    with engine.connect() as conn:
        # Migration 1: Create db_groups table if it doesn't exist
        if "db_groups" not in existing_tables:
            logger.info("Migration: Creating db_groups table...")
            conn.execute(text("""
                CREATE TABLE db_groups (
                    id VARCHAR PRIMARY KEY,
                    name VARCHAR NOT NULL,
                    description TEXT,
                    color VARCHAR DEFAULT 'ocean',
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """))
            conn.commit()
            logger.info("Migration: db_groups table created")
        
        # Migration 2: Add group_id column to hosts table if it doesn't exist
        if "hosts" in existing_tables:
            columns = [col["name"] for col in inspector.get_columns("hosts")]
            if "group_id" not in columns:
                logger.info("Migration: Adding group_id column to hosts table...")
                conn.execute(text("""
                    ALTER TABLE hosts ADD COLUMN group_id VARCHAR REFERENCES db_groups(id)
                """))
                conn.commit()
                conn.commit()
                logger.info("Migration: group_id column added to hosts table")

        # Migration 3: Add mysql_version column to job_hosts table if it doesn't exist
        if "job_hosts" in existing_tables:
            columns = [col["name"] for col in inspector.get_columns("job_hosts")]
            if "mysql_version" not in columns:
                logger.info("Migration: Adding mysql_version column to job_hosts table...")
                conn.execute(text("""
                    ALTER TABLE job_hosts ADD COLUMN mysql_version VARCHAR
                """))
                conn.commit()
                logger.info("Migration: mysql_version column added to job_hosts table")


def init_db() -> None:
    """Initialize database and create all tables."""
    # First create any new tables
    Base.metadata.create_all(bind=engine)
    
    # Then run migrations for schema changes to existing tables
    _run_migrations()


def get_db() -> Generator[Session, None, None]:
    """Dependency for getting database session."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@contextmanager
def get_db_context() -> Generator[Session, None, None]:
    """Context manager for database session (for background tasks)."""
    db = SessionLocal()
    try:
        yield db
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


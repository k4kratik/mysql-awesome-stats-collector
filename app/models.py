"""SQLAlchemy models for job metadata storage."""

from datetime import datetime
from sqlalchemy import Column, String, DateTime, ForeignKey, Enum as SQLEnum, Boolean, Integer, Text
from sqlalchemy.orm import relationship, declarative_base
import enum

Base = declarative_base()


class JobStatus(enum.Enum):
    """Job execution status."""
    pending = "pending"
    running = "running"
    completed = "completed"
    failed = "failed"


class HostJobStatus(enum.Enum):
    """Per-host job execution status."""
    pending = "pending"
    running = "running"
    completed = "completed"
    failed = "failed"


class Job(Base):
    """Job metadata model."""
    __tablename__ = "jobs"

    id = Column(String, primary_key=True)
    name = Column(String, nullable=True)  # Optional job name/label
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    status = Column(SQLEnum(JobStatus), default=JobStatus.pending, nullable=False, index=True)

    # Relationship to job hosts - eager load for performance
    hosts = relationship("JobHost", back_populates="job", cascade="all, delete-orphan", lazy="joined")


class JobHost(Base):
    """Per-host job execution metadata."""
    __tablename__ = "job_hosts"

    id = Column(String, primary_key=True)
    job_id = Column(String, ForeignKey("jobs.id"), nullable=False, index=True)
    host_id = Column(String, nullable=False, index=True)
    status = Column(SQLEnum(HostJobStatus), default=HostJobStatus.pending, nullable=False)
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    mysql_version = Column(String, nullable=True)
    error_message = Column(String, nullable=True)

    # Relationship to parent job
    job = relationship("Job", back_populates="hosts")


class CronJob(Base):
    """Scheduled collection job configuration."""
    __tablename__ = "cron_jobs"

    id = Column(String, primary_key=True)
    name = Column(String, nullable=False)  # Display name for the cron
    host_ids = Column(Text, nullable=False)  # JSON array of host IDs
    interval_minutes = Column(Integer, nullable=False, default=60)  # Run every X minutes
    collect_hot_tables = Column(Boolean, default=False)  # Whether to collect hot tables
    enabled = Column(Boolean, default=True)  # Whether cron is active
    last_run_at = Column(DateTime, nullable=True)  # Last execution time
    last_job_id = Column(String, nullable=True)  # ID of last created job
    next_run_at = Column(DateTime, nullable=True)  # Scheduled next run
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    run_count = Column(Integer, default=0)  # Total number of runs


class DBGroup(Base):
    """Database group for organizing hosts."""
    __tablename__ = "db_groups"

    id = Column(String, primary_key=True)  # Unique identifier (slug-like)
    name = Column(String, nullable=False)  # Display name
    description = Column(Text, nullable=True)  # Optional description
    color = Column(String, nullable=True, default="ocean")  # Color theme for UI
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    # Relationship to hosts
    hosts = relationship("DBHost", back_populates="group", lazy="joined")


class DBHost(Base):
    """Database-stored MySQL host configuration."""
    __tablename__ = "hosts"

    id = Column(String, primary_key=True)  # Unique identifier (slug-like)
    label = Column(String, nullable=False)  # Display name
    host = Column(String, nullable=False)  # Hostname or IP
    port = Column(Integer, nullable=False, default=3306)
    user = Column(String, nullable=False)  # MySQL username
    password = Column(String, nullable=False)  # MySQL password (stored as-is for now)
    group_id = Column(String, ForeignKey("db_groups.id"), nullable=True)  # Optional group
    enabled = Column(Boolean, default=True)  # Whether host is active
    notes = Column(Text, nullable=True)  # Optional notes/description
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    last_test_at = Column(DateTime, nullable=True)  # Last connection test time
    last_test_success = Column(Boolean, nullable=True)  # Result of last connection test

    # Relationship to group
    group = relationship("DBGroup", back_populates="hosts")


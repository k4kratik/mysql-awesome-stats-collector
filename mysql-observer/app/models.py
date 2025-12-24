"""SQLAlchemy models for job metadata storage."""

from datetime import datetime
from sqlalchemy import Column, String, DateTime, ForeignKey, Enum as SQLEnum
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
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    status = Column(SQLEnum(JobStatus), default=JobStatus.pending, nullable=False)

    # Relationship to job hosts
    hosts = relationship("JobHost", back_populates="job", cascade="all, delete-orphan")


class JobHost(Base):
    """Per-host job execution metadata."""
    __tablename__ = "job_hosts"

    id = Column(String, primary_key=True)
    job_id = Column(String, ForeignKey("jobs.id"), nullable=False)
    host_id = Column(String, nullable=False)
    status = Column(SQLEnum(HostJobStatus), default=HostJobStatus.pending, nullable=False)
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    error_message = Column(String, nullable=True)

    # Relationship to parent job
    job = relationship("Job", back_populates="hosts")


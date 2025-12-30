"""Background scheduler for cron job execution."""

import json
import logging
import threading
import time
import uuid
from datetime import datetime, timedelta
from typing import Optional

from .db import get_db_context
from .models import CronJob, Job, JobHost, JobStatus, HostJobStatus
from .collector import run_collection_job

logger = logging.getLogger(__name__)

# Global scheduler instance
_scheduler: Optional['CronScheduler'] = None


class CronScheduler:
    """Background scheduler that runs cron jobs at specified intervals."""
    
    def __init__(self):
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._check_interval = 30  # Check every 30 seconds
    
    def start(self):
        """Start the background scheduler."""
        if self._running:
            logger.warning("Scheduler is already running")
            return
        
        self._running = True
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        logger.info("Cron scheduler started")
        
        # Initialize next_run_at for all enabled crons on startup
        self._initialize_crons()
    
    def stop(self):
        """Stop the background scheduler."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)
        logger.info("Cron scheduler stopped")
    
    def _initialize_crons(self):
        """Set next_run_at for enabled crons that don't have it set."""
        try:
            with get_db_context() as db:
                crons = db.query(CronJob).filter(
                    CronJob.enabled == True,
                    CronJob.next_run_at == None
                ).all()
                
                for cron in crons:
                    cron.next_run_at = datetime.utcnow() + timedelta(minutes=cron.interval_minutes)
                    logger.info(f"Initialized cron '{cron.name}' next run at {cron.next_run_at}")
                
                db.commit()
        except Exception as e:
            logger.exception(f"Error initializing crons: {e}")
    
    def _run_loop(self):
        """Main scheduler loop."""
        logger.info("Scheduler loop started")
        
        while self._running:
            try:
                self._check_and_run_crons()
            except Exception as e:
                logger.exception(f"Error in scheduler loop: {e}")
            
            # Sleep in small increments so we can stop quickly
            for _ in range(self._check_interval):
                if not self._running:
                    break
                time.sleep(1)
    
    def _check_and_run_crons(self):
        """Check for crons that need to run and execute them."""
        now = datetime.utcnow()
        
        with get_db_context() as db:
            # Find enabled crons whose next_run_at is in the past
            due_crons = db.query(CronJob).filter(
                CronJob.enabled == True,
                CronJob.next_run_at <= now
            ).all()
            
            for cron in due_crons:
                logger.info(f"Running cron job: {cron.name} (ID: {cron.id[:8]})")
                
                try:
                    # Parse host IDs
                    host_ids = json.loads(cron.host_ids)
                    
                    # Create a new collection job
                    job_id = str(uuid.uuid4())
                    job_name = f"[Cron] {cron.name}"
                    
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
                    cron.last_run_at = now
                    cron.last_job_id = job_id
                    cron.next_run_at = now + timedelta(minutes=cron.interval_minutes)
                    cron.run_count = (cron.run_count or 0) + 1
                    
                    db.commit()
                    
                    # Run collection in background thread
                    collect_hot_tables = cron.collect_hot_tables
                    threading.Thread(
                        target=run_collection_job,
                        args=(job_id, host_ids, collect_hot_tables),
                        daemon=True
                    ).start()
                    
                    logger.info(f"Cron '{cron.name}' started job {job_id[:8]}, next run at {cron.next_run_at}")
                    
                except Exception as e:
                    logger.exception(f"Error running cron '{cron.name}': {e}")
                    # Still update next_run_at to avoid repeated failures
                    cron.next_run_at = now + timedelta(minutes=cron.interval_minutes)
                    db.commit()


def get_scheduler() -> CronScheduler:
    """Get or create the global scheduler instance."""
    global _scheduler
    if _scheduler is None:
        _scheduler = CronScheduler()
    return _scheduler


def start_scheduler():
    """Start the global scheduler."""
    scheduler = get_scheduler()
    scheduler.start()


def stop_scheduler():
    """Stop the global scheduler."""
    global _scheduler
    if _scheduler:
        _scheduler.stop()
        _scheduler = None


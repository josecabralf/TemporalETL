"""
Memory monitoring utilities for streaming ETL pipeline.
"""

import psutil
import logging
from typing import Dict, Any, Optional
from dataclasses import dataclass, field
from datetime import datetime


logger = logging.getLogger(__name__)


@dataclass
class MemorySnapshot:
    """Snapshot of memory usage at a point in time."""
    timestamp: datetime
    rss_mb: float  # Resident Set Size in MB
    vms_mb: float  # Virtual Memory Size in MB
    percent: float  # Memory percentage
    available_mb: float  # Available system memory in MB
    
    
@dataclass
class MemoryMonitor:
    """Monitor memory usage during ETL operations."""
    process_id: Optional[int] = None
    snapshots: list = field(default_factory=list)
    peak_rss_mb: float = 0.0
    peak_vms_mb: float = 0.0
    
    def __post_init__(self):
        if self.process_id is None:
            self.process_id = psutil.Process().pid
    
    def take_snapshot(self, label: str = "") -> Optional[MemorySnapshot]:
        """Take a memory snapshot."""
        try:
            process = psutil.Process(self.process_id)
            memory_info = process.memory_info()
            memory_percent = process.memory_percent()
            system_memory = psutil.virtual_memory()
            
            snapshot = MemorySnapshot(
                timestamp=datetime.now(),
                rss_mb=memory_info.rss / 1024 / 1024,
                vms_mb=memory_info.vms / 1024 / 1024,
                percent=memory_percent,
                available_mb=system_memory.available / 1024 / 1024
            )
            
            # Update peaks
            self.peak_rss_mb = max(self.peak_rss_mb, snapshot.rss_mb)
            self.peak_vms_mb = max(self.peak_vms_mb, snapshot.vms_mb)
            
            self.snapshots.append(snapshot)
            
            if label:
                logger.info(f"Memory snapshot ({label}): RSS={snapshot.rss_mb:.1f}MB, "
                           f"VMS={snapshot.vms_mb:.1f}MB, Percent={snapshot.percent:.1f}%")
            
            return snapshot
            
        except Exception as e:
            logger.warning(f"Failed to take memory snapshot: {e}")
            return None
    
    def check_threshold(self, threshold_mb: float) -> bool:
        """Check if memory usage exceeds threshold."""
        if not self.snapshots:
            return False
        
        latest = self.snapshots[-1]
        return latest.rss_mb > threshold_mb
    
    def get_memory_stats(self) -> Dict[str, Any]:
        """Get memory usage statistics."""
        if not self.snapshots:
            return {}
        
        rss_values = [s.rss_mb for s in self.snapshots]
        vms_values = [s.vms_mb for s in self.snapshots]
        
        return {
            "peak_rss_mb": self.peak_rss_mb,
            "peak_vms_mb": self.peak_vms_mb,
            "current_rss_mb": rss_values[-1],
            "current_vms_mb": vms_values[-1],
            "avg_rss_mb": sum(rss_values) / len(rss_values),
            "avg_vms_mb": sum(vms_values) / len(vms_values),
            "snapshots_taken": len(self.snapshots),
            "duration_seconds": (self.snapshots[-1].timestamp - self.snapshots[0].timestamp).total_seconds()
        }
    
    def log_final_stats(self):
        """Log final memory statistics."""
        stats = self.get_memory_stats()
        if stats:
            logger.info(f"Memory usage summary: Peak RSS={stats['peak_rss_mb']:.1f}MB, "
                       f"Avg RSS={stats['avg_rss_mb']:.1f}MB, "
                       f"Duration={stats['duration_seconds']:.1f}s, "
                       f"Snapshots={stats['snapshots_taken']}")


def monitor_memory_usage(func):
    """Decorator to monitor memory usage of a function."""
    def wrapper(*args, **kwargs):
        monitor = MemoryMonitor()
        monitor.take_snapshot("start")
        
        try:
            result = func(*args, **kwargs)
            monitor.take_snapshot("end")
            monitor.log_final_stats()
            return result
        except Exception as e:
            monitor.take_snapshot("error")
            monitor.log_final_stats()
            raise
    
    return wrapper


async def monitor_memory_usage_async(func):
    """Async decorator to monitor memory usage of an async function."""
    async def wrapper(*args, **kwargs):
        monitor = MemoryMonitor()
        monitor.take_snapshot("start")
        
        try:
            result = await func(*args, **kwargs)
            monitor.take_snapshot("end")
            monitor.log_final_stats()
            return result
        except Exception as e:
            monitor.take_snapshot("error")
            monitor.log_final_stats()
            raise
    
    return wrapper

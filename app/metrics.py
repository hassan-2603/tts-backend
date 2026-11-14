"""Performance monitoring, system health, and operational metrics."""
import os
import time
import psutil
from datetime import datetime, timezone
from typing import Dict, Any, List
from sqlalchemy import text
from app.database import AsyncSessionLocal

_METRICS_START_TIME = time.time()


class MetricsCollector:
    def __init__(self):
        self.incoming_events_total = 0
        self.duplicate_events_total = 0
        self.event_processing_failures = 0
        self.total_alerts_generated = 0
        self.total_risk_evaluations = 0
        self.risk_evaluation_latencies: List[float] = []
        self.db_query_latencies: List[float] = []
        self.api_response_latencies: List[float] = []

    def record_incoming_event(self):
        self.incoming_events_total += 1

    def record_duplicate_event(self):
        self.duplicate_events_total += 1

    def record_event_failure(self):
        self.event_processing_failures += 1

    def record_risk_latency(self, duration_ms: float):
        self.total_risk_evaluations += 1
        self.risk_evaluation_latencies.append(duration_ms)
        if len(self.risk_evaluation_latencies) > 500:
            self.risk_evaluation_latencies.pop(0)

    def record_db_latency(self, duration_ms: float):
        self.db_query_latencies.append(duration_ms)
        if len(self.db_query_latencies) > 500:
            self.db_query_latencies.pop(0)

    def record_api_latency(self, duration_ms: float):
        self.api_response_latencies.append(duration_ms)
        if len(self.api_response_latencies) > 500:
            self.api_response_latencies.pop(0)

    async def get_system_metrics(self) -> Dict[str, Any]:
        process = psutil.Process(os.getpid())
        mem_info = process.memory_info()

        # Database health check & latency
        t0 = time.perf_counter()
        db_ok = False
        try:
            async with AsyncSessionLocal() as session:
                await session.execute(text("SELECT 1"))
                db_ok = True
        except Exception:
            db_ok = False
        db_ping_ms = round((time.perf_counter() - t0) * 1000, 2)

        avg_risk_latency = (
            round(sum(self.risk_evaluation_latencies) / len(self.risk_evaluation_latencies), 2)
            if self.risk_evaluation_latencies
            else 0.0
        )
        avg_api_latency = (
            round(sum(self.api_response_latencies) / len(self.api_response_latencies), 2)
            if self.api_response_latencies
            else 0.0
        )

        return {
            "uptime_seconds": round(time.time() - _METRICS_START_TIME, 1),
            "cpu_percent": process.cpu_percent(),
            "memory_mb": round(mem_info.rss / (1024 * 1024), 2),
            "database": {
                "healthy": db_ok,
                "ping_ms": db_ping_ms,
            },
            "events": {
                "incoming_total": self.incoming_events_total,
                "duplicates_filtered": self.duplicate_events_total,
                "failures": self.event_processing_failures,
            },
            "latencies_ms": {
                "avg_risk_eval": avg_risk_latency,
                "avg_api_response": avg_api_latency,
            },
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }


METRICS = MetricsCollector()

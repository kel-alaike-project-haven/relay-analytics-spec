import uuid
import random
from datetime import datetime, timezone, timedelta
from typing import List

def uuid4() -> str:
    return str(uuid.uuid4())

def utcnow() -> datetime:
    return datetime.now(timezone.utc)

def rfc3339(ts) -> str:
    return ts.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))

def deltas_minutes(lo: int, hi: int) -> timedelta:
    return timedelta(minutes=random.randint(lo, hi))

def normal_minutes(mu: float, sd: float, lo: float) -> timedelta:
    val = max(random.gauss(mu, sd), lo)
    return timedelta(minutes=val)

def poisson_knuth(lam: float) -> int:
    """
    Draw a Poisson(λ) variate using Knuth's algorithm.
    Accurate and simple for small-to-moderate λ (our rate is per second).
    """
    import math
    L = math.exp(-lam)
    k = 0
    p = 1.0
    while p > L:
        k += 1
        p *= random.random()
    return k - 1

def exponential_gaps(k: int, rate_per_sec: float) -> List[float]:
    """Return k exponential inter-arrival gaps in seconds (mean 1/rate)."""
    if k <= 0 or rate_per_sec <= 0:
        return []
    mean_gap = 1.0 / rate_per_sec
    return [random.expovariate(1.0 / mean_gap) for _ in range(k)]

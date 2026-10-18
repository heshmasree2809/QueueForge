"""Job handlers. Register new job types in HANDLERS. Handlers must be idempotent (at-least-once delivery)."""
import random
import time
from typing import Any, Callable

Handler = Callable[[dict], Any]


def echo(payload: dict) -> dict:
    return {"echo": payload}


def add(payload: dict) -> dict:
    numbers = payload.get("numbers")
    if not isinstance(numbers, list) or not all(isinstance(n, (int, float)) for n in numbers):
        raise ValueError("payload.numbers must be a list of numbers")
    return {"sum": sum(numbers)}


def sleep(payload: dict) -> dict:
    seconds = max(0.0, min(float(payload.get("seconds", 1)), 10.0))
    time.sleep(seconds)
    return {"slept": seconds}


def flaky(payload: dict) -> dict:
    """Fails randomly. Useful for watching retries and backoff on the dashboard."""
    if random.random() < float(payload.get("fail_rate", 0.5)):
        raise RuntimeError("random failure")
    return {"ok": True}


def always_fail(payload: dict) -> dict:
    raise RuntimeError(str(payload.get("message", "this job always fails")))


HANDLERS: dict[str, Handler] = {
    "echo": echo,
    "add": add,
    "sleep": sleep,
    "flaky": flaky,
    "fail": always_fail,
}

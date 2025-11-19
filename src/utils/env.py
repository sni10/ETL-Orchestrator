# src/utils/env.py
import os


def get_int(key, default=0):
    value = os.getenv(key)
    if value is None:
        return default
    try:
        return int(value)
    except (ValueError, TypeError):
        return default


def get_str(key, default=""):
    return os.getenv(key, default)


def get_bool(key, default=False):
    value = os.getenv(key, "").lower()
    if value in ("true", "1", "yes", "on"):
        return True
    elif value in ("false", "0", "no", "off"):
        return False
    return default


def get_float(key, default=0.0):
    value = os.getenv(key)
    if value is None:
        return default
    try:
        return float(value)
    except (ValueError, TypeError):
        return default

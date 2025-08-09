import os
from typing import Any, Dict
import yaml

APP_ENV_VAR = "GENERATOR_ENV"

def _merge_dict(a: Dict[str, Any], b: Dict[str, Any]) -> Dict[str, Any]:
    """Shallow merge: values in b override a. Dict values are merged shallowly."""
    out = dict(a)
    for k, v in (b or {}).items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k].update(v)
        else:
            out[k] = v
    return out

def load_config(config_dir: str) -> Dict[str, Any]:
    env = os.getenv(APP_ENV_VAR, "dev")

    with open(os.path.join(config_dir, "default.yaml"), "r") as f:
        base = yaml.safe_load(f) or {}

    env_path = os.path.join(config_dir, f"{env}.yaml")
    if os.path.exists(env_path):
        with open(env_path, "r") as f:
            env_cfg = yaml.safe_load(f) or {}
        cfg = _merge_dict(base, env_cfg)
    else:
        cfg = base

    # Env var overrides (optional)
    eps_env = os.getenv("GEN_EVENTS_PER_SEC")
    if eps_env:
        cfg.setdefault("rate", {})["events_per_sec"] = int(eps_env)

    topic_env = os.getenv("PUBSUB_TOPIC")
    if topic_env:
        cfg.setdefault("pubsub", {})["topic"] = topic_env

    return cfg

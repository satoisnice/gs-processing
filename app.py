import json
import os
import logging
import logging.config
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Tuple, Optional

import yaml
import requests
import connexion
from apscheduler.schedulers.background import BackgroundScheduler
from connexion.middleware import MiddlewarePosition
from starlette.middleware.cors import CORSMiddleware


with open("/app/config/processing_config.yml", "r") as f:
    app_config = yaml.safe_load(f)

with open("/app/config/log_config.yml", "r") as f:
    LOG_CONFIG = yaml.safe_load(f)
    logging.config.dictConfig(LOG_CONFIG)

logger = logging.getLogger("basicLogger")

BASE_URL = app_config["datastore"]["url"].rstrip("/")
PLAYER_PATH = app_config["events"]["player_telemetry_path"]
HEALTH_PATH = app_config["events"]["server_health_path"]
PERIOD_SEC = int(app_config["scheduler"]["period_sec"])
STATS_FILE = app_config["stats"]["file"]

def get_config():
    with open("/app/config/processing_config.yml", "r") as f:
        return yaml.safe_load(f)

def now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_iso(ts: str) -> datetime:
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    return datetime.fromisoformat(ts)


def ensure_parent_dir(path: str) -> None:
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)


def init_stats() -> dict:
    return {
        "num_server_health_readings": 0,
        "num_player_telemetry_events": 0,

        "max_players": 0,
        "min_players": None,
        "avg_players": 0.0,

        "avg_cpu_usage": 0.0,
        "avg_player_ping": 0.0,

        "last_updated": "1970-01-01T00:00:00Z",

        # internal
        "_player_sum": 0.0,
        "_player_count": 0,
        "_cpu_sum": 0.0,
        "_cpu_count": 0,
        "_ping_sum": 0.0,
        "_ping_count": 0,
    }


def save_stats(stats: dict) -> None:
    ensure_parent_dir(STATS_FILE)
    tmp_path = STATS_FILE + ".tmp"
    with open(tmp_path, "w") as f:
        json.dump(stats, f, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp_path, STATS_FILE)


def load_stats_create_if_missing() -> dict:
    """Used by populate_stats: if file missing/corrupt/empty, start from defaults."""
    ensure_parent_dir(STATS_FILE)

    if not os.path.exists(STATS_FILE) or os.path.getsize(STATS_FILE) == 0:
        stats = init_stats()
        save_stats(stats)
        return stats

    try:
        with open(STATS_FILE, "r") as f:
            data = json.load(f)
        base = init_stats()
        base.update(data)
        return base
    except Exception:
        stats = init_stats()
        save_stats(stats)
        return stats


def load_stats_no_create() -> dict:
    """Used by GET /stats: if file missing, caller must return 404."""
    if not os.path.exists(STATS_FILE):
        raise FileNotFoundError(STATS_FILE)

    with open(STATS_FILE, "r") as f:
        data = json.load(f)

    base = init_stats()
    base.update(data)
    return base


def fetch_events(path: str, start_iso: str, end_iso: str) -> Tuple[int, Any]:
    """
    Return (status_code, payload).
    Never raises: caller logs ERROR on non-200 per lab requirements.
    """
    url = f"{BASE_URL}{path}"
    params = {"start_timestamp": start_iso, "end_timestamp": end_iso}

    try:
        r = requests.get(url, params=params, timeout=10)
        if r.status_code != 200:
            logger.error("GET %s failed status=%s body=%s", r.url, r.status_code, r.text[:300])
            return r.status_code, []
        payload = r.json() if r.content else []
        return 200, payload
    except Exception as ex:
        logger.exception("GET %s failed: %s", url, ex)
        return 0, []


def coerce_list(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict) and "events" in payload and isinstance(payload["events"], list):
        return payload["events"]
    return []


def _extract_records(item: Dict[str, Any], nested_key: str) -> List[Dict[str, Any]]:
    """
    Storage might return:
      - batch objects with nested arrays (e.g. {"readings":[...]} or {"events":[...]})
      - or a flat list of readings/events
    This returns a list of record dicts either way.
    """
    if isinstance(item, dict) and nested_key in item and isinstance(item[nested_key], list):
        return [r for r in item[nested_key] if isinstance(r, dict)]
    if isinstance(item, dict):
        return [item]
    return []


def pick_ts(d: Dict[str, Any]) -> Optional[str]:
    for k in (
        "date_created",
        "created_timestamp",
        "stored_timestamp",
        "received_timestamp",
        "recorded_timestamp",
        "event_timestamp",
        "sent_timestamp",
    ):
        v = d.get(k)
        if isinstance(v, str) and v:
            return v
    return None


def populate_stats():
    logger.info("Periodic processing has started")
    logger.info(f"current config: {get_config()}")

    stats = load_stats_create_if_missing()

    start_iso = stats.get("last_updated") or "1970-01-01T00:00:00Z"
    end_iso = now_utc_iso()

    try:
        start_dt = parse_iso(start_iso) + timedelta(microseconds=1)
        start_iso = start_dt.isoformat().replace("+00:00", "Z")
    except Exception:
        start_iso = "1970-01-01T00:00:00Z"

    sh_status, sh_payload = fetch_events(HEALTH_PATH, start_iso, end_iso)
    pt_status, pt_payload = fetch_events(PLAYER_PATH, start_iso, end_iso)

    if sh_status != 200:
        logger.error("Storage GET server-health failed (status=%s)", sh_status)
        server_health_items: List[Dict[str, Any]] = []
    else:
        server_health_items = coerce_list(sh_payload)

    if pt_status != 200:
        logger.error("Storage GET player-telemetry failed (status=%s)", pt_status)
        telemetry_items: List[Dict[str, Any]] = []
    else:
        telemetry_items = coerce_list(pt_payload)

    logger.info(
        "Events received: server_health=%d player_telemetry=%d (window %s -> %s)",
        len(server_health_items),
        len(telemetry_items),
        start_iso,
        end_iso,
    )

    most_recent_dt = parse_iso(start_iso)
    found_any_timestamp = False

    processed_sh_records = 0
    processed_pt_records = 0

    # ---- server-health processing ----
    for item in server_health_items:
        for r in _extract_records(item, "readings"):
            processed_sh_records += 1
            stats["num_server_health_readings"] += 1

            players = r.get("active_players")
            if players is not None:
                p = int(players)
                stats["max_players"] = max(stats["max_players"], p)
                stats["min_players"] = p if stats["min_players"] is None else min(stats["min_players"], p)
                stats["_player_sum"] += p
                stats["_player_count"] += 1
                stats["avg_players"] = stats["_player_sum"] / stats["_player_count"]

            cpu = r.get("cpu_usage")
            if cpu is not None:
                c = float(cpu)
                stats["_cpu_sum"] += c
                stats["_cpu_count"] += 1
                stats["avg_cpu_usage"] = stats["_cpu_sum"] / stats["_cpu_count"]

            ts = pick_ts(r)
            if ts:
                try:
                    dt = parse_iso(ts)
                    found_any_timestamp = True
                    if dt > most_recent_dt:
                        most_recent_dt = dt
                except Exception:
                    pass

    # ---- telemetry processing ----
    for item in telemetry_items:
        for e in _extract_records(item, "events"):
            processed_pt_records += 1
            stats["num_player_telemetry_events"] += 1

            ping = e.get("player_ping")
            if ping is not None:
                pg = float(ping)
                stats["_ping_sum"] += pg
                stats["_ping_count"] += 1
                stats["avg_player_ping"] = stats["_ping_sum"] / stats["_ping_count"]

            ts = pick_ts(e)
            if ts:
                try:
                    dt = parse_iso(ts)
                    found_any_timestamp = True
                    if dt > most_recent_dt:
                        most_recent_dt = dt
                except Exception:
                    pass

    if found_any_timestamp:
        stats["last_updated"] = most_recent_dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")
    else:
        if processed_sh_records > 0 or processed_pt_records > 0:
            stats["last_updated"] = end_iso

    save_stats(stats)

    logger.debug(
        "Updated stats values: %s",
        {
            "num_server_health_readings": stats["num_server_health_readings"],
            "num_player_telemetry_events": stats["num_player_telemetry_events"],
            "max_players": stats["max_players"],
            "min_players": stats["min_players"],
            "avg_players": stats["avg_players"],
            "avg_cpu_usage": stats["avg_cpu_usage"],
            "avg_player_ping": stats["avg_player_ping"],
            "last_updated": stats["last_updated"],
        },
    )

    logger.info("Periodic processing has ended")


def get_stats():
    logger.info("GET /stats request received")

    try:
        stats = load_stats_no_create()
    except FileNotFoundError:
        logger.error("Statistics do not exist")
        return {"message": "Statistics do not exist"}, 404
    except Exception as ex:
        logger.exception("Failed reading statistics: %s", ex)
        return {"message": "Statistics do not exist"}, 404

    result = {
        "num_server_health_readings": stats["num_server_health_readings"],
        "num_player_telemetry_events": stats["num_player_telemetry_events"],
        "max_players": stats["max_players"],
        "avg_players": stats["avg_players"],
        "min_players": 0 if stats["min_players"] is None else stats["min_players"],
        "avg_cpu_usage": stats["avg_cpu_usage"],
        "avg_player_ping": stats["avg_player_ping"],
        "last_updated": stats["last_updated"],
    }

    logger.debug("Stats dict: %s", result)
    logger.info("GET /stats request completed")
    return result, 200


def init_scheduler():
    sched = BackgroundScheduler(daemon=True)
    sched.add_job(populate_stats, "interval", seconds=PERIOD_SEC)
    sched.start()


app = connexion.FlaskApp(__name__, specification_dir=".")
app.add_api("processing_api.yml", strict_validation=True, validate_responses=True)

app.add_middleware(
    CORSMiddleware,
    position=MiddlewarePosition.BEFORE_EXCEPTION,
    allow_origins=["*"], # dont do in prod
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
if __name__ == "__main__":
    init_scheduler()
    app.run(port=8090, host="0.0.0.0")

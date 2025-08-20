from __future__ import annotations
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timezone
import time

import pandas as pd

from .startgg_api import StartGGClient
from .discovery import discover_tournaments
from .processing import (
    flatten_nested_json_df, player_list, make_matrices, update_elo, sort_elo,
    summarize_players, attendance_from_players
)
from .extract import get_event_id, get_all_set_ids, get_players_and_score


# ---------- Timestamp helpers (LOUD) ----------
def _ts_to_iso(ts: int | float | None) -> str:
    if ts is None:
        return "None"
    try:
        return datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat()
    except Exception:
        return f"Invalid({ts})"

def _loud_ts_window(after_ts: int | None, before_ts: int | None):
    print(f"[TS] Using window: after={after_ts} ({_ts_to_iso(after_ts)})  "
          f"before={before_ts} ({_ts_to_iso(before_ts)})")

def _stamp_event_rows_with_dates(df: pd.DataFrame) -> pd.DataFrame:
    if "startAt" in df.columns:
        df = df.copy()
        def _fmt(x):
            try:
                return datetime.fromtimestamp(int(x)).strftime("%Y-%m-%d")
            except Exception:
                return None
        df["startAt.iso"] = df["startAt"].apply(_ts_to_iso)
        df["Event Date"] = df["startAt"].apply(_fmt)
        print(f"[TS] Added 'startAt.iso' and 'Event Date' columns (rows={len(df)})")
    else:
        print("[TS][WARN] 'startAt' not found in events; cannot derive 'Event Date'.")
    return df


# ---------- Shape normalization fallback ----------
def ensure_event_columns(raw_df: pd.DataFrame) -> pd.DataFrame:
    """
    Return a DataFrame where each row is an event with columns:
      - events.slug
      - events.numEntrants
      - events.videogame.name
      - slug (tournament slug)
      - startAt, name, city
    Works whether raw_df has a nested 'events' list or already-flattened 'events.*' cols.
    """
    # Case A: already flattened
    has_flat_cols = any(c.startswith("events.") for c in raw_df.columns)
    if has_flat_cols:
        print("[TS] ensure_event_columns: already-flat events.* columns detected")
        return raw_df.reset_index(drop=True)

    # Case B: explode list column
    if "events" in raw_df.columns:
        print("[TS] ensure_event_columns: exploding 'events' list column")
        df = raw_df.copy()
        df = df.explode("events").reset_index(drop=True)
        ev = pd.json_normalize(df["events"]).add_prefix("events.")
        ev.index = df.index
        keep = df[["slug", "startAt", "name", "city"]].copy()
        out = pd.concat([keep, ev], axis=1)
        return out.reset_index(drop=True)

    # Case C: rebuild from records (paranoid)
    print("[TS][WARN] ensure_event_columns: neither 'events' nor 'events.*' visible, attempting json_normalize(record_path=['events'])")
    try:
        records = raw_df.to_dict("records")
        out = pd.json_normalize(
            records,
            record_path=["events"],
            meta=["slug", "startAt", "name", "city"],
            errors="ignore"
        )
        ev_cols = [c for c in out.columns if c not in {"slug", "startAt", "name", "city"}]
        out = out.rename(columns={c: f"events.{c}" for c in ev_cols})
        return out.reset_index(drop=True)
    except Exception as e:
        print(f"[TS][ERROR] ensure_event_columns failed: {e}")
        return raw_df.reset_index(drop=True)


def _ultimate_filter(df: pd.DataFrame, min_entrants: int = 16) -> pd.DataFrame:
    if {"events.videogame.name", "events.numEntrants"}.issubset(df.columns):
        mask = (
            (df["events.videogame.name"] == "Super Smash Bros. Ultimate") &
            (pd.to_numeric(df["events.numEntrants"], errors="coerce").fillna(0).astype(int) >= min_entrants)
        )
        return df.loc[mask].copy()
    # fallback: try generic flatten then filter
    print("[TS][WARN] _ultimate_filter: expected columns missing; retrying flatten fallback")
    alt = flatten_nested_json_df(df)
    if {"events.videogame.name", "events.numEntrants"}.issubset(alt.columns):
        mask = (
            (alt["events.videogame.name"] == "Super Smash Bros. Ultimate") &
            (pd.to_numeric(alt["events.numEntrants"], errors="coerce").fillna(0).astype(int) >= min_entrants)
        )
        return alt.loc[mask].copy()
    raise RuntimeError("Could not find event fields after normalization.")


def run_pipeline(
    coordinates_radius: List[Tuple[str, str]] | None,
    after_ts: Optional[int],
    before_ts: Optional[int],
    event_slugs: Optional[List[str]] = None,
    out_dir: Path = Path("data/outputs"),
    bundle_name: str = "players.pkl",
    min_entrants: int = 16,
) -> Path:
    """
    Returns a path to a .pkl containing:
    {
      'tournaments': df_tournaments,  # per-event rows
      'players': df_players,          # per-player summary
      'elo': elo_dict,                # name -> rating
      'metadata': {...}               # args, counts, timestamps
    }
    """
    if coordinates_radius:
        _loud_ts_window(after_ts, before_ts)
    else:
        print("[TS] Discovery disabled in this run.")

    client = StartGGClient()
    out_dir.mkdir(parents=True, exist_ok=True)
    cache: Dict[int, Dict[str, int|None]] = {}

    # 1) Build event list (discovery or direct slugs)
    if event_slugs:
        print(f"[TS] Direct mode with {len(event_slugs)} event slugs (discovery OFF)")
        events_df = pd.DataFrame({"jakeSlug": event_slugs})
    else:
        raw = discover_tournaments(client, coordinates_radius or [], after_ts or 0, before_ts or 0)
        print(f"[TS] discover_tournaments returned {len(raw)} tournament rows (pre-normalization)")
        flat_any = ensure_event_columns(raw)
        print(f"[TS] Normalized to event-level rows: {len(flat_any)}")
        ult = _ultimate_filter(flat_any, min_entrants=min_entrants).reset_index(drop=True)
        print(f"[TS] Ultimate filter kept {len(ult)} event rows (min_entrants={min_entrants})")

        # Add readable dates loudly
        ult = _stamp_event_rows_with_dates(ult)

        # Create URLs & jakeSlug
        if "events.slug" in ult.columns:
            ult["startgg_url"] = "start.gg/" + ult["events.slug"].astype(str)
            ult["StartGG EVENT_ID"] = ult["events.slug"].astype(str).str.split("/").str[-1]
            ult["jakeSlug"] = ult["slug"].astype(str) + "/event/" + ult["StartGG EVENT_ID"]
        else:
            # Nested dict (rare with our normalization, but safe):
            if "events" not in ult.columns:
                raise RuntimeError("Missing both 'events.slug' and 'events' for URL derivation.")
            ult["startgg_url"] = "start.gg/" + ult["events"].map(lambda x: x.get("slug")).astype(str)
            ult["StartGG EVENT_ID"] = ult["events"].map(lambda x: x.get("slug").split("/")[-1])
            ult["jakeSlug"] = ult["slug"].astype(str) + "/event/" + ult["StartGG EVENT_ID"]

        events_df = ult.drop_duplicates("startgg_url").reset_index(drop=True)
        print(f"[TS] Deduped by 'startgg_url' -> {len(events_df)} event rows")
        if "Event Date" in events_df.columns:
            earliest = events_df["Event Date"].dropna().min()
            latest = events_df["Event Date"].dropna().max()
            print(f"[TS] Event date span in results: {earliest} .. {latest}")

    # 2) Resolve event IDs, set IDs, and sets — MAKE THIS VERY VOCAL
    def safe_event_id(slug: str) -> Optional[int]:
        try:
            return get_event_id(client, slug)
        except Exception as e:
            print(f"[API][WARN] get_event_id failed for slug '{slug}': {e}")
            return None

    print("[TS][IDs] Resolving numeric event IDs from jakeSlug…")
    events_df["eventID"] = events_df["jakeSlug"].map(safe_event_id)
    before_drop = len(events_df)
    events_df = events_df.dropna(subset=["eventID"]).copy()
    events_df["eventID"] = events_df["eventID"].astype(int)
    print(f"[TS][IDs] Resolved eventIDs for {len(events_df)} rows (dropped {before_drop - len(events_df)} failures)")

    # Fetch set IDs per event with progress, counts, and timing
    set_ids_col: List[List[int]] = []
    n_events = len(events_df)
    print(f"[TS][IDs] Fetching set IDs for {n_events} events…")
    t0_all = time.perf_counter()
    for idx, row in enumerate(events_df.itertuples(index=False), start=1):
        eid = getattr(row, "eventID")
        slug = getattr(row, "jakeSlug", "")
        t0 = time.perf_counter()
        print(f"[TS][IDs] ({idx}/{n_events}) Event {eid} — {slug}: fetching set IDs…", flush=True)
        try:
            ids = get_all_set_ids(client, eid)
            dt = time.perf_counter() - t0
            print(f"[TS][IDs] ({idx}/{n_events}) Event {eid}: got {len(ids)} set IDs in {dt:.2f}s", flush=True)
        except Exception as e:
            print(f"[TS][IDs][WARN] ({idx}/{n_events}) Event {eid}: failed to fetch set IDs: {e}", flush=True)
            ids = []
        set_ids_col.append(ids)
    dt_all = time.perf_counter() - t0_all
    total_ids = sum(len(x) for x in set_ids_col)
    print(f"[TS][IDs] Completed set ID fetch: {total_ids} IDs across {n_events} events in {dt_all:.2f}s")

    events_df["setIDs"] = set_ids_col

    # Fetch sets with per-event progress and periodic heartbeat
    def fetch_sets_verbose(event_idx: int, set_ids: List[int]) -> List[Dict[str,int|None]]:
        total = len(set_ids)
        t0 = time.perf_counter()
        if total == 0:
            print(f"[TS][SETS] ({event_idx}) No set IDs; skipping.", flush=True)
            return []
        print(f"[TS][SETS] ({event_idx}) Fetching {total} sets…", flush=True)
        sets: List[Dict[str,int|None]] = []
        for j, sid in enumerate(set_ids, start=1):
            # Heartbeat: first, every 25, and last
            if j == 1 or (j % 25 == 0) or j == total:
                print(f"[TS][SETS] ({event_idx}) Progress {j}/{total} (setId={sid})", flush=True)
            res = get_players_and_score(client, sid, cache)
            if "Error" in res:
                continue
            sets.append(res)
        dt = time.perf_counter() - t0
        print(f"[TS][SETS] ({event_idx}) Collected {len(sets)}/{total} sets in {dt:.2f}s", flush=True)
        return sets

    print("[TS][SETS] Fetching per-set details for each event…")
    sets_col: List[List[Dict[str,int|None]]] = []
    for idx, set_ids in enumerate(events_df["setIDs"].tolist(), start=1):
        sets_col.append(fetch_sets_verbose(idx, set_ids))
    events_df["sets"] = sets_col

    # Build players per event
    print("[TS][PLAYERS] Deriving player sets per event…")
    events_df["players"] = events_df["sets"].map(player_list)

    # 3) Attendance
    print("[TS][ATT] Computing attendance per player…")
    attendance = attendance_from_players(events_df["players"])

    # 4) All sets → ELO
    print("[TS][ELO] Aggregating all sets and computing ELO…")
    all_sets: List[Dict[str,int|None]] = [m for sets in events_df["sets"] for m in sets]
    all_players = set().union(*events_df["players"])
    _ = make_matrices(all_players, all_sets)  # matrices available for later if needed

    elo = {p: 1500.0 for p in all_players}
    for m in all_sets:
        elo = update_elo(elo, m)
    elo = sort_elo(elo)
    print(f"[TS][ELO] Rated {len(elo)} players from {len(all_sets)} sets.")

    # 5) Player summary + timestamps in metadata
    print("[TS][PLAYERS] Summarizing players…")
    df_players = summarize_players(events_df["sets"])
    df_players["Tournaments Attended"] = df_players["Player"].map(attendance).fillna(0).astype(int)
    df_players["Loss to Tournament Ratio"] = df_players.apply(
        lambda r: (r["Losses"] / r["Tournaments Attended"]) if r["Tournaments Attended"] > 0 else None, axis=1
    )
    df_players["ELO"] = df_players["Player"].map(elo).fillna(0.0)
    df_players = df_players.sort_values(["ELO","Total Sets"], ascending=[False, False]).reset_index(drop=True)

    # 6) Bundle + write .pkl
    bundle = {
        "tournaments": events_df,
        "players": df_players,
        "elo": elo,
        "metadata": {
            "mode": "direct" if event_slugs else "discovery",
            "event_count": int(events_df.shape[0]),
            "set_count": int(sum(len(x) for x in events_df["setIDs"])),
            "ts_after": after_ts,
            "ts_before": before_ts,
            "ts_after_iso": _ts_to_iso(after_ts),
            "ts_before_iso": _ts_to_iso(before_ts),
        }
    }
    print(f"[TS] Metadata timestamps: after={bundle['metadata']['ts_after_iso']} "
          f"before={bundle['metadata']['ts_before_iso']}")

    out_path = out_dir / bundle_name
    pd.to_pickle(bundle, out_path)
    print(f"[TS] Wrote bundle to {out_path}")
    return out_path

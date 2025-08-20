import os, re, argparse
from pathlib import Path
from typing import List, Optional, Tuple, Dict, Any
from datetime import datetime

from .models import InputSpec

# ----- DEFAULT NORCAL REGIONS -----
DEFAULT_COORDS: List[Tuple[str, str]] = [
    ("37.77151615492457, -122.41563048985462", "70mi"),  # SF Bay
    ("38.57608096237729, -121.49183616631059", "40mi"),  # Sacramento
]

_TS_FMT = "%Y-%m-%d"


# -------- helpers: API key + URL parsing + timestamps --------
def resolve_api_key(api_key: Optional[str] = None) -> Optional[str]:
    if api_key:
        return api_key
    env_key = os.getenv("STARTGG_API_KEY")
    if env_key:
        return env_key
    # soft-fail here; cli will error later with parser.error for better UX
    return None


_EVENT_RE = re.compile(r"start\.gg/tournament/(?P<t_slug>[^/]+)/event/(?P<e_slug>[^/?#]+)", re.I)
_TOURNAMENT_RE = re.compile(r"start\.gg/(?:tournament|event)/(?P<t_slug>[^/]+)", re.I)
_PHASE_RE = re.compile(r"start\.gg/phase/(?P<pid>\d+)", re.I)
_PGROUP_RE = re.compile(r"start\.gg/pools?/(?P<pgid>\d+)", re.I)

def _extract_from_url(url: str) -> Dict[str, Any]:
    url = (url or "").strip()
    if not url:
        return {}
    if m := _EVENT_RE.search(url):
        return {"tournament_slug": m.group("t_slug"), "event_slug": m.group("e_slug")}
    if m := _TOURNAMENT_RE.search(url):
        return {"tournament_slug": m.group("t_slug")}
    if m := _PHASE_RE.search(url):
        return {"phase_id": int(m.group("pid"))}
    if m := _PGROUP_RE.search(url):
        return {"phase_group_ids": [int(m.group("pgid"))]}
    return {}


def _parse_date_str(s: str) -> int:
    """Parse YYYY-MM-DD to unix seconds with loud diagnostics."""
    dt = datetime.strptime(s, _TS_FMT)
    ts = int(dt.timestamp())
    print(f"[TS] Parsed date '{s}' -> unix {ts} ({dt.isoformat()})")
    return ts

def _validate_date_range(start: Optional[str], end: Optional[str]) -> None:
    if start and end:
        ts_start = _parse_date_str(start)
        ts_end = _parse_date_str(end)
        if ts_start > ts_end:
            raise SystemExit(f"[TS][ERROR] start-date '{start}' > end-date '{end}'")
        print(f"[TS] CLI window: {start}..{end}  (unix {ts_start}..{ts_end})")


def _parse_coords_arg(vals: Optional[List[str]]) -> List[Tuple[str, str]]:
    out: List[Tuple[str, str]] = []
    if not vals:
        return out
    for v in vals:
        # "lat,lon:radius"
        try:
            latlon, radius = v.split(":")
            lat, lon = [s.strip() for s in latlon.split(",")]
            out.append((f"{lat}, {lon}", radius.strip()))
        except Exception:
            raise SystemExit(f"Invalid --coords '{v}'. Expected 'LAT,LON:RADIUS'.")
    return out


# -------- argparse --------
def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="startgg-match-insights",
        description="Fetch and analyze Start.GG match data (NorCal discovery by default)."
    )
    # scope (still supported, but you want NorCal only in practice)
    scope = p.add_argument_group("scope (ignored if --norcal or discovery mode)")
    scope.add_argument("--url", help="Start.GG URL (tournament, event, phase, or pools).")
    scope.add_argument("--tournament-slug", help="Tournament slug, e.g., 'tournament/genesis-10'.")
    scope.add_argument("--event-slug", help="Event slug under the tournament.")
    scope.add_argument("--phase-id", type=int, help="Phase ID.")
    scope.add_argument("--phase-group-id", action="append", type=int, help="Phase group ID; repeatable.")

    # filters
    filt = p.add_argument_group("filters")
    filt.add_argument("--game", help="Filter by game (string contains).")
    filt.add_argument("--rounds", nargs="+", help="Filter by round labels, e.g., WF SF GF.")
    filt.add_argument("--start-date", help="YYYY-MM-DD (inclusive).")
    filt.add_argument("--end-date", help="YYYY-MM-DD (inclusive).")

    # discovery
    disc = p.add_argument_group("discovery (NorCal-only mode)")
    disc.add_argument("--norcal", action="store_true", help="Enable NorCal discovery (SF + Sacramento regions).")
    disc.add_argument("--coords", action="append", metavar="LAT,LON:RADIUS",
                      help='Add/override discovery regions, e.g., --coords "37.77,-122.41:70mi" (repeatable).')

    # output
    out = p.add_argument_group("output")
    out.add_argument("--out", type=Path, default=Path("data/outputs"), help="Output dir.")
    out.add_argument("--format", dest="fmt", choices=["csv", "json", "parquet"], default="csv")
    out.add_argument("--overwrite", action="store_true")

    # auth
    auth = p.add_argument_group("auth")
    auth.add_argument("--api-key", help="Start.GG API Key (else env STARTGG_API_KEY or src/keys.py).")

    return p


def parse_args(argv: Optional[List[str]] = None) -> InputSpec:
    parser = build_parser()
    args = parser.parse_args(argv)

    spec = InputSpec(
        tournament_slug=args.tournament_slug,
        event_slug=args.event_slug,
        phase_id=args.phase_id,
        phase_group_ids=args.phase_group_id,
        game=args.game,
        rounds=args.rounds,
        start_date=args.start_date,
        end_date=args.end_date,
        out=args.out,
        fmt=args.fmt,
        overwrite=args.overwrite,
        api_key=resolve_api_key(args.api_key),
    )

    # Merge URL-derived info if provided
    if args.url:
        extracted = _extract_from_url(args.url)
        for k, v in extracted.items():
            if getattr(spec, k) in (None, [], ""):
                setattr(spec, k, v)

    # Discovery mode decision:
    discovery_mode = bool(
        args.norcal or (
            not any([spec.tournament_slug, spec.event_slug, spec.phase_id, spec.phase_group_ids])
            and (spec.start_date and spec.end_date)
        )
    )
    spec.discovery_mode = discovery_mode
    spec.discovery_regions = _parse_coords_arg(args.coords)
    if discovery_mode and not spec.discovery_regions:
        spec.discovery_regions = DEFAULT_COORDS

    # Loudly validate and echo dates if provided
    _validate_date_range(spec.start_date, spec.end_date)

    if discovery_mode:
        print("[TS] Discovery mode ACTIVE")
        if spec.discovery_regions:
            for i, (coords, radius) in enumerate(spec.discovery_regions, 1):
                print(f"[TS] Region {i}: {coords} within {radius}")
        if spec.start_date and spec.end_date:
            # Show both string and unix
            _ = _parse_date_str(spec.start_date)
            _ = _parse_date_str(spec.end_date)
    else:
        print("[TS] Discovery mode OFF (explicit scope required)")

    if not spec.api_key:
        parser.error("No API key. Use --api-key, env STARTGG_API_KEY, or add src/keys.py: STARTGG_API_KEY='...'")

    if not discovery_mode:
        # Only enforce scope when not in discovery mode
        try:
            spec.require_any_scope()
        except ValueError as e:
            parser.error(str(e))

    return spec

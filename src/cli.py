from pathlib import Path
from .input_parse import parse_args
from .pipeline import run_pipeline

def _to_unix(s: str | None):
    if not s: return None
    from datetime import datetime
    return int(datetime.strptime(s, "%Y-%m-%d").timestamp())

def main():
    spec = parse_args()

    if spec.discovery_mode:
        coords = spec.discovery_regions
        after = _to_unix(spec.start_date) if spec.start_date else None
        before = _to_unix(spec.end_date) if spec.end_date else None
        out = run_pipeline(
            coordinates_radius=coords,
            after_ts=after,
            before_ts=before,
            event_slugs=None,
            out_dir=spec.out,
            bundle_name="players.pkl",
            min_entrants=16,
        )
        print(f"✅ NorCal discovery complete. Wrote: {out}")
        return

    # (Fallback: direct event mode if user provided slugs)
    event_slugs = []
    if spec.tournament_slug and spec.event_slug:
        event_slugs.append(f"{spec.tournament_slug}/event/{spec.event_slug}")

    out = run_pipeline(
        coordinates_radius=None,
        after_ts=None,
        before_ts=None,
        event_slugs=event_slugs or None,
        out_dir=spec.out,
        bundle_name="players.pkl",
        min_entrants=16,
    )
    print(f"✅ Wrote: {out}")

if __name__ == "__main__":
    main()

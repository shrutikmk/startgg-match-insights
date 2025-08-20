from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, List, Tuple

@dataclass
class InputSpec:
    # Optional explicit scopes (not used in NorCal-only mode, but kept for flexibility)
    tournament_slug: Optional[str] = None
    event_slug: Optional[str] = None
    phase_id: Optional[int] = None
    phase_group_ids: Optional[List[int]] = None

    # Filters
    game: Optional[str] = None
    rounds: Optional[List[str]] = None
    start_date: Optional[str] = None    # "YYYY-MM-DD"
    end_date: Optional[str] = None

    # Output
    out: Path = Path("data/outputs")
    fmt: str = "csv"  # csv|json|parquet
    overwrite: bool = False

    # Auth (resolved at runtime)
    api_key: Optional[str] = None

    # Discovery mode (NorCal by default if active)
    discovery_mode: bool = False
    discovery_regions: List[Tuple[str, str]] = field(default_factory=list)  # list of (coords, radius), e.g. [("37.77,-122.41", "70mi")]

    def require_any_scope(self) -> None:
        if not any([self.tournament_slug, self.event_slug, self.phase_id, self.phase_group_ids]):
            raise ValueError(
                "Provide at least one scope: --url or "
                "--tournament-slug/--event-slug or --phase-id or --phase-group-id."
            )

from typing import List, Dict, Any, Tuple
import pandas as pd

from .startgg_api import StartGGClient
from .queries import DISCOVER_TOURNAMENTS

def discover_tournaments(client: StartGGClient,
                         coordinates_radius: List[Tuple[str, str]],
                         after_ts: int,
                         before_ts: int,
                         per_page: int = 50,
                         pages: int = 10) -> pd.DataFrame:
    nodes: List[Dict[str, Any]] = []
    for idx, (coords, radius) in enumerate(coordinates_radius, 1):
        print(f"[TS] Region {idx}: coords={coords} radius={radius}")
        for page in range(1, pages + 1):
            print(f"[TS] Query page {page}/{pages} for region {idx}")
            data = client.gql(
                DISCOVER_TOURNAMENTS,
                {"page": page, "perPage": per_page, "coordinates": coords, "radius": radius,
                 "after": after_ts, "before": before_ts}
            )
            page_nodes = (data.get("tournaments") or {}).get("nodes") or []
            print(f"[TS]   -> received {len(page_nodes)} tournaments")
            if not page_nodes:
                break
            nodes.extend(page_nodes)

    df = pd.DataFrame(nodes)
    print(f"[TS] Discovery total tournaments: {len(df)}")
    return df

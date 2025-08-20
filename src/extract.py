from typing import Dict, List, Any, Optional
from .startgg_api import StartGGClient
from .queries import GET_EVENT_ID, GET_SETS_PAGE, GET_SET_DETAIL

def get_event_id(client: StartGGClient, slug: str) -> int:
    data = client.gql(GET_EVENT_ID, {"slug": slug})
    ev = (data.get("event") or {})
    if not ev or not ev.get("id"):
        raise RuntimeError(f"Event not found for slug: {slug}")
    return int(ev["id"])

def get_all_set_ids(client: StartGGClient, event_id: int, per_page: int = 40) -> List[int]:
    set_ids: List[int] = []
    data = client.gql(GET_SETS_PAGE, {"eventId": event_id, "page": 1, "perPage": per_page})
    info = ((data.get("event") or {}).get("sets") or {}).get("pageInfo") or {}
    total_pages = int(info.get("totalPages", 1))
    nodes = ((data["event"]["sets"]).get("nodes")) or []
    set_ids.extend(int(n["id"]) for n in nodes if n and n.get("id"))
    for page in range(2, total_pages + 1):
        d = client.gql(GET_SETS_PAGE, {"eventId": event_id, "page": page, "perPage": per_page})
        ns = ((d.get("event") or {}).get("sets") or {}).get("nodes") or []
        set_ids.extend(int(n["id"]) for n in ns if n and n.get("id"))
    return set_ids

def get_players_and_score(client: StartGGClient, set_id: int, cache: Dict[int, Dict[str,int|None]]) -> Dict[str, int|None]:
    if set_id in cache:
        return cache[set_id]
    data = client.gql(GET_SET_DETAIL, {"setId": set_id})
    slots = ((data.get("set") or {}).get("slots")) or []
    if len(slots) < 2 or any(s.get("entrant") is None for s in slots):
        res = {"Error": f"Incomplete data for set {set_id}"}
        cache[set_id] = res
        return res
    def name(slot):
        p = slot["entrant"]["participants"][0]["player"]
        tag = p["gamerTag"]; pre = p.get("prefix") or ""
        return tag if pre == "" else f"{pre} | {tag}"
    def score(slot):
        sc = (((slot.get("standing") or {}).get("stats") or {}).get("score") or {}).get("value")
        return sc if isinstance(sc, int) else None
    p1, p2 = name(slots[0]), name(slots[1])
    s1, s2 = score(slots[0]), score(slots[1])
    res = {p1: s1, p2: s2}
    cache[set_id] = res
    return res

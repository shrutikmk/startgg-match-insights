from collections import defaultdict, Counter
from dataclasses import dataclass
from typing import Dict, List, Tuple, Iterable, Set, Any
import math
import numpy as np
import pandas as pd

@dataclass
class SetResult:
    p1: str
    p2: str
    s1: int | None
    s2: int | None

def flatten_nested_json_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.reset_index(drop=True)
    while True:
        list_cols = [c for c in df.columns if df[c].map(type).eq(list).all()]
        dict_cols = [c for c in df.columns if df[c].map(type).eq(dict).all()]
        if not list_cols and not dict_cols:
            break
        for c in dict_cols:
            expanded = pd.json_normalize(df[c]).add_prefix(f"{c}.")
            expanded.index = df.index
            df = pd.concat([df.drop(columns=[c]), expanded], axis=1)
        for c in list_cols:
            df = df.drop(columns=[c]).join(df[c].explode().to_frame()).reset_index(drop=True)
    return df

def player_list(sets: Iterable[Dict[str, Any]]) -> Set[str]:
    players: Set[str] = set()
    for m in sets:
        if not isinstance(m, dict) or len(m) < 2:
            continue
        (p1, p2), (s1, s2) = list(m.keys())[:2], list(m.values())[:2]
        if p1 and p2:
            players.add(p1); players.add(p2)
    return players

def make_matrices(players: Iterable[str], sets: Iterable[Dict[str,int|None]]):
    players = list(players)
    idx = {p: i for i, p in enumerate(players)}
    n = len(players)
    setM = np.zeros((n, n))
    gameM = np.zeros((n, n))
    for m in sets:
        if not isinstance(m, dict) or len(m) < 2: 
            continue
        p1, p2 = list(m.keys())[:2]
        s1, s2 = list(m.values())[:2]
        if s1 is None or s2 is None: 
            continue
        i, j = idx[p1], idx[p2]
        gameM[i, j] += s1; gameM[j, i] += s2
        if s1 > s2: setM[i, j] += 1
        elif s2 > s1: setM[j, i] += 1
    return idx, gameM, setM

def update_elo(elo: Dict[str, float], match: Dict[str, int|None], k: float = 30.0) -> Dict[str, float]:
    if not isinstance(match, dict) or len(match) < 2: 
        return elo
    p1, p2 = list(match.keys())[:2]
    s1, s2 = list(match.values())[:2]
    if s1 is None or s2 is None:
        return elo
    r1, r2 = elo.get(p1, 1500.0), elo.get(p2, 1500.0)
    p1_prob = 1.0/(1.0 + 10 ** ((r2 - r1)/400.0))
    outcome = 1.0 if s1 > s2 else 0.0
    r1 += k * (outcome - p1_prob)
    r2 += k * ((1.0 - outcome) - (1.0 - p1_prob))
    elo[p1], elo[p2] = r1, r2
    return elo

def sort_elo(elo: Dict[str, float]) -> Dict[str, float]:
    return dict(sorted(elo.items(), key=lambda kv: kv[1], reverse=True))

def summarize_players(sets_per_event: List[List[Dict[str,int|None]]]) -> pd.DataFrame:
    stats = defaultdict(lambda: {"wins":0,"losses":0,"h2h":defaultdict(lambda:[0,0]),
                                 "won_against":[],"lost_against":[]})
    for sets in sets_per_event:
        for m in sets:
            if not isinstance(m, dict) or len(m) < 2: 
                continue
            p1, p2 = list(m.keys())[:2]
            s1, s2 = list(m.values())[:2]
            if s1 is None or s2 is None: 
                continue
            if s1 > s2:
                stats[p1]["wins"] += 1; stats[p2]["losses"] += 1
                stats[p1]["h2h"][p2][0] += 1; stats[p2]["h2h"][p1][1] += 1
                stats[p1]["won_against"].append(p2); stats[p2]["lost_against"].append(p1)
            elif s2 > s1:
                stats[p2]["wins"] += 1; stats[p1]["losses"] += 1
                stats[p2]["h2h"][p1][0] += 1; stats[p1]["h2h"][p2][1] += 1
                stats[p2]["won_against"].append(p1); stats[p1]["lost_against"].append(p2)
    rows = []
    for player, s in stats.items():
        pos, even, neg = [], [], []
        for opp, (w, l) in s["h2h"].items():
            rec = f"{w}-{l}"
            if w > l: pos.append((opp,rec))
            elif w == l and w > 0: even.append((opp,rec))
            else: neg.append((opp,rec))
        rows.append({
            "Player": player,
            "Wins": s["wins"], "Losses": s["losses"],
            "Total Sets": s["wins"] + s["losses"],
            "Positive H2H": pos, "Even H2H": even, "Negative H2H": neg,
            "Won Against": s["won_against"], "Lost Against": s["lost_against"],
        })
    df = pd.DataFrame(rows).sort_values("Total Sets", ascending=False).reset_index(drop=True)
    return df

def attendance_from_players(players_per_event: Iterable[Set[str]]) -> Dict[str, int]:
    c = Counter()
    for s in players_per_event:
        c.update(s)
    return dict(c)

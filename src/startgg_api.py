import os, time, json, requests
from typing import Dict, Any, Optional

STARTGG_URL = "https://api.start.gg/gql/alpha"
DEFAULT_RATE_SECONDS = float(os.getenv("STARTGG_RATE_SECONDS", "1.1"))
MAX_RETRIES = 3

class StartGGClient:
    def __init__(self, api_key: Optional[str] = None, rate_seconds: float = DEFAULT_RATE_SECONDS):
        self.api_key = api_key or os.getenv("STARTGG_API_KEY")
        if not self.api_key:
            raise RuntimeError("Missing STARTGG_API_KEY.")
        self.rate_seconds = rate_seconds
        self._last_call = 0.0
        self.session = requests.Session()
        self.headers = {"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"}

    def _respect_rate(self):
        dt = time.time() - self._last_call
        if dt < self.rate_seconds:
            time.sleep(self.rate_seconds - dt)

    def gql(self, query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        payload = {"query": query, "variables": variables or {}}
        for attempt in range(1, MAX_RETRIES + 1):
            self._respect_rate()
            self._last_call = time.time()
            try:
                resp = self.session.post(STARTGG_URL, headers=self.headers, data=json.dumps(payload), timeout=30)
                if resp.status_code != 200:
                    print(f"[API][WARN] HTTP {resp.status_code} on attempt {attempt}: {resp.text[:200]}")
                    if attempt == MAX_RETRIES:
                        raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:200]}")
                    continue
                data = resp.json()
                if "errors" in data:
                    print(f"[API][WARN] GraphQL errors on attempt {attempt}: {data['errors']}")
                    if attempt == MAX_RETRIES:
                        raise RuntimeError(f"GraphQL errors: {data['errors']}")
                    continue
                return data["data"]
            except (requests.RequestException, ValueError) as e:
                print(f"[API][WARN] Exception on attempt {attempt}: {e}")
                if attempt == MAX_RETRIES:
                    raise
        raise RuntimeError("Unreachable")

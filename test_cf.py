import os
import random
from typing import List, Tuple, Any

import requests
from curl_cffi import requests as cffi_requests

BASE_URL = os.getenv("TEST_BASE_URL", "https://www.khmertimeskh.com").rstrip("/")
SLUG = os.getenv("TEST_SLUG", "national").strip()
EGRESS_PROXY = os.getenv("EGRESS_PROXY", "").strip()
WARP_PROXY = os.getenv("WARP_PROXY", "socks5h://127.0.0.1:40000").strip()
USE_FREE_PROXIES = os.getenv("USE_FREE_PROXIES", "0").strip() in ("1", "true", "yes")

TARGETS = [
    f"{BASE_URL}/wp-json/wp/v2/posts?per_page=1",
    f"{BASE_URL}/category/{SLUG}/",
]


def get_free_proxies(limit: int = 8) -> List[str]:
    urls = [
        "https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/protocols/socks5/data.txt",
        "https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/protocols/http/data.txt",
    ]
    found: List[str] = []
    for u in urls:
        try:
            r = requests.get(u, timeout=8)
            if r.status_code == 200:
                for line in r.text.splitlines():
                    p = line.strip()
                    if "://" in p:
                        if p.startswith("socks5://"):
                            p = "socks5h://" + p[len("socks5://"):]
                        found.append(p)
        except Exception:
            pass
    random.shuffle(found)
    return found[:limit]


def build_clients() -> List[Tuple[str, Any, float]]:
    clients: List[Tuple[str, Any, float]] = [
        ("DIRECT", cffi_requests.Session(impersonate="chrome124"), 15.0),
    ]
    if WARP_PROXY:
        clients.append((
            "WARP",
            cffi_requests.Session(
                impersonate="chrome124",
                proxies={"http": WARP_PROXY, "https": WARP_PROXY},
            ),
            15.0,
        ))
    if EGRESS_PROXY:
        clients.append((
            "EGRESS_PROXY",
            cffi_requests.Session(
                impersonate="chrome124",
                proxies={"http": EGRESS_PROXY, "https": EGRESS_PROXY},
            ),
            15.0,
        ))
    if USE_FREE_PROXIES:
        for i, p in enumerate(get_free_proxies(8), start=1):
            clients.append((
                f"FREE_{i}({p[:28]})",
                cffi_requests.Session(
                    impersonate="chrome124",
                    proxies={"http": p, "https": p},
                ),
                6.0,
            ))
    return clients


def main():
    clients = build_clients()
    print(f"Target base: {BASE_URL} | slug: {SLUG} | channels: {len(clients)}")

    for name, sess, timeout in clients:
        for url in TARGETS:
            try:
                r = sess.get(url, timeout=timeout)
                hdrs = {k.lower(): v for k, v in (r.headers or {}).items()}
                cf_mit = hdrs.get("cf-mitigated", "-")
                server = hdrs.get("server", "-")
                ctype = hdrs.get("content-type", "-")
                snippet = " ".join((r.text or "")[:120].split())
                print(
                    f"[{name}] {r.status_code} | server={server} | "
                    f"cf-mitigated={cf_mit} | ctype={ctype} | url={url} | head={snippet}"
                )
            except Exception as e:
                print(f"[{name}] ERR | url={url} | err={type(e).__name__}: {e}")


if __name__ == "__main__":
    main()

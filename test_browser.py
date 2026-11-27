import os
from urllib.parse import urlparse
from camoufox.sync_api import Camoufox

URL = os.getenv("TEST_URL", "https://www.khmertimeskh.com/category/national/")
WAIT_SEC = int(os.getenv("TEST_WAIT_SEC", "12"))
EGRESS_PROXY = os.getenv("EGRESS_PROXY", "").strip()


def build_browser_proxy(raw_proxy: str):
    if not raw_proxy:
        return None

    normalized = raw_proxy
    if normalized.startswith("socks5://"):
        normalized = "socks5h://" + normalized[len("socks5://"):]

    parsed = urlparse(normalized)
    if not parsed.hostname or not parsed.port:
        return {"server": normalized}

    server = f"{parsed.scheme}://{parsed.hostname}:{parsed.port}"
    proxy_cfg = {"server": server}
    if parsed.username:
        proxy_cfg["username"] = parsed.username
    if parsed.password:
        proxy_cfg["password"] = parsed.password
    return proxy_cfg


def main():
    proxy_cfg = build_browser_proxy(EGRESS_PROXY)
    proxy_info = proxy_cfg["server"] if proxy_cfg else "NONE (direct)"
    print(f"Opening {URL} (wait up to {WAIT_SEC}s) | proxy={proxy_info}...")

    camoufox_kwargs = {"headless": False}
    if proxy_cfg:
        camoufox_kwargs["proxy"] = proxy_cfg

    with Camoufox(**camoufox_kwargs) as browser:
        page = browser.new_page()
        page.set_default_timeout(30000)

        try:
            resp = page.goto(URL, wait_until="domcontentloaded")
            status = resp.status if resp else None
            print(f"Initial status: {status}")

            for i in range(1, WAIT_SEC + 1):
                title = page.title() or ""
                body_len = len(page.inner_text("body") or "")
                print(f"[{i}s] title={title!r} | text_len={body_len}")
                if "just a moment" not in title.lower() and body_len > 300:
                    break
                page.wait_for_timeout(1000)

            final_title = page.title() or ""
            final_html = page.content()
            cookies = page.context.cookies()
            cf_cookies = [c["name"] for c in cookies if "cf" in c["name"].lower()]

            print(f"Final title: {final_title!r}")
            print(f"HTML length: {len(final_html)} | CF cookies: {cf_cookies}")
            if "just a moment" in final_title.lower():
                print("RESULT: BLOCKED_OR_CHALLENGE")
            else:
                print("RESULT: PASSED")
        finally:
            browser.close()


if __name__ == "__main__":
    main()

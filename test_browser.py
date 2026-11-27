import os
from camoufox.sync_api import Camoufox

URL = os.getenv("TEST_URL", "https://www.khmertimeskh.com/category/national/")
WAIT_SEC = int(os.getenv("TEST_WAIT_SEC", "12"))


def main():
    print(f"Opening {URL} (wait up to {WAIT_SEC}s)...")
    with Camoufox(headless=False) as browser:
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

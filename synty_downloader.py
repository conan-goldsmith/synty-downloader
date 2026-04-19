#!/usr/bin/env python3
"""
synty_downloader.py
===================
Bulk downloader for Synty Store (syntystore.com) owned assets.

Authenticates via exported browser session cookies, crawls the Sky Pilot
digital downloads app that powers syntystore.com, and downloads every owned
pack to a local or NAS directory organised by pack name.

How it works
------------
Synty Store is a Shopify store that uses the Sky Pilot app to serve digital
downloads. There is no public API; instead this script mimics what your browser
does when you visit the downloads section:

  1. GET /apps/downloads/orders
         Shopify redirects to /apps/downloads/orders/{customer_id}?shop=...
         The customer ID in the redirect URL confirms you are authenticated.

  2. GET /apps/downloads/orders/{customer_id}[?line_items_page=N]
         Returns a paginated HTML listing of every pack you own. Each pack is a
         link to its own order-items page.

  3. GET /apps/downloads/customers/{cid}/orders/{oid}/order_items/{iid}
         Returns an HTML page for a single pack with one download button per
         file variant (Unity, Unreal, source files, icons, etc.).  Each button
         carries a label like "Unity_2022_3 | v1_8_0" and a size hint.

  4. GET /apps/downloads/downloads/{file_id}?email=...&order_id=...
         Sky Pilot validates the request and issues a 302 redirect to a
         time-limited pre-signed S3 / CDN URL. The file is streamed from there.

Authentication
--------------
The script does NOT automate the OAuth 2.0 login flow. Instead, you export
your live session cookies from the browser once (using the free "Get
cookies.txt LOCALLY" extension) and hand them to the script via cookies.txt.
Cookies typically stay valid for several weeks. When they expire, re-export.

Format filtering
----------------
Each file variant is classified as 'unity', 'unreal', or 'source' based on
the engine-version label on the page. Set ``formats`` in config.yaml to
control what gets downloaded. Use ``[all]`` to grab every variant.

Usage
-----
    python synty_downloader.py                        # uses ./config.yaml
    python synty_downloader.py --config /path/to/config.yaml
    python synty_downloader.py --dry-run              # list without downloading
    python synty_downloader.py --check-auth           # verify cookies are valid

Setup (one-time)
----------------
    1. Log into https://syntystore.com in your browser.
    2. Install the "Get cookies.txt LOCALLY" extension (Chrome/Firefox).
    3. Export cookies from syntystore.com -- save as cookies.txt.
    4. Copy cookies.txt next to this script (or set cookies_file in config.yaml).
    5. python3 -m venv ~/synty_env
    6. ~/synty_env/bin/pip install -r requirements.txt

Running (activate venv first)
------------------------------
    source ~/synty_env/bin/activate
    python synty_downloader.py [--check-auth] [--dry-run]
    deactivate

Or without activating:
    ~/synty_env/bin/python synty_downloader.py [--check-auth] [--dry-run]
"""

import argparse
import http.cookiejar
import os
import re
import sys
import time
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin

import requests
import yaml
from bs4 import BeautifulSoup
from tqdm import tqdm

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BASE_URL = "https://syntystore.com"

# Entry point for the Sky Pilot downloads app.  A GET to this URL with a valid
# Shopify session redirects to /apps/downloads/orders/{customer_id}?shop=...
ORDERS_ENTRY = f"{BASE_URL}/apps/downloads/orders"

# Sentinel value recognised by _wanted_format() to bypass all filtering.
FORMAT_ALL = "all"

REQUEST_TIMEOUT = 30    # seconds – used for every page / API request
DOWNLOAD_TIMEOUT = None  # no timeout – large files can take a long time
CHUNK_SIZE = 64 * 1024   # 64 KB per read() call during streaming


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

def load_config(config_path: str) -> dict:
    """Load and normalise settings from a YAML config file.

    Missing keys are filled with sensible defaults so the file only needs to
    contain the values you actually want to override.

    Args:
        config_path: Path to the YAML configuration file.

    Returns:
        A dictionary with at least the keys:
        ``download_dir``, ``cookies_file``, ``formats``,
        ``skip_existing``, ``delay_between_downloads``.

    Exits:
        Calls sys.exit(1) if the file is missing or malformed.
    """
    try:
        with open(config_path) as fh:
            cfg = yaml.safe_load(fh) or {}
    except FileNotFoundError:
        print(f"ERROR: Config file not found: {config_path}")
        sys.exit(1)
    except yaml.YAMLError as exc:
        print(f"ERROR: Could not parse config file: {exc}")
        sys.exit(1)

    # Expand ~ and apply defaults
    cfg["download_dir"] = os.path.expanduser(
        cfg.get("download_dir", "./synty_downloads")
    )
    cfg["cookies_file"] = os.path.expanduser(
        cfg.get("cookies_file", "./cookies.txt")
    )
    cfg.setdefault("formats", ["unity", "source"])
    cfg.setdefault("skip_existing", True)
    cfg.setdefault("delay_between_downloads", 1.5)
    return cfg


# ---------------------------------------------------------------------------
# Session / authentication
# ---------------------------------------------------------------------------

def build_session(cookies_file: str) -> requests.Session:
    """Create an authenticated requests.Session from a Netscape cookies.txt file.

    The session is pre-loaded with browser-like headers so that Shopify/Sky
    Pilot does not treat it as a bare HTTP client.

    Args:
        cookies_file: Path to a Netscape-format cookies.txt file exported from
            the browser while logged into syntystore.com.

    Returns:
        A ``requests.Session`` with cookies and headers configured.

    Exits:
        Calls sys.exit(1) if the file is missing or not a valid cookie jar.
    """
    jar = http.cookiejar.MozillaCookieJar(cookies_file)
    try:
        jar.load(ignore_discard=True, ignore_expires=True)
    except FileNotFoundError:
        print(f"ERROR: Cookies file not found: {cookies_file}")
        print()
        print("How to create cookies.txt:")
        print("  1. Log into https://syntystore.com in your browser")
        print("  2. Install 'Get cookies.txt LOCALLY' (Chrome/Firefox extension)")
        print("  3. On syntystore.com, click the extension and export cookies")
        print("  4. Save the file as cookies.txt alongside this script")
        sys.exit(1)
    except http.cookiejar.LoadError as exc:
        print(f"ERROR: Could not load cookies file ({cookies_file}): {exc}")
        print("Make sure it is a valid Netscape-format cookies.txt file.")
        sys.exit(1)

    session = requests.Session()
    session.cookies = jar
    # Mimic a real browser so Shopify does not block the request
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": (
            "text/html,application/xhtml+xml,application/xml;"
            "q=0.9,image/avif,image/webp,*/*;q=0.8"
        ),
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": BASE_URL,
    })
    return session


def get_customer_id(session: requests.Session) -> Optional[str]:
    """Verify authentication and return the Shopify customer ID.

    Sends a GET to the Sky Pilot orders entry point.  If the session cookies
    are valid, Shopify redirects to:
        /apps/downloads/orders/{customer_id}?shop=synty-store.myshopify.com

    The numeric customer ID embedded in that URL is extracted and returned.
    Any other response (login redirect, 404, network error) returns None.

    Args:
        session: An authenticated ``requests.Session``.

    Returns:
        The customer ID as a string, or None if authentication failed.
    """
    try:
        resp = session.get(ORDERS_ENTRY, allow_redirects=True, timeout=REQUEST_TIMEOUT)
    except requests.RequestException as exc:
        print(f"Network error during auth check: {exc}")
        return None

    # A successful auth produces a URL like:
    #   https://syntystore.com/apps/downloads/orders/6307192373500?shop=...
    m = re.search(r'/apps/downloads/orders/(\d+)', resp.url)
    if not m:
        return None
    if resp.status_code != 200:
        return None
    return m.group(1)


# ---------------------------------------------------------------------------
# Asset discovery
# ---------------------------------------------------------------------------

def fetch_pack_listing(session: requests.Session, customer_id: str) -> list:
    """Fetch the complete list of owned packs across all pagination pages.

    Walks /apps/downloads/orders/{customer_id}[?line_items_page=N] until no
    more pages are found.

    Args:
        session: An authenticated ``requests.Session``.
        customer_id: The Shopify customer ID returned by ``get_customer_id()``.

    Returns:
        A list of dicts, each with keys:
        ``title`` (str) – the product name shown in the library, and
        ``order_items_url`` (str) – the URL of the per-pack file listing page.
    """
    packs = []
    page = 1

    while True:
        # First page uses the base URL; subsequent pages add pagination params
        if page == 1:
            url = f"{ORDERS_ENTRY}/{customer_id}"
        else:
            url = (
                f"{ORDERS_ENTRY}/{customer_id}"
                f"?logged_in_customer_id={customer_id}&line_items_page={page}"
            )

        try:
            resp = session.get(url, timeout=REQUEST_TIMEOUT)
        except requests.RequestException as exc:
            print(f"  WARNING: Failed to fetch pack listing page {page}: {exc}")
            break

        if resp.status_code != 200:
            print(f"  WARNING: Pack listing returned HTTP {resp.status_code} on page {page}")
            break

        soup = BeautifulSoup(resp.text, "html.parser")
        page_packs = _parse_pack_listing(soup)

        # An empty page means we have gone past the last page
        if not page_packs:
            break

        packs.extend(page_packs)

        # Advance to the next page only if a navigation link exists
        next_link = soup.find("a", string=re.compile(r'next', re.I))
        if not next_link:
            if not _find_next_page_href(soup, page):
                break

        page += 1
        time.sleep(0.3)   # be polite to the server between page requests

    return packs


def _parse_pack_listing(soup: BeautifulSoup) -> list:
    """Extract pack title + order-items URL from one page of the library listing.

    Sky Pilot renders each owned product as an ``<a>`` element whose href
    contains ``/apps/downloads/customers/``.  We deduplicate by URL since the
    same product can appear on multiple orders (e.g. Humble Bundle redemptions
    and direct purchases).

    Args:
        soup: Parsed HTML of a library listing page.

    Returns:
        List of ``{"title": str, "order_items_url": str}`` dicts.
    """
    packs = []
    seen = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/apps/downloads/customers/" not in href:
            continue
        title = a.get_text(strip=True)
        full_url = href if href.startswith("http") else BASE_URL + href
        if full_url not in seen and title:
            seen.add(full_url)
            packs.append({"title": title, "order_items_url": full_url})
    return packs


def _find_next_page_href(soup: BeautifulSoup, current_page: int) -> Optional[str]:
    """Return the href of a next-page link, or None if on the last page.

    Checks for a link containing ``line_items_page={current_page + 1}`` as a
    fallback when the «Next» text link is styled differently.

    Args:
        soup: Parsed HTML of the current listing page.
        current_page: The page number currently being processed (1-based).

    Returns:
        The href string of the next-page link, or None.
    """
    next_page = current_page + 1
    for a in soup.find_all("a", href=True):
        if f"line_items_page={next_page}" in a["href"]:
            return a["href"]
    return None


def fetch_pack_files(
    session: requests.Session, pack_title: str, order_items_url: str
) -> list:
    """Fetch the file list for a single pack from its order-items page.

    The order-items page contains one download button per file variant.  Each
    button sits inside a container element whose text describes the file, e.g.:
        ['POLYGON_Adventure', 'Unity_2022_3 | v1_8_0', '(89.8 MB)', 'Download']

    Args:
        session: An authenticated ``requests.Session``.
        pack_title: The display name of the pack (used only for warning messages).
        order_items_url: Full URL of the pack's order-items page.

    Returns:
        A list of dicts with keys:
        ``name_stem`` (str)  – filename without extension, e.g. "POLYGON_Adventure_Unity_2022_3_v1_8_0"
        ``url``       (str)  – Sky Pilot download endpoint URL
        ``format``    (str)  – 'unity', 'unreal', or 'source'
        ``size_str``  (str)  – human-readable size from the page, e.g. "89.8 MB"
    """
    try:
        resp = session.get(order_items_url, timeout=REQUEST_TIMEOUT)
    except requests.RequestException as exc:
        print(f"  WARNING: Could not fetch files for '{pack_title}': {exc}")
        return []

    if resp.status_code != 200:
        print(f"  WARNING: Files page returned HTTP {resp.status_code} for '{pack_title}'")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    files = []

    for a in soup.find_all("a", href=True):
        href = a["href"]
        # Only process links that point to Sky Pilot's download endpoint
        if "/apps/downloads/downloads/" not in href:
            continue

        full_url = href if href.startswith("http") else BASE_URL + href

        # Gather the text labels from the surrounding container element
        container_texts = _extract_container_texts(a)
        name_stem = _build_name_stem(container_texts)
        fmt = _detect_format(container_texts)
        size_str = _extract_size(container_texts)

        files.append({
            "name_stem": name_stem,
            "url": full_url,
            "format": fmt,
            "size_str": size_str,
        })

    return files


def _extract_container_texts(link_tag) -> list:
    """Walk up the DOM from a download link to find its descriptive label.

    Sky Pilot wraps each download button in a small container element that also
    holds the filename, version label, and file size.  This function climbs the
    parent chain until it finds a node with more than one text segment, then
    returns those segments as a list.

    Typical result:
        ['POLYGON_Adventure', 'Unity_2022_3 | v1_8_0', '(89.8 MB)', 'Download']

    Args:
        link_tag: The BeautifulSoup ``<a>`` element for the download button.

    Returns:
        A list of non-empty stripped text strings from the nearest informative
        ancestor element.  Falls back to ``[link_tag.get_text()]`` if no such
        ancestor is found within 8 levels.
    """
    node = link_tag
    for _ in range(8):
        node = node.parent
        if node is None:
            break
        texts = [t.strip() for t in node.stripped_strings if t.strip()]
        if len(texts) > 1:
            return texts
    return [link_tag.get_text(strip=True)]


def _build_name_stem(texts: list) -> str:
    """Construct a safe filename stem from a container's text segments.

    ``texts[0]`` is taken as the base filename (e.g. ``POLYGON_Adventure``).
    If ``texts[1]`` looks like a version/engine label (contains ``|`` or
    engine keywords), it is sanitised and appended with an underscore separator
    to produce a unique, descriptive stem.

    Args:
        texts: The list of text segments returned by ``_extract_container_texts``.

    Returns:
        A filename stem string, e.g. ``POLYGON_Adventure_Unity_2022_3_v1_8_0``.
    """
    if not texts:
        return "download"
    stem = texts[0]
    if len(texts) > 1:
        t1 = texts[1]
        # Append the version label when it looks like one
        if "|" in t1 or re.search(r'unity|unreal|source|godot|v\d', t1, re.I):
            version_part = re.sub(r'[^A-Za-z0-9._-]', '_', t1).strip('_')
            version_part = re.sub(r'_+', '_', version_part)
            stem = f"{stem}_{version_part}"
    return stem


def _detect_format(texts: list) -> str:
    """Determine the engine/type category of a file from its label text.

    Classification rules (first match wins):
      - Contains "unity"  -> 'unity'
      - Contains "unreal" -> 'unreal'
      - Anything else     -> 'source'  (source files, Godot exports, icons, …)

    Args:
        texts: The list of text segments from ``_extract_container_texts``.

    Returns:
        One of the strings ``'unity'``, ``'unreal'``, or ``'source'``.
    """
    combined = " ".join(texts).lower()
    if "unity" in combined:
        return "unity"
    if "unreal" in combined:
        return "unreal"
    # Catch-all: source archives, Godot packages, icon PNGs, readme files, etc.
    return "source"


def _extract_size(texts: list) -> str:
    """Extract the human-readable file size from a container text list.

    Sky Pilot renders the size as a parenthesised string, e.g. ``(89.8 MB)``.
    This function finds the first such segment and strips the parentheses.

    Args:
        texts: The list of text segments from ``_extract_container_texts``.

    Returns:
        The size string without parentheses, e.g. ``"89.8 MB"``, or ``""`` if
        no size segment is found.
    """
    for t in texts:
        m = re.match(r'^\((.+)\)$', t.strip())
        if m:
            return m.group(1)
    return ""


# ---------------------------------------------------------------------------
# Format filtering
# ---------------------------------------------------------------------------

def _wanted_format(file_format: str, formats: list) -> bool:
    """Return True if a file should be downloaded given the configured formats.

    The special value ``'all'`` in the formats list bypasses filtering entirely,
    causing every file variant (Unity, Unreal, source files, icons, etc.) to
    be downloaded.

    Args:
        file_format: The format string assigned by ``_detect_format()``
            ('unity', 'unreal', or 'source').
        formats: The list of format strings from config, e.g. ``['unity', 'source']``
            or ``['all']``.

    Returns:
        True if the file should be downloaded, False otherwise.
    """
    if FORMAT_ALL in formats:
        return True
    return file_format in formats


# ---------------------------------------------------------------------------
# Filesystem helpers
# ---------------------------------------------------------------------------

# Characters that are illegal in file/directory names on Windows, Linux, and macOS
_UNSAFE_CHARS = re.compile(r'[<>:"/\\|?*\x00-\x1f]')


def _safe_name(name: str, max_len: int = 160) -> str:
    """Sanitise a string for use as a file or directory name.

    Replaces illegal characters with underscores, collapses consecutive
    underscores, and trims leading/trailing dots and spaces.

    Args:
        name: The raw name to sanitise.
        max_len: Maximum character length of the result (default 160).

    Returns:
        A filesystem-safe string, or ``"unknown"`` if the result would be empty.
    """
    name = _UNSAFE_CHARS.sub("_", name).strip(". ")
    name = re.sub(r'_+', '_', name)
    return name[:max_len] if name else "unknown"


def _find_existing(pack_dir: Path, stem: str) -> Optional[Path]:
    """Search a pack directory for a previously downloaded file matching a stem.

    Used to implement skip-existing logic before downloading.  Two strategies:

    1. Exact glob: looks for ``{safe_stem}.*`` (any extension).
    2. Prefix match: looks for any file whose name starts with the first 40
       characters of the stem.  This handles cases where the server provided a
       different (but recognisably similar) filename via Content-Disposition.

    ``.part`` files (incomplete downloads) are never treated as existing.

    Args:
        pack_dir: The directory for this pack.
        stem: The filename stem as built by ``_build_name_stem()``.

    Returns:
        The ``Path`` of a matching file, or ``None`` if not found.
    """
    if not pack_dir.exists():
        return None
    safe_stem = _safe_name(stem)
    # Strategy 1: exact stem with any extension
    for candidate in pack_dir.glob(f"{safe_stem}.*"):
        if candidate.suffix != ".part":
            return candidate
    # Strategy 2: prefix match for Content-Disposition-renamed files
    prefix = safe_stem[:40]
    for candidate in pack_dir.iterdir():
        if candidate.suffix != ".part" and candidate.name.startswith(prefix):
            return candidate
    return None


# ---------------------------------------------------------------------------
# Downloading
# ---------------------------------------------------------------------------

def download_file(
    session: requests.Session,
    url: str,
    pack_dir: Path,
    name_stem: str,
    dry_run: bool,
    delay: float,
) -> bool:
    """Stream a single file from a Sky Pilot download URL to disk.

    Flow:
      1. GET the Sky Pilot URL (which 302-redirects to a pre-signed S3/CDN URL).
      2. Read the ``Content-Disposition`` header to determine the real filename.
      3. Stream the response body to ``{pack_dir}/{filename}.part`` in 64 KB
         chunks, updating a tqdm progress bar.
      4. On success, rename ``.part`` -> final filename atomically.
      5. On any OS error, delete the partial file and return False.

    Args:
        session: An authenticated ``requests.Session``.
        url: The Sky Pilot download endpoint URL.
        pack_dir: Destination directory for this pack.
        name_stem: Fallback filename stem if Content-Disposition is absent.
        dry_run: If True, print what would be downloaded and return immediately.
        delay: Seconds to sleep after a successful download (server politeness).

    Returns:
        True on success, False on any HTTP or filesystem error.
    """
    if dry_run:
        print(f"    [DRY RUN] {name_stem}  ->  {pack_dir}/")
        return True

    pack_dir.mkdir(parents=True, exist_ok=True)

    try:
        resp = session.get(
            url, stream=True, timeout=DOWNLOAD_TIMEOUT, allow_redirects=True
        )
        resp.raise_for_status()
    except requests.HTTPError as exc:
        print(f"    ERROR: HTTP {exc.response.status_code} for {name_stem}")
        return False
    except requests.RequestException as exc:
        print(f"    ERROR: {exc}")
        return False

    # Use the server-provided filename when available; fall back to the stem
    dest_name = _filename_from_response(resp, name_stem)
    dest = pack_dir / dest_name
    tmp = dest.with_suffix(dest.suffix + ".part")

    total = int(resp.headers.get("content-length", 0)) or None

    try:
        with open(tmp, "wb") as fh:
            with tqdm(
                total=total,
                unit="B",
                unit_scale=True,
                unit_divisor=1024,
                desc=f"  {dest_name[:55]}",
                leave=False,
            ) as bar:
                for chunk in resp.iter_content(chunk_size=CHUNK_SIZE):
                    fh.write(chunk)
                    bar.update(len(chunk))
        tmp.rename(dest)
    except OSError as exc:
        print(f"    ERROR: File system error: {exc}")
        if tmp.exists():
            tmp.unlink()
        return False

    time.sleep(delay)
    return True


def _filename_from_response(resp: requests.Response, fallback_stem: str) -> str:
    """Derive a filesystem-safe filename from an HTTP response.

    Preference order:
      1. ``filename=`` value from the ``Content-Disposition`` response header.
      2. Fallback stem + extension guessed from ``Content-Type``.

    Args:
        resp: A completed ``requests.Response`` (headers available).
        fallback_stem: The name stem to use if Content-Disposition is absent.

    Returns:
        A sanitised filename string including extension where possible.
    """
    cd = resp.headers.get("content-disposition", "")
    m = re.search(r'filename[^;=\n]*=\s*["\']?([^"\'\n;]+)["\']?', cd, re.I)
    if m:
        name = _safe_name(m.group(1).strip())
        if name:
            return name

    # Guess extension from Content-Type as a last resort
    ct = resp.headers.get("content-type", "")
    if "zip" in ct:
        ext = ".zip"
    elif "octet-stream" in ct:
        ext = ".bin"
    else:
        ext = ""

    return _safe_name(fallback_stem) + ext


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    """Entry point: parse arguments, authenticate, discover packs, and download.

    Execution flow:
      1. Load config.yaml.
      2. Build an authenticated session from cookies.txt.
      3. Confirm authentication by fetching the customer ID.
      4. Fetch the full paginated pack listing.
      5. For each pack: fetch its file list, apply format filtering,
         skip already-downloaded files, then stream each file to disk.
      6. Print a summary of results.
    """
    parser = argparse.ArgumentParser(
        description="Bulk downloader for Synty Store owned assets",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python synty_downloader.py\n"
            "  python synty_downloader.py --config /nas/synty/config.yaml\n"
            "  python synty_downloader.py --dry-run\n"
            "  python synty_downloader.py --check-auth\n"
        ),
    )
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="Path to config.yaml (default: ./config.yaml)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List what would be downloaded without writing any files",
    )
    parser.add_argument(
        "--check-auth",
        action="store_true",
        help="Verify that cookies.txt is valid and exit",
    )
    args = parser.parse_args()

    cfg = load_config(args.config)
    session = build_session(cfg["cookies_file"])

    print("Synty Bulk Downloader")
    print("=" * 56)
    print("Checking authentication...", end=" ", flush=True)

    customer_id = get_customer_id(session)
    if not customer_id:
        print("FAILED\n")
        print("Could not authenticate. Your session cookies may be invalid or expired.")
        print()
        print("Steps to fix:")
        print("  1. Log into https://syntystore.com in your browser")
        print("  2. Use 'Get cookies.txt LOCALLY' extension to export cookies")
        print("     (click the extension icon while on syntystore.com)")
        print("  3. Replace cookies.txt at:", cfg["cookies_file"])
        sys.exit(1)

    print(f"OK  (customer #{customer_id})")

    if args.check_auth:
        print("Session is valid. You're good to go.")
        sys.exit(0)

    formats = cfg["formats"]
    # Normalise: if 'all' appears anywhere, collapse the list to ['all'] for clarity
    if FORMAT_ALL in formats:
        formats = [FORMAT_ALL]

    print(f"Download directory : {cfg['download_dir']}")
    print(f"Formats            : {', '.join(formats)}")
    print(f"Skip existing      : {cfg['skip_existing']}")
    if args.dry_run:
        print("Mode               : DRY RUN (no files will be written)")
    print()

    # ---- Discover packs -------------------------------------------------------
    print("Fetching pack listing...")
    packs = fetch_pack_listing(session, customer_id)
    if not packs:
        print("\nNo packs found in your library.")
        print("  - Check that your account has direct Synty Store purchases")
        print("  - If the library loaded in your browser but not here,")
        print("    re-export cookies.txt and try again.")
        sys.exit(0)

    print(f"  Found {len(packs)} pack(s) in your library")
    print()

    # ---- Download loop --------------------------------------------------------
    download_dir = Path(cfg["download_dir"])
    skip_existing = cfg["skip_existing"]
    delay = float(cfg.get("delay_between_downloads", 1.5))

    stats = {"downloaded": 0, "skipped": 0, "filtered": 0, "failed": 0}

    for pack in packs:
        title = pack["title"]
        pack_dir = download_dir / _safe_name(title)

        print(f"[PACK ] {title}")

        files = fetch_pack_files(session, title, pack["order_items_url"])
        if not files:
            print("  (no files found)")
            continue

        for finfo in files:
            fmt = finfo["format"]
            stem = finfo["name_stem"]
            url = finfo["url"]
            size_str = finfo.get("size_str", "")

            label = f"  [{fmt.upper():<6}] {stem}"
            if size_str:
                label += f"  ({size_str})"

            if not _wanted_format(fmt, formats):
                print(f"  [SKIP  ] {stem}  (format '{fmt}' not in config)")
                stats["filtered"] += 1
                continue

            if skip_existing:
                existing = _find_existing(pack_dir, stem)
                if existing:
                    print(f"  [SKIP  ] {existing.name}")
                    stats["skipped"] += 1
                    continue

            print(label)
            ok = download_file(session, url, pack_dir, stem, args.dry_run, delay)
            if ok:
                stats["downloaded"] += 1
            else:
                stats["failed"] += 1

        time.sleep(0.5)   # brief pause between packs

    print()
    print("=" * 56)
    print("Finished!")
    print(f"  Downloaded  : {stats['downloaded']}")
    print(f"  Skipped     : {stats['skipped']}")
    print(f"  Filtered    : {stats['filtered']}  (format not in config)")
    print(f"  Failed      : {stats['failed']}")
    if stats["failed"]:
        print("\n  Re-run the script to retry failed downloads.")


if __name__ == "__main__":
    main()

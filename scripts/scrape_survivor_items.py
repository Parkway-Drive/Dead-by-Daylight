#!/usr/bin/env python3
"""Scrape Survivor Items from the official Dead by Daylight wiki (wiki.gg).

Strategy
- Discover item pages by traversing Category:Items recursively via MediaWiki API.
- For each item page, use action=parse to get rendered HTML.
- Extract the primary item table (Icon / Description / Cost).

Output
- Writes JSON to output/survivor_items.json by default.

Notes
- wiki.gg blocks requests with missing/empty User-Agent (HTTP 403).
- Please be polite: keep request rate low.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

from bs4 import BeautifulSoup

WIKI_BASE = "https://deadbydaylight.wiki.gg"
API_ENDPOINT = f"{WIKI_BASE}/api.php"

DEFAULT_UA = "dbd-wiki-scraper/0.1 (wiki.gg; educational; contact: none)"


class FetchError(RuntimeError):
    pass


def _request(url: str, *, user_agent: str, timeout_s: int = 30) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": user_agent})
    try:
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            return resp.read()
    except Exception as e:  # noqa: BLE001
        raise FetchError(f"Request failed: {url} ({e})") from e


def fetch_json(params: Dict[str, Any], *, user_agent: str, timeout_s: int = 30) -> Dict[str, Any]:
    url = f"{API_ENDPOINT}?{urllib.parse.urlencode(params)}"
    raw = _request(url, user_agent=user_agent, timeout_s=timeout_s)
    try:
        return json.loads(raw.decode("utf-8"))
    except Exception as e:  # noqa: BLE001
        raise FetchError(f"Failed to decode JSON for: {url} ({e})") from e


def normalize_category_name(cat: str) -> str:
    # MediaWiki returns categories without the Category: prefix in parse output.
    return cat.replace("_", " ").strip()


_SPACE_BEFORE_PUNCT_RE = re.compile(r"\s+([,.;:!?%])")
_MULTISPACE_RE = re.compile(r"\s+")


def clean_text(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    s = _MULTISPACE_RE.sub(" ", s).strip()
    # Remove spaces before punctuation (common after stripping spans).
    s = _SPACE_BEFORE_PUNCT_RE.sub(r"\1", s)
    return s or None


def join_url(path_or_url: str) -> str:
    if not path_or_url:
        return path_or_url
    if path_or_url.startswith("http://") or path_or_url.startswith("https://"):
        return path_or_url
    if path_or_url.startswith("/"):
        return f"{WIKI_BASE}{path_or_url}"
    return f"{WIKI_BASE}/{path_or_url}"


def canonical_page_url(title: str) -> str:
    # wiki.gg uses spaces as underscores in URLs typically.
    return f"{WIKI_BASE}/wiki/{urllib.parse.quote(title.replace(' ', '_'))}"


def iter_category_members(
    category_title: str,
    *,
    user_agent: str,
    cmtype: str = "page|subcat",
    sleep_s: float = 0.2,
) -> Iterable[Dict[str, Any]]:
    cont: Dict[str, Any] = {}
    while True:
        params: Dict[str, Any] = {
            "action": "query",
            "list": "categorymembers",
            "cmtitle": category_title,
            "cmtype": cmtype,
            "cmlimit": "max",
            "format": "json",
        }
        params.update(cont)
        data = fetch_json(params, user_agent=user_agent)
        members = data.get("query", {}).get("categorymembers", [])
        for m in members:
            yield m
        cont = data.get("continue", {})
        if not cont:
            break
        time.sleep(sleep_s)


def discover_item_pages(*, user_agent: str, sleep_s: float) -> List[str]:
    """Traverse Category:Items and return unique main-namespace item page titles."""

    seen_cats: Set[str] = set()
    queue: List[str] = ["Category:Items"]
    pages: Set[str] = set()

    while queue:
        cat = queue.pop(0)
        if cat in seen_cats:
            continue
        seen_cats.add(cat)

        for m in iter_category_members(cat, user_agent=user_agent, sleep_s=sleep_s):
            ns = m.get("ns")
            title = m.get("title")
            if not title:
                continue
            # ns 14: Category
            if ns == 14:
                queue.append(title)
                continue
            # ns 0: main namespace pages
            if ns == 0:
                pages.add(title)

        time.sleep(sleep_s)

    return sorted(pages)


@dataclass
class ParsedCost:
    amount: Optional[int]
    currency: Optional[str]


_CURRENCY_HINTS: Tuple[Tuple[str, str], ...] = (
    ("bloodpoints", "bloodpoints"),
    ("bp", "bloodpoints"),
    ("iridescent shards", "iridescent_shards"),
    ("shards", "iridescent_shards"),
    ("auric cells", "auric_cells"),
    ("cells", "auric_cells"),
)


def parse_cost_cell(td) -> ParsedCost:
    if td is None:
        return ParsedCost(None, None)

    # Try to detect currency by icon alt/src and/or visible text.
    text = " ".join(td.get_text(" ", strip=True).split())

    currency: Optional[str] = None
    for img in td.select("img"):
        alt = (img.get("alt") or "").lower()
        src = (img.get("src") or "").lower()
        blob = f"{alt} {src}"
        if "bloodpoints" in blob:
            currency = "bloodpoints"
            break
        if "shards" in blob:
            currency = "iridescent_shards"
            break
        if "auric" in blob or "cells" in blob:
            currency = "auric_cells"
            break

    if currency is None:
        lower = text.lower()
        for needle, mapped in _CURRENCY_HINTS:
            if needle in lower:
                currency = mapped
                break

    m = re.search(r"(\d[\d,]*)", text)
    amount = int(m.group(1).replace(",", "")) if m else None

    return ParsedCost(amount=amount, currency=currency)


def extract_description_parts(desc_td) -> Dict[str, Any]:
    """Extract summary, effects (bullets), and flavor text from description cell."""
    if desc_td is None:
        return {"summary": None, "effects": [], "flavor_text": None, "raw_text": None}

    # Clone-ish approach: work off soup fragment.
    # Summary: text up to first <ul> (if present)
    summary_parts: List[str] = []
    effects: List[str] = []
    flavor_text: Optional[str] = None

    ul = desc_td.find("ul")
    if ul is not None:
        # Gather sibling text before ul
        for node in list(desc_td.children):
            if getattr(node, "name", None) == "ul":
                break
            # Skip purely whitespace nodes, but preserve spacing between fragments.
            if hasattr(node, "get_text"):
                txt = node.get_text(" ", strip=True)
            else:
                txt = str(node).strip()
            txt = clean_text(txt)
            if txt:
                summary_parts.append(txt)
        for li in ul.find_all("li"):
            li_txt = clean_text(li.get_text(" ", strip=True))
            if li_txt:
                effects.append(li_txt)

    # Flavor text: often in <p><i>"..."</i></p>
    for ital in desc_td.select("p i"):
        t = clean_text(ital.get_text(" ", strip=True))
        if t and '"' in t:
            flavor_text = t.strip()
            break

    raw_text = clean_text(desc_td.get_text(" ", strip=True))
    summary = clean_text(" ".join(summary_parts))
    flavor_text = clean_text(flavor_text)

    return {"summary": summary, "effects": effects, "flavor_text": flavor_text, "raw_text": raw_text}


def parse_item_page(title: str, *, user_agent: str, sleep_s: float) -> Dict[str, Any]:
    params = {
        "action": "parse",
        "page": title,
        "prop": "text|categories",
        "format": "json",
    }
    data = fetch_json(params, user_agent=user_agent)
    parse = data.get("parse")
    if not parse:
        raise FetchError(f"Missing parse output for page: {title}")

    html = parse.get("text", {}).get("*") or ""
    cats = [normalize_category_name(c.get("*", "")) for c in parse.get("categories", []) if c.get("*")]
    # Drop noisy maintenance categories.
    cats = [c for c in cats if not c.lower().startswith("pages using ")]

    soup = BeautifulSoup(html, "lxml")

    # Find the main item table; on item pages it is typically the first wikitable.
    table = soup.select_one("table.wikitable")

    icon_url: Optional[str] = None
    description: Dict[str, Any] = {"summary": None, "effects": [], "flavor_text": None, "raw_text": None}
    cost = ParsedCost(None, None)

    if table is not None:
        # First data row contains icon/description/cost.
        row = table.select_one("tbody tr + tr")  # skip header row
        if row is None:
            rows = table.select("tr")
            row = rows[1] if len(rows) > 1 else None

        if row is not None:
            cells = row.find_all(["td", "th"], recursive=False)
            if len(cells) >= 1:
                img = cells[0].select_one("img")
                if img is not None:
                    icon_url = join_url(img.get("src") or "")
            if len(cells) >= 2:
                description = extract_description_parts(cells[1])
            if len(cells) >= 3:
                cost = parse_cost_cell(cells[2])

    # Rarity inferred from categories, but also keep all categories.
    rarity: Optional[str] = None
    for c in cats:
        # categories look like: "Uncommon Items", "Very Rare Items"...
        if c.lower().endswith(" items") and c.lower() not in {"items"}:
            rarity = c[: -len(" items")].strip()
            break

    time.sleep(sleep_s)

    return {
        "title": title,
        "wiki_url": canonical_page_url(title),
        "rarity": rarity,
        "icon_url": icon_url,
        "cost": {"amount": cost.amount, "currency": cost.currency},
        "description": description,
        "categories": cats,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Scrape Survivor Items from deadbydaylight.wiki.gg")
    ap.add_argument("--out", default="/workspace/output/survivor_items.json", help="Output JSON path")
    ap.add_argument("--user-agent", default=DEFAULT_UA, help="HTTP User-Agent header")
    ap.add_argument("--sleep", type=float, default=0.25, help="Delay between requests (seconds)")
    ap.add_argument("--limit", type=int, default=0, help="Limit number of item pages (0 = no limit)")
    args = ap.parse_args()

    scraped_at = dt.datetime.now(dt.timezone.utc).isoformat()

    titles = discover_item_pages(user_agent=args.user_agent, sleep_s=args.sleep)
    if args.limit and args.limit > 0:
        titles = titles[: args.limit]

    items: List[Dict[str, Any]] = []
    errors: List[Dict[str, str]] = []

    for i, title in enumerate(titles, start=1):
        try:
            item = parse_item_page(title, user_agent=args.user_agent, sleep_s=args.sleep)
            items.append(item)
        except Exception as e:  # noqa: BLE001
            errors.append({"title": title, "error": str(e)})
        if i % 10 == 0:
            print(f"[{i}/{len(titles)}] scraped...")

    payload = {
        "source": WIKI_BASE,
        "scraped_at": scraped_at,
        "count": len(items),
        "items": items,
        "errors": errors,
    }

    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
        f.write("\n")

    print(f"Wrote {len(items)} items to {args.out} (errors: {len(errors)})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

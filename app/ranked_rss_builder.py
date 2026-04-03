import os
import re
import json
import math
import html
import hashlib
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime, format_datetime

import requests
import feedparser


# ============================================================
# CONFIG
# ============================================================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE_DIR)

SOURCE_RSS_URL = "https://www.crash.net/rss/f1"
OUTPUT_RSS_FILE = os.path.join(PROJECT_ROOT, "docs", "ranked_f1_feed.xml")
USED_URLS_FILE = os.path.join(PROJECT_ROOT, "data", "used_urls.json")
# Max number of items to include in output feed
MAX_ITEMS = 20

# Image rules
TOP_IMAGE_ITEMS = 5  # items 1-5 keep image data

# Freshness settings
FRESH_HOURS = 18
STALE_HOURS = 24

# Behaviour for older items:
# - items older than STALE_HOURS get a very large penalty
# - if there are not enough good items, they can still be used as fallback
OLDER_THAN_24H_PENALTY = 1000
BETWEEN_18_AND_24H_PENALTY = 20

# Previously used URL handling
USED_URL_PENALTY = 1000  # high penalty so they usually drop out

# Duplicate suppression
TITLE_SIMILARITY_THRESHOLD = 0.72
DUPLICATE_TOPIC_PENALTY = 15

# Ranking weights
WEIGHTS = {
    "top_tier_keyword": 12,
    "mid_tier_keyword": 7,
    "low_tier_keyword": 3,
    "has_image": 4,
    "recent_bonus": 10,
    "very_recent_bonus": 5,   # extra if very fresh
    "title_length_good": 2,
    "title_length_bad": -2,
}

# F1-specific keyword buckets
TOP_TIER_KEYWORDS = [
    "verstappen",
    "hamilton",
    "leclerc",
    "norris",
    "piastri",
    "russell",
    "alonso",
    "ferrari",
    "red bull",
    "mercedes",
    "mclaren",
]

MID_TIER_KEYWORDS = [
    "suzuka",
    "japanese gp",
    "fia",
    "qualifying",
    "grid penalty",
    "crash",
    "pole",
    "podium",
    "practice",
    "team principal",
    "stewards",
    "disqualified",
]

LOW_TIER_KEYWORDS = [
    "rookie",
    "reserve",
    "test",
    "tyre",
    "strategy",
    "upgrade",
    "contract",
    "rumour",
]

# Output channel metadata
CHANNEL_TITLE = "Crash F1 Ranked Feed"
CHANNEL_LINK = "https://www.crash.net/"
CHANNEL_DESCRIPTION = "Editorially ranked F1 RSS feed for newsletter consumption"
CHANNEL_LANGUAGE = "en-gb"


# ============================================================
# HELPERS
# ============================================================

def normalise_whitespace(text):
    if not text:
        return ""
    return re.sub(r"\s+", " ", text).strip()


def strip_html(text):
    if not text:
        return ""
    # Remove tags
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text)
    return normalise_whitespace(text)


def xml_escape(text):
    if text is None:
        return ""
    return html.escape(str(text), quote=True)


def wrap_cdata(text):
    """
    Safely wrap text in CDATA, splitting any existing ]]> markers.
    """
    if text is None:
        text = ""
    return "<![CDATA[" + text.replace("]]>", "]]]]><![CDATA[>") + "]]>"


def load_used_urls(path):
    if not os.path.exists(path):
        return set()

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return set(data)
        if isinstance(data, dict) and "used_urls" in data and isinstance(data["used_urls"], list):
            return set(data["used_urls"])
    except Exception:
        pass

    return set()


def save_used_urls(path, urls):
    payload = {
        "updated_at_utc": datetime.now(timezone.utc).isoformat(),
        "used_urls": sorted(set(urls)),
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)


def parse_pub_date(entry):
    """
    Try multiple date fields commonly found in feedparser entries.
    Return timezone-aware UTC datetime where possible.
    """
    date_candidates = []

    for field in ("published", "updated", "created"):
        if field in entry:
            date_candidates.append(entry.get(field))

    for field_struct in ("published_parsed", "updated_parsed", "created_parsed"):
        struct_val = entry.get(field_struct)
        if struct_val:
            try:
                return datetime(*struct_val[:6], tzinfo=timezone.utc)
            except Exception:
                pass

    for value in date_candidates:
        if not value:
            continue
        try:
            dt = parsedate_to_datetime(value)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            continue

    return None


def format_rss_date(dt):
    if dt is None:
        dt = datetime.now(timezone.utc)
    return format_datetime(dt)


def tokenise_title_for_similarity(title):
    """
    Simple normalisation for duplicate-topic suppression.
    """
    title = title.lower()
    title = re.sub(r"[^a-z0-9\s]", " ", title)
    tokens = [t for t in title.split() if len(t) > 2]
    return set(tokens)


def jaccard_similarity(a_tokens, b_tokens):
    if not a_tokens or not b_tokens:
        return 0.0
    intersection = len(a_tokens.intersection(b_tokens))
    union = len(a_tokens.union(b_tokens))
    if union == 0:
        return 0.0
    return intersection / union


def extract_best_image(entry):
    """
    Try to extract an image URL from common RSS/media patterns.
    """
    # 1. Enclosures
    enclosures = entry.get("enclosures", [])
    for enc in enclosures:
        href = enc.get("href") or enc.get("url")
        enc_type = (enc.get("type") or "").lower()
        if href and ("image" in enc_type or looks_like_image_url(href)):
            return href

    # 2. media_content
    media_content = entry.get("media_content", [])
    for media in media_content:
        url = media.get("url")
        media_type = (media.get("type") or "").lower()
        if url and ("image" in media_type or looks_like_image_url(url)):
            return url

    # 3. media_thumbnail
    media_thumbnail = entry.get("media_thumbnail", [])
    for media in media_thumbnail:
        url = media.get("url")
        if url and looks_like_image_url(url):
            return url

    # 4. links rel=enclosure
    links = entry.get("links", [])
    for link in links:
        href = link.get("href")
        rel = (link.get("rel") or "").lower()
        link_type = (link.get("type") or "").lower()
        if href and rel == "enclosure" and ("image" in link_type or looks_like_image_url(href)):
            return href

    # 5. Parse <img src=""> from summary/description/content
    possible_html_fields = []

    summary = entry.get("summary")
    if summary:
        possible_html_fields.append(summary)

    description = entry.get("description")
    if description:
        possible_html_fields.append(description)

    contents = entry.get("content", [])
    for c in contents:
        if isinstance(c, dict) and c.get("value"):
            possible_html_fields.append(c["value"])

    for blob in possible_html_fields:
        match = re.search(r'<img[^>]+src=["\']([^"\']+)["\']', blob, flags=re.I)
        if match:
            return match.group(1)

    return None


def looks_like_image_url(url):
    if not url:
        return False
    url_lower = url.lower()
    image_extensions = [".jpg", ".jpeg", ".png", ".webp", ".gif", ".avif"]
    return any(ext in url_lower for ext in image_extensions)


def get_description_text(entry):
    """
    Prefer summary/description, stripped to plain text.
    """
    for field in ("summary", "description"):
        value = entry.get(field)
        if value:
            return strip_html(value)

    contents = entry.get("content", [])
    for c in contents:
        if isinstance(c, dict) and c.get("value"):
            return strip_html(c["value"])

    return ""


def count_keyword_hits(text, keywords):
    text_lower = text.lower()
    hits = 0
    for kw in keywords:
        kw_escaped = re.escape(kw.lower())
        pattern = r"(?<!\w)" + kw_escaped + r"(?!\w)"
        if re.search(pattern, text_lower):
            hits += 1
    return hits


def score_item(item, now_utc, used_urls):
    """
    Returns:
    - score
    - debug reasons list
    """
    score = 0
    reasons = []

    combined_text = f"{item['title']} {item['description']}".strip().lower()

    # Keyword scoring
    top_hits = count_keyword_hits(combined_text, TOP_TIER_KEYWORDS)
    mid_hits = count_keyword_hits(combined_text, MID_TIER_KEYWORDS)
    low_hits = count_keyword_hits(combined_text, LOW_TIER_KEYWORDS)

    if top_hits:
        added = top_hits * WEIGHTS["top_tier_keyword"]
        score += added
        reasons.append(f"top-tier keywords x{top_hits} (+{added})")

    if mid_hits:
        added = mid_hits * WEIGHTS["mid_tier_keyword"]
        score += added
        reasons.append(f"mid-tier keywords x{mid_hits} (+{added})")

    if low_hits:
        added = low_hits * WEIGHTS["low_tier_keyword"]
        score += added
        reasons.append(f"low-tier keywords x{low_hits} (+{added})")

    # Image bonus
    if item["image_url"]:
        score += WEIGHTS["has_image"]
        reasons.append(f"has image (+{WEIGHTS['has_image']})")

    # Freshness
    if item["pub_date"]:
        age = now_utc - item["pub_date"]
        age_hours = age.total_seconds() / 3600

        if age_hours <= FRESH_HOURS:
            score += WEIGHTS["recent_bonus"]
            reasons.append(f"within {FRESH_HOURS}h (+{WEIGHTS['recent_bonus']})")

            if age_hours <= 6:
                score += WEIGHTS["very_recent_bonus"]
                reasons.append(f"within 6h (+{WEIGHTS['very_recent_bonus']})")

        elif FRESH_HOURS < age_hours <= STALE_HOURS:
            score -= BETWEEN_18_AND_24H_PENALTY
            reasons.append(f"between {FRESH_HOURS}-{STALE_HOURS}h (-{BETWEEN_18_AND_24H_PENALTY})")

        else:
            score -= OLDER_THAN_24H_PENALTY
            reasons.append(f"older than {STALE_HOURS}h (-{OLDER_THAN_24H_PENALTY})")
    else:
        # Unknown date gets a small penalty
        score -= 10
        reasons.append("missing pub date (-10)")

    # Title quality heuristic
    title_len = len(item["title"])
    if 30 <= title_len <= 110:
        score += WEIGHTS["title_length_good"]
        reasons.append(f"good title length (+{WEIGHTS['title_length_good']})")
    else:
        score += WEIGHTS["title_length_bad"]
        reasons.append(f"awkward title length ({WEIGHTS['title_length_bad']})")

    # Previously used URL penalty
    if item["link"] in used_urls:
        score -= USED_URL_PENALTY
        reasons.append(f"previously used URL (-{USED_URL_PENALTY})")

    return score, reasons


def deduplicate_and_rank(items, now_utc, used_urls, max_items):
    """
    Scores everything, sorts, then suppresses duplicate topics.
    """
    scored = []
    for item in items:
        score, reasons = score_item(item, now_utc, used_urls)
        item["score"] = score
        item["score_reasons"] = reasons
        scored.append(item)

    # Highest score first, newest first as tie-breaker
    scored.sort(
        key=lambda x: (
            x["score"],
            x["pub_date"].timestamp() if x["pub_date"] else 0
        ),
        reverse=True
    )

    selected = []
    selected_title_tokens = []

    for item in scored:
        tokens = tokenise_title_for_similarity(item["title"])

        similar_found = False
        highest_similarity = 0.0

        for existing_tokens in selected_title_tokens:
            sim = jaccard_similarity(tokens, existing_tokens)
            highest_similarity = max(highest_similarity, sim)
            if sim >= TITLE_SIMILARITY_THRESHOLD:
                similar_found = True
                break

        # If topic is too similar, suppress unless we really need fallback
        if similar_found:
            item["score"] -= DUPLICATE_TOPIC_PENALTY
            item["score_reasons"].append(
                f"duplicate-topic suppression ({highest_similarity:.2f}) (-{DUPLICATE_TOPIC_PENALTY})"
            )

            # Only keep it if we still do not have enough items and it remains viable
            if len(selected) < max_items // 2 and item["score"] > -900:
                selected.append(item)
                selected_title_tokens.append(tokens)
        else:
            selected.append(item)
            selected_title_tokens.append(tokens)

        if len(selected) >= max_items:
            break

    # If aggressive suppression left us short, fill from remaining scored items
    if len(selected) < max_items:
        selected_links = {x["link"] for x in selected}
        for item in scored:
            if item["link"] in selected_links:
                continue
            selected.append(item)
            selected_links.add(item["link"])
            if len(selected) >= max_items:
                break

    return selected[:max_items]


def build_item_xml(item, include_images):
    title = xml_escape(item["title"])
    link = xml_escape(item["link"])
    pub_date = xml_escape(format_rss_date(item["pub_date"]))

    description_plain = item["description"] or ""

    parts = []
    parts.append("    <item>")
    parts.append(f"      <title>{title}</title>")
    parts.append(f"      <link>{link}</link>")
    parts.append(f"      <guid isPermaLink=\"true\">{link}</guid>")
    parts.append(f"      <pubDate>{pub_date}</pubDate>")

    if include_images and item["image_url"]:
        image_url_escaped = xml_escape(item["image_url"])
        description_html = (
            f'<img src="{image_url_escaped}" alt="{title}" />'
            f"<p>{html.escape(description_plain)}</p>"
        )
        parts.append(f"      <description>{wrap_cdata(description_html)}</description>")
        parts.append(
            f"      <enclosure url=\"{image_url_escaped}\" type=\"image/jpeg\" />"
        )
        parts.append(
            f"      <media:content url=\"{image_url_escaped}\" medium=\"image\" type=\"image/jpeg\" />"
        )
    else:
        parts.append(f"      <description>{wrap_cdata(html.escape(description_plain))}</description>")

    parts.append("    </item>")
    return "\n".join(parts)


def build_rss_xml(channel_title, channel_link, channel_description, items):
    now_utc = datetime.now(timezone.utc)

    lines = []
    lines.append('<?xml version="1.0" encoding="UTF-8"?>')
    lines.append('<rss version="2.0" xmlns:media="http://search.yahoo.com/mrss/">')
    lines.append("  <channel>")
    lines.append(f"    <title>{xml_escape(channel_title)}</title>")
    lines.append(f"    <link>{xml_escape(channel_link)}</link>")
    lines.append(f"    <description>{xml_escape(channel_description)}</description>")
    lines.append(f"    <language>{xml_escape(CHANNEL_LANGUAGE)}</language>")
    lines.append(f"    <lastBuildDate>{xml_escape(format_rss_date(now_utc))}</lastBuildDate>")

    for index, item in enumerate(items, start=1):
        include_images = index <= TOP_IMAGE_ITEMS
        lines.append(build_item_xml(item, include_images))

    lines.append("  </channel>")
    lines.append("</rss>")

    return "\n".join(lines)


def fetch_and_parse_feed(url):
    """
    Fetches the feed with requests, then parses with feedparser.
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) RankedRSSBuilder/1.0"
    }

    response = requests.get(url, headers=headers, timeout=20)
    response.raise_for_status()

    parsed = feedparser.parse(response.content)

    if parsed.bozo and not parsed.entries:
        raise ValueError("Feed could not be parsed cleanly and contained no usable entries.")

    return parsed


def convert_entries_to_items(parsed_feed):
    items = []

    for entry in parsed_feed.entries:
        title = normalise_whitespace(entry.get("title", "")).strip()
        link = normalise_whitespace(entry.get("link", "")).strip()

        if not title or not link:
            continue

        pub_date = parse_pub_date(entry)
        description = get_description_text(entry)
        image_url = extract_best_image(entry)

        item = {
            "title": title,
            "link": link,
            "pub_date": pub_date,
            "description": description,
            "image_url": image_url,
        }
        items.append(item)

    return items


def main():
    os.makedirs(os.path.dirname(OUTPUT_RSS_FILE), exist_ok=True)
    os.makedirs(os.path.dirname(USED_URLS_FILE), exist_ok=True)

    print(f"Fetching feed: {SOURCE_RSS_URL}")
    used_urls = load_used_urls(USED_URLS_FILE)

    try:
        parsed_feed = fetch_and_parse_feed(SOURCE_RSS_URL)
    except requests.RequestException as e:
        print(f"ERROR: Could not fetch source feed: {e}")
        return
    except Exception as e:
        print(f"ERROR: Could not parse source feed: {e}")
        return

    items = convert_entries_to_items(parsed_feed)

    if not items:
        print("ERROR: No usable items found in the source feed.")
        return

    now_utc = datetime.now(timezone.utc)
    ranked_items = deduplicate_and_rank(
        items=items,
        now_utc=now_utc,
        used_urls=used_urls,
        max_items=MAX_ITEMS,
    )

    rss_xml = build_rss_xml(
        channel_title=CHANNEL_TITLE,
        channel_link=CHANNEL_LINK,
        channel_description=CHANNEL_DESCRIPTION,
        items=ranked_items,
    )

    try:
        with open(OUTPUT_RSS_FILE, "w", encoding="utf-8", newline="\n") as f:
            f.write(rss_xml)
    except Exception as e:
        print(f"ERROR: Could not write RSS file: {e}")
        return

    # Save selected URLs back to used URL store
    selected_urls = [item["link"] for item in ranked_items]
    merged_used_urls = set(used_urls).union(selected_urls)

    try:
        save_used_urls(USED_URLS_FILE, merged_used_urls)
    except Exception as e:
        print(f"WARNING: RSS written, but could not save used URLs file: {e}")

    print(f"Done. Wrote ranked RSS to: {OUTPUT_RSS_FILE}")
    print(f"Stored used URLs in: {USED_URLS_FILE}")
    print("Top selected stories:")

    for i, item in enumerate(ranked_items[:10], start=1):
        age_text = "unknown age"
        if item["pub_date"]:
            age_hours = (now_utc - item["pub_date"]).total_seconds() / 3600
            age_text = f"{age_hours:.1f}h old"

        print(f"{i}. {item['title']}")
        print(f"   Score: {item.get('score')}")
        print(f"   Age: {age_text}")
        print(f"   Image: {'yes' if item.get('image_url') else 'no'}")
        print(f"   URL: {item['link']}")
        print(f"   Reasons: {', '.join(item.get('score_reasons', []))}")
        print()


if __name__ == "__main__":
    main()
import os
import re
import json
import html
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime, format_datetime

import requests
import feedparser


# ============================================================
# PATHS / CONFIG
# ============================================================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE_DIR)

SOURCE_RSS_URL = "https://www.crash.net/rss/f1"
OUTPUT_RSS_FILE = os.path.join(PROJECT_ROOT, "docs", "ranked_f1_feed.xml")
USED_URLS_FILE = os.path.join(PROJECT_ROOT, "data", "used_urls.json")

# Optional debug output
WRITE_DEBUG_JSON = True
DEBUG_JSON_FILE = os.path.join(PROJECT_ROOT, "data", "ranked_items_debug.json")

# Max number of items in the RSS output
MAX_ITEMS = 20

# Top 5 are image-led in RSS
TOP_IMAGE_ITEMS = 5

# Freshness rules
FRESH_HOURS = 18
STALE_HOURS = 24
BETWEEN_18_AND_24H_PENALTY = 20
OLDER_THAN_24H_PENALTY = 100

# Previously used URL handling
USED_URL_PENALTY = 100

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
    "very_recent_bonus": 5,
    "title_length_good": 2,
    "title_length_bad": -2,
}

# F1 keyword buckets
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

# RSS channel metadata
CHANNEL_TITLE = "Crash F1 Ranked Feed"
CHANNEL_LINK = "https://www.crash.net/"
CHANNEL_DESCRIPTION = "Editorially ranked F1 RSS feed for newsletter consumption"
CHANNEL_LANGUAGE = "en-gb"

# Summary trimming
TOP_SUMMARY_WORDS = 32
LOWER_SUMMARY_WORDS = 14

# ============================================================
# BUTTONDOWN API CONFIG (OPTIONAL)
# ============================================================

ENABLE_BUTTONDOWN_DRAFT = os.getenv("ENABLE_BUTTONDOWN_DRAFT", "false").lower() == "true"
BUTTONDOWN_API_KEY = os.getenv("c64e18d0-94d1-41d3-a72c-5d9b12b8390d", "").strip()
BUTTONDOWN_API_URL = "https://api.buttondown.com/v1/emails"

# Draft subject options
BUTTONDOWN_SUBJECT_PREFIX = "Crash F1 Briefing"
BUTTONDOWN_INCLUDE_DATE_IN_SUBJECT = True

# Draft metadata / archive behaviour
BUTTONDOWN_EMAIL_TYPE = "public"
BUTTONDOWN_ARCHIVAL_MODE = "enabled"
BUTTONDOWN_COMMENTING_MODE = "disabled"
BUTTONDOWN_REVIEW_MODE = "disabled"

# If true, only create the draft if at least this many ranked items exist
BUTTONDOWN_MIN_ITEMS = 5

# Buttondown body uses HTML ("fancy" mode)
BUTTONDOWN_INTRO_TEXT = "Here are the key F1 stories from the latest ranking run."
BUTTONDOWN_FOOTER_TEXT = "You're receiving this briefing because you're subscribed to our motorsport updates."

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
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text)
    return normalise_whitespace(text)


def xml_escape(text):
    if text is None:
        return ""
    return html.escape(str(text), quote=True)


def wrap_cdata(text):
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
    for field_struct in ("published_parsed", "updated_parsed", "created_parsed"):
        struct_val = entry.get(field_struct)
        if struct_val:
            try:
                return datetime(*struct_val[:6], tzinfo=timezone.utc)
            except Exception:
                pass

    for field in ("published", "updated", "created"):
        value = entry.get(field)
        if value:
            try:
                dt = parsedate_to_datetime(value)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.astimezone(timezone.utc)
            except Exception:
                pass

    return None


def format_rss_date(dt):
    if dt is None:
        dt = datetime.now(timezone.utc)
    return format_datetime(dt)


def tokenise_title_for_similarity(title):
    title = title.lower()
    title = re.sub(r"[^a-z0-9\s]", " ", title)
    return {t for t in title.split() if len(t) > 2}


def jaccard_similarity(a_tokens, b_tokens):
    if not a_tokens or not b_tokens:
        return 0.0
    intersection = len(a_tokens.intersection(b_tokens))
    union = len(a_tokens.union(b_tokens))
    if union == 0:
        return 0.0
    return intersection / union


def looks_like_image_url(url):
    if not url:
        return False
    url_lower = url.lower()
    image_extensions = [".jpg", ".jpeg", ".png", ".webp", ".gif", ".avif"]
    return any(ext in url_lower for ext in image_extensions)


def guess_mime_type_from_url(url):
    if not url:
        return "image/jpeg"
    u = url.lower()
    if ".png" in u:
        return "image/png"
    if ".webp" in u:
        return "image/webp"
    if ".gif" in u:
        return "image/gif"
    if ".avif" in u:
        return "image/avif"
    return "image/jpeg"


def extract_best_image(entry):
    enclosures = entry.get("enclosures", [])
    for enc in enclosures:
        href = enc.get("href") or enc.get("url")
        enc_type = (enc.get("type") or "").lower()
        if href and ("image" in enc_type or looks_like_image_url(href)):
            return href

    media_content = entry.get("media_content", [])
    for media in media_content:
        url = media.get("url")
        media_type = (media.get("type") or "").lower()
        if url and ("image" in media_type or looks_like_image_url(url)):
            return url

    media_thumbnail = entry.get("media_thumbnail", [])
    for media in media_thumbnail:
        url = media.get("url")
        if url and looks_like_image_url(url):
            return url

    links = entry.get("links", [])
    for link in links:
        href = link.get("href")
        rel = (link.get("rel") or "").lower()
        link_type = (link.get("type") or "").lower()
        if href and rel == "enclosure" and ("image" in link_type or looks_like_image_url(href)):
            return href

    possible_html_fields = []

    for field in ("summary", "description"):
        value = entry.get(field)
        if value:
            possible_html_fields.append(value)

    contents = entry.get("content", [])
    for c in contents:
        if isinstance(c, dict) and c.get("value"):
            possible_html_fields.append(c["value"])

    for blob in possible_html_fields:
        match = re.search(r'<img[^>]+src=["\']([^"\']+)["\']', blob, flags=re.I)
        if match:
            return match.group(1)

    return None


def get_description_text(entry):
    for field in ("summary", "description"):
        value = entry.get(field)
        if value:
            return strip_html(value)

    contents = entry.get("content", [])
    for c in contents:
        if isinstance(c, dict) and c.get("value"):
            return strip_html(c["value"])

    return ""


def trim_words(text, max_words):
    text = normalise_whitespace(text)
    if not text:
        return ""
    words = text.split()
    if len(words) <= max_words:
        return text
    return " ".join(words[:max_words]).rstrip(" ,;:.") + "…"


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
    score = 0
    reasons = []

    combined_text = f"{item['title']} {item['description']}".strip().lower()

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

    if item["image_url"]:
        score += WEIGHTS["has_image"]
        reasons.append(f"has image (+{WEIGHTS['has_image']})")

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
        score -= 10
        reasons.append("missing pub date (-10)")

    title_len = len(item["title"])
    if 30 <= title_len <= 110:
        score += WEIGHTS["title_length_good"]
        reasons.append(f"good title length (+{WEIGHTS['title_length_good']})")
    else:
        score += WEIGHTS["title_length_bad"]
        reasons.append(f"awkward title length ({WEIGHTS['title_length_bad']})")

    if item["link"] in used_urls:
        score -= USED_URL_PENALTY
        reasons.append(f"previously used URL (-{USED_URL_PENALTY})")

    return score, reasons


def deduplicate_and_rank(items, now_utc, used_urls, max_items):
    scored = []
    for item in items:
        score, reasons = score_item(item, now_utc, used_urls)
        item["score"] = score
        item["score_reasons"] = reasons
        scored.append(item)

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

        if similar_found:
            item["score"] -= DUPLICATE_TOPIC_PENALTY
            item["score_reasons"].append(
                f"duplicate-topic suppression ({highest_similarity:.2f}) (-{DUPLICATE_TOPIC_PENALTY})"
            )
            if len(selected) < max_items // 2 and item["score"] > -900:
                selected.append(item)
                selected_title_tokens.append(tokens)
        else:
            selected.append(item)
            selected_title_tokens.append(tokens)

        if len(selected) >= max_items:
            break

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


def fetch_and_parse_feed(url):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) RankedRSSBuilder/2.0"
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

        items.append({
            "title": title,
            "link": link,
            "pub_date": pub_date,
            "description": description,
            "image_url": image_url,
        })

    return items


# ============================================================
# RSS BUILDING
# ============================================================

def build_description_html(item, include_images, summary_word_limit):
    summary_text = trim_words(item.get("description", ""), summary_word_limit)
    safe_summary = html.escape(summary_text)

    if include_images and item.get("image_url"):
        image_url_escaped = xml_escape(item["image_url"])
        return f'<img src="{image_url_escaped}" alt="" /><p>{safe_summary}</p>'

    return safe_summary


def build_item_xml(item, include_images):
    title = xml_escape(item["title"])
    link = xml_escape(item["link"])
    pub_date = xml_escape(format_rss_date(item["pub_date"]))

    summary_word_limit = TOP_SUMMARY_WORDS if include_images else LOWER_SUMMARY_WORDS
    description_html = build_description_html(item, include_images, summary_word_limit)

    parts = []
    parts.append("    <item>")
    parts.append(f"      <title>{title}</title>")
    parts.append(f"      <link>{link}</link>")
    parts.append(f"      <guid isPermaLink=\"true\">{link}</guid>")
    parts.append(f"      <pubDate>{pub_date}</pubDate>")
    parts.append(f"      <description>{wrap_cdata(description_html)}</description>")

    if include_images and item.get("image_url"):
        image_url_escaped = xml_escape(item["image_url"])
        mime_type = xml_escape(guess_mime_type_from_url(item["image_url"]))
        parts.append(f"      <enclosure url=\"{image_url_escaped}\" type=\"{mime_type}\" />")
        parts.append(
            f"      <media:content url=\"{image_url_escaped}\" medium=\"image\" type=\"{mime_type}\" />"
        )

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


# ============================================================
# BUTTONDOWN DRAFT HTML
# ============================================================

def build_buttondown_subject():
    if BUTTONDOWN_INCLUDE_DATE_IN_SUBJECT:
        date_str = datetime.now().strftime("%d %b %Y")
        return f"{BUTTONDOWN_SUBJECT_PREFIX} | {date_str}"
    return BUTTONDOWN_SUBJECT_PREFIX


def build_buttondown_email_html(items):
    """
    Generates safe-ish one-column HTML for email.
    Top 5 are image-led.
    6+ are text-led.
    """
    pieces = []
    pieces.append("<!-- buttondown-editor-mode: fancy -->")
    pieces.append('<div style="margin:0; padding:0; background:#f4f4f4;">')
    pieces.append('<table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0" style="width:100%; border-collapse:collapse; background:#f4f4f4;">')
    pieces.append("<tr><td align=\"center\" style=\"padding:24px 12px;\">")

    pieces.append('<table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0" style="max-width:600px; width:100%; border-collapse:collapse; background:#ffffff;">')

    # Header
    pieces.append("<tr>")
    pieces.append('<td style="padding:24px 24px 12px 24px; font-family:Arial, Helvetica, sans-serif; font-size:28px; line-height:32px; font-weight:700; color:#111111;">')
    pieces.append("Crash F1 Briefing")
    pieces.append("</td>")
    pieces.append("</tr>")

    pieces.append("<tr>")
    pieces.append('<td style="padding:0 24px 24px 24px; font-family:Arial, Helvetica, sans-serif; font-size:15px; line-height:22px; color:#555555;">')
    pieces.append(html.escape(BUTTONDOWN_INTRO_TEXT))
    pieces.append("</td>")
    pieces.append("</tr>")

    # Top 5
    top_items = items[:TOP_IMAGE_ITEMS]
    lower_items = items[TOP_IMAGE_ITEMS:]

    if top_items:
        pieces.append("<tr>")
        pieces.append('<td style="padding:0 24px 12px 24px; font-family:Arial, Helvetica, sans-serif; font-size:13px; line-height:13px; font-weight:700; letter-spacing:1px; text-transform:uppercase; color:#c8102e;">')
        pieces.append("Top stories")
        pieces.append("</td>")
        pieces.append("</tr>")

    for idx, item in enumerate(top_items, start=1):
        title = html.escape(item["title"])
        link = html.escape(item["link"])
        summary = html.escape(trim_words(item.get("description", ""), TOP_SUMMARY_WORDS))
        image_url = item.get("image_url")

        pieces.append("<tr>")
        pieces.append('<td style="padding:0 24px 24px 24px;">')

        if image_url:
            image_url_escaped = html.escape(image_url)
            pieces.append(
                f'<a href="{link}" style="text-decoration:none;">'
                f'<img src="{image_url_escaped}" alt="" style="display:block; width:100%; height:auto; border:0; margin:0 0 14px 0;" />'
                f'</a>'
            )

        pieces.append(
            f'<div style="font-family:Arial, Helvetica, sans-serif; font-size:24px; line-height:30px; font-weight:700; color:#111111; margin:0 0 10px 0;">'
            f'<a href="{link}" style="color:#111111; text-decoration:none;">{title}</a>'
            f'</div>'
        )

        if summary:
            pieces.append(
                f'<div style="font-family:Arial, Helvetica, sans-serif; font-size:15px; line-height:22px; color:#444444; margin:0 0 12px 0;">'
                f'{summary}'
                f'</div>'
            )

        pieces.append(
            f'<div style="font-family:Arial, Helvetica, sans-serif; font-size:14px; line-height:20px; font-weight:700;">'
            f'<a href="{link}" style="color:#c8102e; text-decoration:none;">Read more</a>'
            f'</div>'
        )

        pieces.append("</td>")
        pieces.append("</tr>")

        if idx < len(top_items):
            pieces.append("<tr>")
            pieces.append('<td style="padding:0 24px 24px 24px;">')
            pieces.append('<div style="border-top:1px solid #e5e5e5; line-height:1px; font-size:1px;">&nbsp;</div>')
            pieces.append("</td>")
            pieces.append("</tr>")

    # More news
    if lower_items:
        pieces.append("<tr>")
        pieces.append('<td style="padding:8px 24px 12px 24px; font-family:Arial, Helvetica, sans-serif; font-size:13px; line-height:13px; font-weight:700; letter-spacing:1px; text-transform:uppercase; color:#c8102e;">')
        pieces.append("More news")
        pieces.append("</td>")
        pieces.append("</tr>")

    for item in lower_items:
        title = html.escape(item["title"])
        link = html.escape(item["link"])
        summary = html.escape(trim_words(item.get("description", ""), LOWER_SUMMARY_WORDS))

        pieces.append("<tr>")
        pieces.append('<td style="padding:0 24px 16px 24px;">')
        pieces.append(
            f'<div style="font-family:Arial, Helvetica, sans-serif; font-size:18px; line-height:24px; font-weight:700; color:#111111; margin:0 0 6px 0;">'
            f'<a href="{link}" style="color:#111111; text-decoration:none;">{title}</a>'
            f'</div>'
        )

        if summary:
            pieces.append(
                f'<div style="font-family:Arial, Helvetica, sans-serif; font-size:14px; line-height:20px; color:#555555; margin:0;">'
                f'{summary}'
                f'</div>'
            )

        pieces.append("</td>")
        pieces.append("</tr>")
        pieces.append("<tr>")
        pieces.append('<td style="padding:0 24px 16px 24px;">')
        pieces.append('<div style="border-top:1px solid #eeeeee; line-height:1px; font-size:1px;">&nbsp;</div>')
        pieces.append("</td>")
        pieces.append("</tr>")

    # Footer
    pieces.append("<tr>")
    pieces.append('<td style="padding:12px 24px 24px 24px; font-family:Arial, Helvetica, sans-serif; font-size:12px; line-height:18px; color:#777777;">')
    pieces.append(html.escape(BUTTONDOWN_FOOTER_TEXT))
    pieces.append("</td>")
    pieces.append("</tr>")

    pieces.append("</table>")
    pieces.append("</td></tr></table>")
    pieces.append("</div>")

    return "".join(pieces)


def create_buttondown_draft(items):
    api_key = BUTTONDOWN_API_KEY or os.getenv("BUTTONDOWN_API_KEY", "").strip()

    if not api_key:
        print("Buttondown draft skipped: no API key configured.")
        return None

    if len(items) < BUTTONDOWN_MIN_ITEMS:
        print(f"Buttondown draft skipped: only {len(items)} ranked items, minimum is {BUTTONDOWN_MIN_ITEMS}.")
        return None

    subject = build_buttondown_subject()
    body_html = build_buttondown_email_html(items)

    first_image = ""
    for item in items[:TOP_IMAGE_ITEMS]:
        if item.get("image_url"):
            first_image = item["image_url"]
            break

    payload = {
        "subject": subject,
        "body": body_html,
        "description": "Automated draft generated from ranked RSS items.",
        "email_type": BUTTONDOWN_EMAIL_TYPE,
        "archival_mode": BUTTONDOWN_ARCHIVAL_MODE,
        "commenting_mode": BUTTONDOWN_COMMENTING_MODE,
        "review_mode": BUTTONDOWN_REVIEW_MODE,
        "image": first_image,
        "metadata": {
            "source_feed": SOURCE_RSS_URL,
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "item_count": len(items),
            "generator": "ranked_rss_builder.py"
        },
        # Draft-only: do not set publish_date, do not send
        "status": "draft",
    }

    headers = {
        "Authorization": f"Token {api_key}",
        "Content-Type": "application/json",
    }

    response = requests.post(BUTTONDOWN_API_URL, headers=headers, json=payload, timeout=30)

    if response.status_code not in (200, 201):
        raise RuntimeError(
            f"Buttondown draft creation failed. Status {response.status_code}. Response: {response.text}"
        )

    return response.json()


# ============================================================
# MAIN
# ============================================================

def main():
    os.makedirs(os.path.dirname(OUTPUT_RSS_FILE), exist_ok=True)
    os.makedirs(os.path.dirname(USED_URLS_FILE), exist_ok=True)
    if WRITE_DEBUG_JSON:
        os.makedirs(os.path.dirname(DEBUG_JSON_FILE), exist_ok=True)

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

    selected_urls = [item["link"] for item in ranked_items]
    merged_used_urls = set(used_urls).union(selected_urls)

    try:
        save_used_urls(USED_URLS_FILE, merged_used_urls)
    except Exception as e:
        print(f"WARNING: RSS written, but could not save used URLs file: {e}")

    if WRITE_DEBUG_JSON:
        try:
            debug_payload = []
            for idx, item in enumerate(ranked_items, start=1):
                debug_payload.append({
                    "position": idx,
                    "title": item["title"],
                    "link": item["link"],
                    "pub_date": item["pub_date"].isoformat() if item["pub_date"] else None,
                    "image_url": item.get("image_url"),
                    "score": item.get("score"),
                    "score_reasons": item.get("score_reasons", []),
                })
            with open(DEBUG_JSON_FILE, "w", encoding="utf-8") as f:
                json.dump(debug_payload, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"WARNING: Could not write debug JSON: {e}")

    print(f"Done. Wrote ranked RSS to: {OUTPUT_RSS_FILE}")
    print(f"Stored used URLs in: {USED_URLS_FILE}")

    if WRITE_DEBUG_JSON:
        print(f"Wrote debug JSON to: {DEBUG_JSON_FILE}")

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

    if ENABLE_BUTTONDOWN_DRAFT:
        try:
            result = create_buttondown_draft(ranked_items)
            if result:
                print("Buttondown draft created successfully.")
                print(f"Buttondown email ID: {result.get('id')}")
                print(f"Buttondown status: {result.get('status')}")
                print(f"Buttondown subject: {result.get('subject')}")
                if result.get("absolute_url"):
                    print(f"Buttondown URL: {result.get('absolute_url')}")
        except Exception as e:
            print(f"WARNING: RSS succeeded, but Buttondown draft creation failed: {e}")
    else:
        print("Buttondown draft creation is disabled.")


if __name__ == "__main__":
    main()
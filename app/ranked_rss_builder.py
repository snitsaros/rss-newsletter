import os
import re
import json
import html
from dataclasses import dataclass, field
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime, format_datetime

import requests
import feedparser


# ============================================================
# PATHS
# ============================================================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE_DIR)
DOCS_DIR = os.path.join(PROJECT_ROOT, "docs")
DATA_DIR = os.path.join(PROJECT_ROOT, "data")

# ============================================================
# GLOBAL DEFAULTS
#
# Any feed can override these via FeedConfig fields.
# ============================================================

DEFAULT_MAX_ITEMS = 20
DEFAULT_TOP_IMAGE_ITEMS = 5  # top N are image-led in RSS

DEFAULT_FRESH_HOURS = 18
DEFAULT_STALE_HOURS = 24
DEFAULT_BETWEEN_FRESH_AND_STALE_PENALTY = 20
DEFAULT_OLDER_THAN_STALE_PENALTY = 100

DEFAULT_USED_URL_PENALTY = 100

DEFAULT_TITLE_SIMILARITY_THRESHOLD = 0.72
DEFAULT_DUPLICATE_TOPIC_PENALTY = 15

DEFAULT_CHANNEL_LANGUAGE = "en-gb"

DEFAULT_TOP_SUMMARY_WORDS = 32
DEFAULT_LOWER_SUMMARY_WORDS = 14

DEFAULT_WEIGHTS = {
    "top_tier_keyword": 12,
    "mid_tier_keyword": 7,
    "low_tier_keyword": 3,
    "has_image": 4,
    "recent_bonus": 10,
    "very_recent_bonus": 5,
    "title_length_good": 2,
    "title_length_bad": -2,
}


# ============================================================
# FEED CONFIG
# ============================================================

@dataclass
class FeedConfig:
    id: str
    source_url: str

    # Filenames only. Joined with docs/ and data/ at runtime.
    output_rss_file: str
    used_urls_file: str
    debug_json_file: str

    channel_title: str
    channel_link: str
    channel_description: str

    top_tier_keywords: list
    mid_tier_keywords: list
    low_tier_keywords: list

    channel_language: str = DEFAULT_CHANNEL_LANGUAGE

    max_items: int = DEFAULT_MAX_ITEMS
    top_image_items: int = DEFAULT_TOP_IMAGE_ITEMS

    fresh_hours: int = DEFAULT_FRESH_HOURS
    stale_hours: int = DEFAULT_STALE_HOURS
    between_fresh_and_stale_penalty: int = DEFAULT_BETWEEN_FRESH_AND_STALE_PENALTY
    older_than_stale_penalty: int = DEFAULT_OLDER_THAN_STALE_PENALTY

    used_url_penalty: int = DEFAULT_USED_URL_PENALTY

    title_similarity_threshold: float = DEFAULT_TITLE_SIMILARITY_THRESHOLD
    duplicate_topic_penalty: int = DEFAULT_DUPLICATE_TOPIC_PENALTY

    top_summary_words: int = DEFAULT_TOP_SUMMARY_WORDS
    lower_summary_words: int = DEFAULT_LOWER_SUMMARY_WORDS

    weights: dict = field(default_factory=lambda: dict(DEFAULT_WEIGHTS))

    write_debug_json: bool = True

    @property
    def output_rss_path(self):
        return os.path.join(DOCS_DIR, self.output_rss_file)

    @property
    def used_urls_path(self):
        return os.path.join(DATA_DIR, self.used_urls_file)

    @property
    def debug_json_path(self):
        return os.path.join(DATA_DIR, self.debug_json_file)


# ============================================================
# FEEDS
#
# Add, remove, or edit feeds freely -- the builder loops over
# however many feeds are configured here. Each feed gets its
# own output RSS file, used-URL history, and debug JSON, so
# newsletters never suppress each other's stories.
# ============================================================

FEEDS = [
    FeedConfig(
        id="f1",
        source_url="https://www.crash.net/rss/f1",
        output_rss_file="ranked_f1_feed.xml",
        used_urls_file="used_urls_f1.json",
        debug_json_file="ranked_f1_debug.json",
        channel_title="Crash F1 Ranked Feed",
        channel_link="https://www.crash.net/",
        channel_description="Editorially ranked F1 RSS feed for newsletter consumption",
        top_tier_keywords=[
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
        ],
        mid_tier_keywords=[
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
        ],
        low_tier_keywords=[
            "rookie",
            "reserve",
            "test",
            "tyre",
            "strategy",
            "upgrade",
            "contract",
            "rumour",
        ],
    ),

    # ------------------------------------------------------------
    # PLACEHOLDER EXAMPLE -- replace source_url and keyword buckets
    # with confirmed values before enabling. This shows the shape a
    # second feed config should take; add more entries the same way
    # for each additional newsletter (six, seven, or more).
    # ------------------------------------------------------------
    # FeedConfig(
    #     id="motogp",
    #     source_url="https://www.crash.net/rss/motogp",  # PLACEHOLDER -- confirm real feed URL
    #     output_rss_file="ranked_motogp_feed.xml",
    #     used_urls_file="used_urls_motogp.json",
    #     debug_json_file="ranked_motogp_debug.json",
    #     channel_title="Crash MotoGP Ranked Feed",
    #     channel_link="https://www.crash.net/",
    #     channel_description="Editorially ranked MotoGP RSS feed for newsletter consumption",
    #     top_tier_keywords=[
    #         "marquez",
    #         "bagnaia",
    #         "quartararo",
    #         "martin",
    #         "ducati",
    #         "yamaha",
    #         "honda",
    #     ],  # PLACEHOLDER -- confirm final rider/team list
    #     mid_tier_keywords=[
    #         "qualifying",
    #         "sprint",
    #         "pole",
    #         "podium",
    #         "crash",
    #         "penalty",
    #     ],  # PLACEHOLDER
    #     low_tier_keywords=[
    #         "rookie",
    #         "test",
    #         "tyre",
    #         "contract",
    #         "rumour",
    #     ],  # PLACEHOLDER
    # ),
]


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

    for field_name in ("published", "updated", "created"):
        value = entry.get(field_name)
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

    for field_name in ("summary", "description"):
        value = entry.get(field_name)
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
    for field_name in ("summary", "description"):
        value = entry.get(field_name)
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


# ============================================================
# RANKING ENGINE (shared across all feeds)
# ============================================================

def score_item(item, now_utc, used_urls, config: FeedConfig):
    score = 0
    reasons = []
    weights = config.weights

    combined_text = f"{item['title']} {item['description']}".strip().lower()

    top_hits = count_keyword_hits(combined_text, config.top_tier_keywords)
    mid_hits = count_keyword_hits(combined_text, config.mid_tier_keywords)
    low_hits = count_keyword_hits(combined_text, config.low_tier_keywords)

    if top_hits:
        added = top_hits * weights["top_tier_keyword"]
        score += added
        reasons.append(f"top-tier keywords x{top_hits} (+{added})")

    if mid_hits:
        added = mid_hits * weights["mid_tier_keyword"]
        score += added
        reasons.append(f"mid-tier keywords x{mid_hits} (+{added})")

    if low_hits:
        added = low_hits * weights["low_tier_keyword"]
        score += added
        reasons.append(f"low-tier keywords x{low_hits} (+{added})")

    if item["image_url"]:
        score += weights["has_image"]
        reasons.append(f"has image (+{weights['has_image']})")

    if item["pub_date"]:
        age = now_utc - item["pub_date"]
        age_hours = age.total_seconds() / 3600

        if age_hours <= config.fresh_hours:
            score += weights["recent_bonus"]
            reasons.append(f"within {config.fresh_hours}h (+{weights['recent_bonus']})")

            if age_hours <= 6:
                score += weights["very_recent_bonus"]
                reasons.append(f"within 6h (+{weights['very_recent_bonus']})")

        elif config.fresh_hours < age_hours <= config.stale_hours:
            score -= config.between_fresh_and_stale_penalty
            reasons.append(
                f"between {config.fresh_hours}-{config.stale_hours}h "
                f"(-{config.between_fresh_and_stale_penalty})"
            )
        else:
            score -= config.older_than_stale_penalty
            reasons.append(f"older than {config.stale_hours}h (-{config.older_than_stale_penalty})")
    else:
        score -= 10
        reasons.append("missing pub date (-10)")

    title_len = len(item["title"])
    if 30 <= title_len <= 110:
        score += weights["title_length_good"]
        reasons.append(f"good title length (+{weights['title_length_good']})")
    else:
        score += weights["title_length_bad"]
        reasons.append(f"awkward title length ({weights['title_length_bad']})")

    if item["link"] in used_urls:
        score -= config.used_url_penalty
        reasons.append(f"previously used URL (-{config.used_url_penalty})")

    return score, reasons


def deduplicate_and_rank(items, now_utc, used_urls, config: FeedConfig):
    max_items = config.max_items

    scored = []
    for item in items:
        score, reasons = score_item(item, now_utc, used_urls, config)
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
            if sim >= config.title_similarity_threshold:
                similar_found = True
                break

        if similar_found:
            item["score"] -= config.duplicate_topic_penalty
            item["score_reasons"].append(
                f"duplicate-topic suppression ({highest_similarity:.2f}) "
                f"(-{config.duplicate_topic_penalty})"
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


def build_item_xml(item, include_images, config: FeedConfig):
    title = xml_escape(item["title"])
    link = xml_escape(item["link"])
    pub_date = xml_escape(format_rss_date(item["pub_date"]))

    summary_word_limit = config.top_summary_words if include_images else config.lower_summary_words
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


def build_rss_xml(config: FeedConfig, items):
    now_utc = datetime.now(timezone.utc)

    lines = []
    lines.append('<?xml version="1.0" encoding="UTF-8"?>')
    lines.append('<rss version="2.0" xmlns:media="http://search.yahoo.com/mrss/">')
    lines.append("  <channel>")
    lines.append(f"    <title>{xml_escape(config.channel_title)}</title>")
    lines.append(f"    <link>{xml_escape(config.channel_link)}</link>")
    lines.append(f"    <description>{xml_escape(config.channel_description)}</description>")
    lines.append(f"    <language>{xml_escape(config.channel_language)}</language>")
    lines.append(f"    <lastBuildDate>{xml_escape(format_rss_date(now_utc))}</lastBuildDate>")

    for index, item in enumerate(items, start=1):
        include_images = index <= config.top_image_items
        lines.append(build_item_xml(item, include_images, config))

    lines.append("  </channel>")
    lines.append("</rss>")

    return "\n".join(lines)


# ============================================================
# PER-FEED RUN
# ============================================================

def run_feed(config: FeedConfig):
    print(f"\n=== Feed: {config.id} ===")
    print(f"Fetching feed: {config.source_url}")

    os.makedirs(os.path.dirname(config.output_rss_path), exist_ok=True)
    os.makedirs(os.path.dirname(config.used_urls_path), exist_ok=True)
    if config.write_debug_json:
        os.makedirs(os.path.dirname(config.debug_json_path), exist_ok=True)

    used_urls = load_used_urls(config.used_urls_path)

    try:
        parsed_feed = fetch_and_parse_feed(config.source_url)
    except requests.RequestException as e:
        print(f"ERROR [{config.id}]: Could not fetch source feed: {e}")
        return
    except Exception as e:
        print(f"ERROR [{config.id}]: Could not parse source feed: {e}")
        return

    items = convert_entries_to_items(parsed_feed)
    usable_item_count = len(items)

    if not items:
        print(f"ERROR [{config.id}]: No usable items found in the source feed.")
        return

    now_utc = datetime.now(timezone.utc)
    ranked_items = deduplicate_and_rank(
        items=items,
        now_utc=now_utc,
        used_urls=used_urls,
        config=config,
    )

    rss_xml = build_rss_xml(config=config, items=ranked_items)

    try:
        with open(config.output_rss_path, "w", encoding="utf-8", newline="\n") as f:
            f.write(rss_xml)
    except Exception as e:
        print(f"ERROR [{config.id}]: Could not write RSS file: {e}")
        return

    selected_urls = [item["link"] for item in ranked_items]
    merged_used_urls = set(used_urls).union(selected_urls)

    try:
        save_used_urls(config.used_urls_path, merged_used_urls)
    except Exception as e:
        print(f"WARNING [{config.id}]: RSS written, but could not save used URLs file: {e}")

    if config.write_debug_json:
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
            with open(config.debug_json_path, "w", encoding="utf-8") as f:
                json.dump(debug_payload, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"WARNING [{config.id}]: Could not write debug JSON: {e}")

    print(f"Feed:   {config.id}")
    print(f"Source: {config.source_url}")
    print(f"Output: {config.output_rss_path}")
    print(f"Usable source items: {usable_item_count}")
    print(f"Ranked output items:  {len(ranked_items)}")
    if config.write_debug_json:
        print(f"Debug JSON: {config.debug_json_path}")
    print(f"Used URLs file: {config.used_urls_path}")

    print("\nTop ranked items:")
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


# ============================================================
# MAIN
# ============================================================

def main():
    if not FEEDS:
        print("No feeds configured. Add at least one FeedConfig to FEEDS.")
        return

    for config in FEEDS:
        run_feed(config)


if __name__ == "__main__":
    main()

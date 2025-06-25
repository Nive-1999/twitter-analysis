import os
import tweepy
from collections import Counter, defaultdict
import pytz
import pandas as pd
from pymongo import MongoClient
import unicodedata
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import yagmail
from datetime import datetime
from openpyxl import load_workbook
from openpyxl.styles import Border, Side, PatternFill, Font, Alignment
from openpyxl.utils import get_column_letter

# ==== Secrets ====
MONGO_URI = os.getenv("MONGO_URI")
BEARER_TOKEN = os.getenv("TWITTER_BEARER")

if not MONGO_URI:
    raise ValueError("❌ MONGO_URI secret is not set.")
if not BEARER_TOKEN:
    raise ValueError("❌ TWITTER_BEARER secret is not set.")

# ==== MongoDB Setup ====
mongo_client = MongoClient(MONGO_URI)
db = mongo_client["twitter_analysis"]
collection = db["daily_reports"]

# ==== Twitter Client ====
client = tweepy.Client(bearer_token=BEARER_TOKEN, wait_on_rate_limit=True)

# ==== News Handles ====
news_handles = [
    "TV9Telugu", "sakshitvdigital", "10TvTeluguNews", "RTVnewsnetwork", "NtvTeluguLive",
    "SumanTvOfficial", "V6News", "abntelugutv", "News18Telugu", "Telugu360",
    "PTI_News", "etvandhraprades", "bbcnewstelugu", "GulteOfficial", "99TVTelugu"
]

# ==== Keywords ====
leader_keywords = {
    "tdp": ["chandrababuNaidu", "ncbn", "tdp", "lokesh", "naralokesh", "balakrishna",
            "#cmchandrababu", "#tdp", "#naralokesh", "Narachandrababunaidu",
            "Vangalapudianitha", "@anitha_TDP",
            "చంద్రబాబు", "సీఎం చంద్రబాబు", "నారా లోకేష్", "లోకేష్", "బాలకృష్ణ", "టిడిపి",
            "మంత్రి లోకేష్", "మంత్రి నారా లోకేష్", "లోకేశ్"],
    "ysrcp": ["jagan", "ysjagan", "ys jagan", "ysr", "ysrcp", "#ysjagan", "#ycp", "#ysrcp", "vidadalarajini"],
    "jsp": ["Pawankalyan", "janasena", "DeputyCMPawanKalyan"],
    "bjp": ["bjp", "modi", "amit shah", "narendra modi", "#bjp", "pmmodi"],
    "inc": ["rahul gandhi", "congress", "indian national congress", "yssharmila", "inc"]
}

govt_keywords = [
    "ap liquor scam", "apliquorscam", "#apliquorscam", "liquor case", "liquor scam",
    "ysrcp liquor", "jagan liquor", "liquor mafia", "ap liquor", "liquor irregularities", "liquor tenders"
]

telangana_keywords = ["telangana", "kcr", "ktr", "brs", "#brs", "b.r.s", "cmrevanthreddy", "revanthreddy"]

specific_keywords = [
    "cmchandrababu", "ysjagan", "pawankalyan", "DeputyCMPawanKalyan", "tdp", "ysrcp", "naralokesh",
    "janasena", "pithapuram", "thallikivandanam", "rapparappa", "ncbn", "chandrababuNaidu",
    "చంద్రబాబు", "సీఎం చంద్రబాబు", "నారా లోకేష్", "లోకేష్"
]

# ==== Date/Time Setup ====
ist = pytz.timezone("Asia/Kolkata")
target_date = datetime.datetime.now(ist).date()
start_ist = datetime.datetime.combine(target_date, datetime.time(0, 0, tzinfo=ist))
end_ist = datetime.datetime.combine(target_date, datetime.time(23, 59, 59, tzinfo=ist))
start_time = start_ist.astimezone(pytz.UTC).isoformat()
end_time = end_ist.astimezone(pytz.UTC).isoformat()

# ==== Time Slots ====
time_slots = {
    "12 AM–2:59 AM": (0, 3),
    "3 AM–5:59 AM": (3, 6),
    "6 AM–8:59 AM": (6, 9),
    "9 AM–11:59 AM": (9, 12),
    "12 PM–2:59 PM": (12, 15),
    "3 PM–5:59 PM": (15, 18),
    "6 PM–8:59 PM": (18, 21),
    "9 PM–11:59 PM": (21, 24)
}

def get_time_slot(dt):
    for slot, (s, e) in time_slots.items():
        if s <= dt.hour < e:
            return slot
    return "Unknown"

def fetch_tweets(username, start_time, end_time, max_results=100):
    tweets = []
    try:
        user = client.get_user(username=username)
        if not user.data:
            print(f"❌ User not found: {username}")
            return []
        uid = user.data.id
        paginator = tweepy.Paginator(
            client.get_users_tweets,
            id=uid,
            start_time=start_time,
            end_time=end_time,
            tweet_fields=["created_at", "public_metrics", "entities", "text"],
            max_results=max_results
        )
        for page in paginator:
            if page.data:
                tweets.extend(page.data)
    except Exception as e:
        print(f"⚠️ Error fetching tweets for {username}: {e}")
    return tweets

def process_handle(handle):
    print(f"\n📥 Processing @{handle}...")
    tweets = fetch_tweets(handle, start_time, end_time)
    counts = defaultdict(int)
    hashtag_counter = Counter()
    keyword_counter = Counter()
    time_slot_counter = Counter()
    most_viewed = {"views": 0, "text": "", "url": ""}

    for t in tweets:
        dt = t.created_at.astimezone(ist)
        text = unicodedata.normalize("NFKC", t.text.lower())
        counts["Total"] += 1
        slot = get_time_slot(dt)
        time_slot_counter[slot] += 1

        for party, keywords in leader_keywords.items():
            if party == "inc":
                if "sharmila" in text or "ys sharmila" in text:
                    if not any(tel_kw in text for tel_kw in telangana_keywords):
                        counts["INC_Related"] += 1
                continue
            if any(kw.lower() in text for kw in keywords):
                counts[f"{party.upper()}_Related"] += 1
                break

        if any(gk in text for gk in govt_keywords):
            counts["Govt_Related"] += 1

        if t.entities and "hashtags" in t.entities:
            for tag in t.entities["hashtags"]:
                ht = "#" + tag["tag"].lower()
                hashtag_counter[ht] += 1

        for kw in specific_keywords:
            if kw.lower() in text:
                keyword_counter[kw] += 1

        views = t.public_metrics.get("impression_count", 0)
        if views > most_viewed["views"]:
            most_viewed = {
                "views": views,
                "text": t.text,
                "url": f"https://x.com/{handle}/status/{t.id}"
            }

    summary = {
        "Handle": handle,
        "Date": str(target_date),
        "Total Tweets": counts["Total"],
        "TDP Tweets": counts["TDP_Related"],
        "YSRCP Tweets": counts["YSRCP_Related"],
        "JSP Tweets": counts["JSP_Related"],
        "BJP Tweets": counts["BJP_Related"],
        "INC Tweets (Sharmila, AP only)": counts["INC_Related"],
        "Govt Related Tweets": counts["Govt_Related"],
        **{slot: time_slot_counter.get(slot, 0) for slot in time_slots},
        "Top 50 Hashtags": "; ".join(f"{ht}:{c}" for ht, c in hashtag_counter.most_common(50)),
        "Top Tweet Views": most_viewed["views"],
        "Top Tweet URL": most_viewed["url"],
        "Top Tweet Text": most_viewed["text"]
    }

    for kw in specific_keywords:
        summary[f"{kw}_mentions"] = keyword_counter.get(kw, 0)

    collection.insert_one(summary)
    print(f"✅ Inserted summary for @{handle}")
    return summary

def run_in_batches(handles, batch_size=5):
    all_summaries = []
    total_batches = (len(handles) + batch_size - 1) // batch_size

    for i in range(0, len(handles), batch_size):
        batch = handles[i:i + batch_size]
        print(f"\n🧵 Starting batch {i//batch_size + 1}/{total_batches} — Handles: {batch}")
        batch_summaries = []

        with ThreadPoolExecutor(max_workers=len(batch)) as executor:
            futures = {executor.submit(process_handle, h): h for h in batch}
            for future in as_completed(futures):
                try:
                    result = future.result()
                    if result:
                        batch_summaries.append(result)
                except Exception as e:
                    print(f"❌ Error processing {futures[future]}: {e}")

        all_summaries.extend(batch_summaries)
        if i + batch_size < len(handles):
            print("⏳ Sleeping 90 seconds to avoid rate limits...")
            time.sleep(90)

    return all_summaries

def format_and_send_excel(filename):
    wb = load_workbook(filename)
    ws = wb.active

    for col in ws.columns:
        max_len = 0
        col_letter = col[0].column_letter
        for cell in col:
            if cell.value:
                max_len = max(max_len, len(str(cell.value)))
        ws.column_dimensions[col_letter].width = max(12, max_len + 2)

    ws.freeze_panes = "A2"
    thin_border = Border(
        left=Side(style='thin'), right=Side(style='thin'),
        top=Side(style='thin'), bottom=Side(style='thin')
    )
    header_fill = PatternFill("solid", fgColor="D9E1F2")
    header_font = Font(bold=True)
    center_align = Alignment(horizontal="center", vertical="center", wrap_text=True)

    for row in ws.iter_rows():
        for cell in row:
            cell.border = thin_border
            cell.alignment = center_align

    for cell in ws[1]:
        cell.fill = header_fill
        cell.font = header_font

    wb.save(filename)
    print(f"📁 Final formatted Excel saved: {filename}")

    sender_email = os.getenv("SENDER_EMAIL")
    sender_password = os.getenv("SENDER_PASSWORD")
    recipient_email = os.getenv("TO_EMAIL")
    cc_email = os.getenv("CC_EMAIL")

    yag = yagmail.SMTP(user=sender_email, password=sender_password)
    subject = f"🗳️ Daily Twitter News Analysis Report - {datetime.now().strftime('%d %B %Y')}"
    body = "Hi,\n\nPlease find attached the formatted daily News Twitter analysis report.\n\nBest regards,\nAutomated Report"
    yag.send(to=recipient_email, cc=cc_email, subject=subject, contents=body, attachments=[filename])
    print(f"📧 Email sent to {recipient_email} with CC to {cc_email}")

# ==== Main Run ====
if __name__ == "__main__":
    output_filename = "daily_twitter_analysis.xlsx"
    df = pd.DataFrame(run_in_batches(news_handles))
    df.to_excel(output_filename, index=False)
    format_and_send_excel(output_filename)

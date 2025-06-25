# ==== Daily Twitter News Analysis: Final Python Script ====
import os
import tweepy
import datetime
from collections import Counter, defaultdict
import pytz
import pandas as pd
from pymongo import MongoClient
import unicodedata
import time
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
from openpyxl import load_workbook
from openpyxl.styles import Border, Side, PatternFill, Font, Alignment
from openpyxl.utils import get_column_letter
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Email, To, Cc, Attachment, FileContent, FileName, FileType, Disposition
import base64

# ==== Secrets ====
MONGO_URI = os.getenv("MONGO_URI")
BEARER_TOKEN = os.getenv("TWITTER_BEARER")
SENDGRID_API_KEY = os.getenv("SENDGRID_API_KEY")
SENDER_EMAIL = os.getenv("SENDER_EMAIL")
TO_EMAIL = os.getenv("TO_EMAIL")
CC_EMAIL = os.getenv("CC_EMAIL")

assert MONGO_URI, "MONGO_URI secret is not set."
assert BEARER_TOKEN, "TWITTER_BEARER secret is not set."
assert SENDGRID_API_KEY, "SENDGRID_API_KEY is not set."

mongo_client = MongoClient(MONGO_URI)
db = mongo_client["twitter_analysis"]
collection = db["daily_reports"]

client = tweepy.Client(bearer_token=BEARER_TOKEN, wait_on_rate_limit=True)

news_handles = [
    "TV9Telugu", "sakshitvdigital", "10TvTeluguNews", "RTVnewsnetwork", "NtvTeluguLive",
    "SumanTvOfficial", "V6News", "abntelugutv", "News18Telugu", "Telugu360",
    "PTI_News", "etvandhraprades", "bbcnewstelugu", "GulteOfficial", "99TVTelugu"
]

leader_keywords = {
    "tdp": ["chandrababuNaidu", "ncbn", "tdp", "lokesh", "naralokesh", "balakrishna",
            "#cmchandrababu", "#tdp", "#naralokesh", "Narachandrababunaidu",
            "Vangalapudianitha", "@anitha_TDP",
            "చంద్రబాబు", "సీయం చంద్రబాబు",
            "నారా లోకేష్"],
    "ysrcp": ["jagan", "ysjagan", "ys jagan", "ysr", "ysrcp", "#ysjagan", "#ycp", "#ysrcp", "vidadalarajini"],
    "jsp": ["Pawankalyan", "janasena", "DeputyCMPawanKalyan"],
    "bjp": ["bjp", "modi", "amit shah", "narendra modi", "#bjp", "pmmodi"],
    "inc": ["rahul gandhi", "congress", "indian national congress", "yssharmila", "inc"]
}

ist = pytz.timezone("Asia/Kolkata")
target_date = datetime.datetime.now(ist).date()
start_ist = datetime.datetime.combine(target_date, datetime.time(0, 0, tzinfo=ist))
end_ist = datetime.datetime.combine(target_date, datetime.time(23, 59, 59, tzinfo=ist))
start_time = start_ist.astimezone(pytz.UTC).isoformat()
end_time = end_ist.astimezone(pytz.UTC).isoformat()

time_slots = {
    "12 AM–2:59 AM": (0, 3), "3 AM–5:59 AM": (3, 6), "6 AM–8:59 AM": (6, 9),
    "9 AM–11:59 AM": (9, 12), "12 PM–2:59 PM": (12, 15), "3 PM–5:59 PM": (15, 18),
    "6 PM–8:59 PM": (18, 21), "9 PM–11:59 PM": (21, 24)
}

def get_time_slot(dt):
    for slot, (s, e) in time_slots.items():
        if s <= dt.hour < e:
            return slot
    return "Unknown"

def fetch_tweets(username, start_time, end_time):
    tweets = []
    try:
        user = client.get_user(username=username)
        if not user.data:
            return []
        uid = user.data.id
        paginator = tweepy.Paginator(
            client.get_users_tweets,
            id=uid,
            start_time=start_time,
            end_time=end_time,
            tweet_fields=["created_at", "public_metrics", "entities", "text"],
            max_results=100
        )
        for page in paginator:
            if page.data:
                tweets.extend(page.data)
    except Exception as e:
        print(f"Error fetching tweets for {username}: {e}")
    return tweets

def process_handle(handle):
    tweets = fetch_tweets(handle, start_time, end_time)
    total = len(tweets)
    party_counts = defaultdict(int)
    slot_counts = defaultdict(int)
    hashtags, keywords = [], []
    max_views = 0
    most_viewed = ""

    for tweet in tweets:
        text = tweet.text.lower()
        for party, keys in leader_keywords.items():
            if any(k.lower() in text for k in keys):
                party_counts[party] += 1
        if tweet.entities and tweet.entities.get("hashtags"):
            hashtags.extend([h["tag"].lower() for h in tweet.entities["hashtags"]])
        for word in text.split():
            word = unicodedata.normalize("NFKD", word)
            if word.startswith("#") or word.startswith("@"): continue
            if len(word) > 3: keywords.append(word)
        views = tweet.public_metrics.get("impression_count", 0)
        if views > max_views:
            max_views = views
            most_viewed = f"https://twitter.com/{handle}/status/{tweet.id}"
        slot = get_time_slot(tweet.created_at.astimezone(ist))
        slot_counts[slot] += 1

    summary = {
        "Handle": handle,
        "Total Tweets": total,
        **{f"{p.upper()} Tweets": party_counts.get(p, 0) for p in leader_keywords},
        **{slot: slot_counts.get(slot, 0) for slot in time_slots},
        "Most Viewed Tweet": most_viewed,
        "Views": max_views,
        "Top Hashtags": ", ".join([h for h, _ in Counter(hashtags).most_common(5)]),
        "Top Keywords": ", ".join([k for k, _ in Counter(keywords).most_common(5)])
    }
    collection.insert_one({"date": target_date.isoformat(), **summary})
    return summary

def run_in_batches(handles, batch_size=5):
    all_summaries = []
    for i in range(0, len(handles), batch_size):
        batch = handles[i:i + batch_size]
        with ThreadPoolExecutor(max_workers=min(len(batch), 3)) as executor:
            futures = {executor.submit(process_handle, h): h for h in batch}
            for future in as_completed(futures, timeout=300):
                try:
                    result = future.result()
                    if result:
                        all_summaries.append(result)
                except Exception as e:
                    print(f"Error processing {futures[future]}: {e}")
        if i + batch_size < len(handles):
            time.sleep(90)
    return all_summaries

def format_and_send_excel(filename):
    wb = load_workbook(filename)
    ws = wb.active
    for col in ws.columns:
        max_len = max((len(str(cell.value)) if cell.value else 0) for cell in col)
        ws.column_dimensions[col[0].column_letter].width = max(12, max_len + 2)

    ws.freeze_panes = "A2"
    border = Border(left=Side(style='thin'), right=Side(style='thin'),
                    top=Side(style='thin'), bottom=Side(style='thin'))
    header_fill = PatternFill("solid", fgColor="D9E1F2")
    header_font = Font(bold=True)
    center_align = Alignment(horizontal="center", vertical="center", wrap_text=True)

    for row in ws.iter_rows():
        for cell in row:
            cell.border = border
            cell.alignment = center_align
    for cell in ws[1]:
        cell.fill = header_fill
        cell.font = header_font

    wb.save(filename)

    with open(filename, "rb") as f:
        encoded = base64.b64encode(f.read()).decode()

    message = Mail(
        from_email=Email(SENDER_EMAIL),
        to_emails=To(TO_EMAIL),
        subject=f"🗳️ Daily Twitter News Analysis Report - {datetime.datetime.now().strftime('%d %B %Y')}",
        html_content="<p>Hi,</p><p>Attached is the daily Twitter analysis report.</p><p>Regards,<br>Automated Bot</p>"
    )
    if CC_EMAIL:
        message.add_cc(Cc(CC_EMAIL))
    attachment = Attachment(
        FileContent(encoded), FileName(filename),
        FileType("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
        Disposition("attachment")
    )
    message.attachment = attachment
    try:
        sg = SendGridAPIClient(SENDGRID_API_KEY)
        response = sg.send(message)
        print(f"Email sent. Status Code: {response.status_code}")
    except Exception as e:
        print(f"Email failed: {e}")

if __name__ == "__main__":
    output_file = "daily_twitter_analysis.xlsx"
    df = pd.DataFrame(run_in_batches(news_handles))
    df.to_excel(output_file, index=False)
    format_and_send_excel(output_file)

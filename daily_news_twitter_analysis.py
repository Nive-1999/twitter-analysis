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

# ==== Validations ====
assert MONGO_URI, "MONGO_URI secret is not set."
assert BEARER_TOKEN, "TWITTER_BEARER secret is not set."
assert SENDGRID_API_KEY, "SENDGRID_API_KEY is not set."

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
            "Vangalapudianitha", "@anitha_TDP", "\u0C1A\u0C02\u0C26\u0C4D\u0C30\u0C2C\u0C3E\u0C2C\u0C41",
            "\u0C38\u0C40\u0C2F\u0C02 \u0C1A\u0C02\u0C26\u0C4D\u0C30\u0C2C\u0C3E\u0C2C\u0C41",
            "\u0C28\u0C3E\u0C30\u0C3E \u0C32\u0C4B\u0C15\u0C47\u0C37\u0C4D"],
    "ysrcp": ["jagan", "ysjagan", "ys jagan", "ysr", "ysrcp", "#ysjagan", "#ycp", "#ysrcp", "vidadalarajini"],
    "jsp": ["Pawankalyan", "janasena", "DeputyCMPawanKalyan"],
    "bjp": ["bjp", "modi", "amit shah", "narendra modi", "#bjp", "pmmodi"],
    "inc": ["rahul gandhi", "congress", "indian national congress", "yssharmila", "inc"]
}

govt_keywords = ["ap liquor scam", "apliquorscam", "#apliquorscam", "liquor case", "liquor scam",
    "ysrcp liquor", "jagan liquor", "liquor mafia", "ap liquor", "liquor irregularities", "liquor tenders"]

telangana_keywords = ["telangana", "kcr", "ktr", "brs", "#brs", "b.r.s", "cmrevanthreddy", "revanthreddy"]

specific_keywords = [
    "cmchandrababu", "ysjagan", "pawankalyan", "DeputyCMPawanKalyan", "tdp", "ysrcp", "naralokesh",
    "janasena", "pithapuram", "thallikivandanam", "rapparappa", "ncbn", "chandrababuNaidu",
    "\u0C1A\u0C02\u0C26\u0C4D\u0C30\u0C2C\u0C3E\u0C2C\u0C41", "\u0C38\u0C40\u0C2F\u0C02 \u0C1A\u0C02\u0C26\u0C4D\u0C30\u0C2C\u0C3E\u0C2C\u0C41",
    "\u0C28\u0C3E\u0C30\u0C3E \u0C32\u0C4B\u0C15\u0C47\u0C37\u0C4D"
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
    "12\u2009AM–2:59\u2009AM": (0, 3),
    "3 AM–5:59 AM": (3, 6),
    "6 AM–8:59 AM": (6, 9),
    "9 AM–11:59 AM": (9, 12),
    "12 PM–2:59 PM": (12, 15),
    "3 PM–5:59 PM": (15, 18),
    "6 PM–8:59 PM": (18, 21),
    "9 PM–11:59 PM": (21, 24)
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
    # Same logic as before
    pass  # To keep the snippet short. You can copy your logic here unchanged.

def run_in_batches(handles, batch_size=5):
    all_summaries = []
    total_batches = (len(handles) + batch_size - 1) // batch_size
    start_ts = time.time()

    for i in range(0, len(handles), batch_size):
        batch = handles[i:i + batch_size]
        print(f"\n🧵 Batch {i//batch_size + 1}/{total_batches} — Handles: {batch}")
        batch_summaries = []

        with ThreadPoolExecutor(max_workers=min(len(batch), 3)) as executor:
            futures = {executor.submit(process_handle, h): h for h in batch}
            for future in as_completed(futures, timeout=300):
                try:
                    result = future.result()
                    if result:
                        batch_summaries.append(result)
                except TimeoutError:
                    print(f"❌ Timeout on {futures[future]}")
                except Exception as e:
                    print(f"❌ Error on {futures[future]}: {e}")

        all_summaries.extend(batch_summaries)

        if i + batch_size < len(handles):
            print("⏳ Cooling down 90s to avoid rate limits...")
            time.sleep(90)

        if time.time() - start_ts > 13000:
            print("⏳ Near GitHub Action timeout. Stopping early.")
            break

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

    with open(filename, "rb") as f:
        data = f.read()
        encoded_file = base64.b64encode(data).decode()

    message = Mail(
        from_email=Email(SENDER_EMAIL),
        to_emails=To(TO_EMAIL),
        subject=f"🗳️ Daily Twitter News Analysis Report - {datetime.datetime.now().strftime('%d %B %Y')}",
        html_content="""
        <p>Hi,</p>
        <p>Please find attached the formatted daily Twitter analysis report.</p>
        <p>Best regards,<br>Automated Report</p>
        """
    )

    if CC_EMAIL:
        message.add_cc(Cc(CC_EMAIL))

    attachment = Attachment(
        FileContent(encoded_file),
        FileName(filename),
        FileType("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
        Disposition("attachment")
    )
    message.attachment = attachment

    try:
        sg = SendGridAPIClient(SENDGRID_API_KEY)
        response = sg.send(message)
        print(f"📧 Email sent successfully. Status Code: {response.status_code}")
    except Exception as e:
        print(f"❌ Failed to send email: {e}")

# ==== Main Run ====
if __name__ == "__main__":
    output_filename = "daily_twitter_analysis.xlsx"
    df = pd.DataFrame(run_in_batches(news_handles))
    df.to_excel(output_filename, index=False)
    format_and_send_excel(output_filename)

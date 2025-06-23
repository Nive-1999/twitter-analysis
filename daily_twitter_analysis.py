import tweepy
import datetime
from collections import Counter, defaultdict
import pytz
import pandas as pd
from pymongo import MongoClient

# ==== Twitter API Bearer Token ====
BEARER_TOKEN = "AAAAAAAAAAAAAAAAAAAAALRC2gEAAAAAGSkUF9M0AlsTpq%2F0uPyhGnBQXss%3DOOOHCzR9EdpuyBjz1WizGS1GnWqyizKRwFOn7taQyM6TqSg25F"
client = tweepy.Client(bearer_token=BEARER_TOKEN, wait_on_rate_limit=True)

# ==== MongoDB Setup ====
MONGO_URI = "mongodb+srv://niveditha:NdeZU4l4G63c594g@cluster0.mvz2fhb.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
mongo_client = MongoClient(MONGO_URI)
db = mongo_client["twitter_analysis"]
collection = db["daily_reports"]

# ==== News Handles ====
news_handles = [
    "PTI_News", "etvandhraprades", "bbcnewstelugu", "GulteOfficial", "99TVTelugu"
]

# ==== Party Leader Keywords ====
leader_keywords = {
    "tdp": ["chandrababuNaidu", "ncbn", "tdp", "lokesh", "naralokesh", "balakrishna", "#cmchandrababu", "#tdp", "#naralokesh"],
    "ysrcp": ["jagan", "ysjagan", "ys jagan", "ysr", "ysrcp", "#ysjagan", "#ycp", "#ysrcp"],
    "jsp": ["pawankalyan", "janasena", "DeputyCMPawanKalyan"],
    "bjp": ["bjp", "modi", "amit shah", "narendra modi", "#bjp"],
    "inc": ["rahul gandhi", "congress", "indian national congress", "yssharmila", "inc"]
}

govt_keywords = [
    "ap liquor scam", "apliquorscam", "#apliquorscam", "liquor case", "liquor scam", "ysrcp liquor",
    "jagan liquor", "liquor mafia", "ap liquor", "liquor irregularities", "liquor tenders"
]

telangana_keywords = ["telangana", "kcr", "ktr", "brs", "#brs", "b.r.s", "cmrevanthreddy", "revanthreddy"]

specific_keywords = [
    "cmchandrababu", "ysjagan", "pawankalyan", "DeputyCMPawanKalyan",
    "tdp", "ysrcp", "naralokesh", "janasena", "pithapuram", "thallikivandanam", "rapparappa", "ncbn", "chandrababuNaidu"
]

# ==== Time Setup ====
ist = pytz.timezone("Asia/Kolkata")
target_date = datetime.datetime.now(ist).date()
start_ist = datetime.datetime.combine(target_date, datetime.time(0, 0, tzinfo=ist))
end_ist = datetime.datetime.combine(target_date, datetime.time(23, 59, 59, tzinfo=ist))
start_time = start_ist.astimezone(pytz.UTC).isoformat()
end_time = end_ist.astimezone(pytz.UTC).isoformat()

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
    h = dt.hour
    for slot, (s, e) in time_slots.items():
        if s <= h < e:
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

all_summaries = []

for handle in news_handles:
    print(f"\n📥 Processing @{handle}...")
    tweets = fetch_tweets(handle, start_time, end_time)
    counts = defaultdict(int)
    hashtag_counter = Counter()
    keyword_counter = Counter()
    time_slot_counter = Counter()
    most_viewed = {"views": 0, "text": "", "url": ""}

    for t in tweets:
        dt = t.created_at.astimezone(ist)
        text = t.text.lower()
        counts["Total"] += 1
        slot = get_time_slot(dt)
        time_slot_counter[slot] += 1

        for party, keywords in leader_keywords.items():
            if party == "inc":
                if "sharmila" in text:
                    if not any(tel_kw in text for tel_kw in telangana_keywords):
                        counts["INC_Related"] += 1
                continue
            if any(kw in text for kw in keywords):
                counts[f"{party.upper()}_Related"] += 1
                break

        if any(gk in text for gk in govt_keywords):
            counts["Govt_Related"] += 1

        if t.entities and "hashtags" in t.entities:
            for tag in t.entities["hashtags"]:
                ht = "#" + tag["tag"].lower()
                hashtag_counter[ht] += 1

        for kw in specific_keywords:
            if kw in text:
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

    all_summaries.append(summary)

# ==== Insert into MongoDB ====
collection.insert_many(all_summaries)
print(f"✅ Inserted {len(all_summaries)} records into MongoDB")

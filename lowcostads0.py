from flask import Flask, jsonify
import requests
import pandas as pd
import time
import traceback
from datetime import datetime, timedelta

app = Flask(__name__)

# ------------------- روابط Dataslayer -------------------
ADS_URL = "https://query-manager.dataslayer.ai/get_results/f7c65286119ec815fe2f66dc64d857ff4d399397b4b8f73c9e9ceb56454f60f9:1f539603fca748cf971a7500a61ebcb2?"
AVG_URL = "https://query-manager.dataslayer.ai/get_results/3497f00804eac919c7b9e6c652d5b6eecbcdbb2e60c1dcbaad287606fd81b46f:249c8499ee734d309911b43440e87660?"

# ------------------- كاش داخلي -------------------
CACHE = {}
CACHE_TTL = 600  # 10 دقائق

def get_from_cache(key):
    if key in CACHE:
        data, timestamp = CACHE[key]
        if time.time() - timestamp < CACHE_TTL:
            print(f"✅ رجعنا النتيجة من الكاش: {key}")
            return data
        else:
            del CACHE[key]
    return None

def save_to_cache(key, data):
    CACHE[key] = (data, time.time())

# ------------------- جلب البيانات -------------------
def fetch_data(url):
    cached = get_from_cache(url)
    if cached is not None:
        return cached
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        json_data = response.json()
        if "result" not in json_data:
            return pd.DataFrame()
        result = json_data["result"]
        if not result or len(result) < 2:
            return pd.DataFrame()
        df = pd.DataFrame(result[1:], columns=result[0])
        save_to_cache(url, df)
        return df
    except Exception as e:
        print("❌ Error fetching data:", e)
        return pd.DataFrame()

# ------------------- معالجة البيانات -------------------
def process_data():
    df_ads = fetch_data(ADS_URL)
    df_avg = fetch_data(AVG_URL)

    if df_ads.empty or df_avg.empty:
        return pd.DataFrame()

    rename_map = {
        "Account id": "account_id",
        "Account name": "account_name",
        "Account Currency": "account_currency",
        "Ad id": "ad_id",
        "Ad name": "ad_name",
        "AdSet id": "adset_id",
        "AdSet name": "adset_name",
        "AdSet status": "adset_status",
        "AdSet start date": "adset_start_date",
        "Campaign id": "campaign_id",
        "Campaign name": "campaign_name",
        "Campaign status": "campaign_status",
        "Cost per New Conversation started": "cost_per_new_conversation_started"
    }
    df_ads.rename(columns=rename_map, inplace=True)
    df_avg.rename(columns=rename_map, inplace=True)

    # تحويل الأعمدة الرقمية والتاريخية
    df_ads["cost_per_new_conversation_started"] = pd.to_numeric(
        df_ads["cost_per_new_conversation_started"], errors="coerce"
    )
    df_avg["cost_per_new_conversation_started"] = pd.to_numeric(
        df_avg["cost_per_new_conversation_started"], errors="coerce"
    )

    # تحويل التاريخ إلى datetime
    df_ads["adset_start_date"] = pd.to_datetime(df_ads["adset_start_date"], errors="coerce")

    # دمج البيانات على مستوى الـ campaign_id
    merged = pd.merge(
        df_ads,
        df_avg[["campaign_id", "cost_per_new_conversation_started"]],
        on="campaign_id",
        how="left",
        suffixes=("", "_avg")
    )

    merged = merged.rename(columns={"cost_per_new_conversation_started_avg": "avg_cost"})
    merged["avg_cost"] = merged["avg_cost"].fillna(0)

    # فلترة حسب مرور 3 أيام على بداية الإعلان
    three_days_ago = datetime.now() - timedelta(days=3)
    merged = merged[merged["adset_start_date"] < three_days_ago]

    # 🔗 إنشاء عمود الرابط الصحيح الذي يفتح الاعلان مباشرة
    
    merged["ad_link"] = merged.apply(
        lambda row: f"https://adsmanager.facebook.com/adsmanager/manage/ads?act={row['account_id']}&filter_set=SEARCH_BY_AD_ID-STRING%1EEQUAL%1E%22{row['ad_id']}%22&selected_ad_ids={row['ad_id']}&sort=delivery_info~1",
    axis=1
)
    return merged

# ------------------- API -------------------
@app.route("/analyze_low_cost", methods=["GET"])
def analyze_low_cost():
    try:
        df = process_data()
        if df.empty:
            return jsonify({"data": [], "message": "No data found"})

        # فلترة: الإعلان أقل أو يساوي من المتوسط
        low_cost = df[df["cost_per_new_conversation_started"] <= df["avg_cost"]]

        # إزالة الإعلانات المكررة
        low_cost = low_cost.drop_duplicates(subset=["ad_id"])

        return jsonify({
            "data": low_cost.to_dict(orient="records"),
            "message": f"تم العثور على {len(low_cost)} إعلان منخفض التكلفة (بعد مرور 3 أيام)"
        })

    except Exception as e:
        return jsonify({"error": str(e), "trace": traceback.format_exc()})

# ------------------- تشغيل التطبيق -------------------
if __name__ == "__main__":
    app.run(debug=True, port=5001)

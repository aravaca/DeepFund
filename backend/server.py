import os
import json
from flask import Flask, request, jsonify
from flask_cors import CORS
import base64
import requests
import pandas as pd
import yfinance as yf
from collections import OrderedDict
from flask import Response

excel_path = os.path.join(os.path.dirname(__file__), "deep_fund.xlsx")


app = Flask(__name__, static_folder="dist", static_url_path="")
CORS(app, resources={r"/*": {"origins": "*"}})

RECIPIENT_FILE = "backend/recipients.json"


@app.route("/")
def serve_vue():
    return app.send_static_file("index.html")


def push_recipients_json():
    repo = "pozuelodealarcon/Portfolio"
    path = RECIPIENT_FILE
    branch = "main"
    token = os.getenv("GITHUB_TOKEN")

    if not token:
        return False, "GITHUB_TOKEN이 설정되지 않았습니다."

    with open(path, "r") as f:
        content = f.read()
    b64_content = base64.b64encode(content.encode()).decode()

    url = f"https://api.github.com/repos/{repo}/contents/{path}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github.v3+json",
    }
    r = requests.get(url, headers=headers)
    if r.status_code == 200:
        sha = r.json()["sha"]
    else:
        sha = None

    data = {
        "message": "Update recipients.json from Railway",
        "content": b64_content,
        "branch": branch,
    }
    if sha:
        data["sha"] = sha

    r = requests.put(url, headers=headers, json=data)
    if r.status_code in [200, 201]:
        return True, "업데이트 성공"
    else:
        return False, f"{r.status_code} - {r.text}"


@app.route("/subscribe", methods=["POST"])
def subscribe():
    email = request.json.get("email")
    if not email:
        return jsonify({"message": "⚠️ 유효한 이메일이 아닙니다."}), 400

    if not os.path.exists(RECIPIENT_FILE):
        with open(RECIPIENT_FILE, "w") as f:
            json.dump([], f)

    with open(RECIPIENT_FILE, "r+") as f:
        data = json.load(f)
        if email in data:
            return jsonify({"message": "⚠️ 이미 등록된 이메일입니다."}), 400
        data.append(email)
        f.seek(0)
        json.dump(data, f, indent=2)
        f.truncate()

    # Push to GitHub
    success, msg = push_recipients_json()
    if success:
        return jsonify({"message": f"✅ 구독 완료: {email}"})
    else:
        return jsonify({"message": f"❌ GitHub 업로드 실패: {msg}"}), 500


@app.route("/api/market-data")
def market_data():
    indices = OrderedDict(
        [
            ("S&P500", "^GSPC"),
            ("NASDAQ", "^IXIC"),
            ("Dow Jones", "^DJI"),
            ("KOSPI", "^KS11"),
            ("KOSDAQ", "^KQ11"),
            ("USD/KRW", "USDKRW=X"),
            ("BTC/USD", "BTC-USD"),
            ("ETH/USD", "ETH-USD"),
            ("Gold", "GC=F"),
            ("WTI", "CL=F"),
            ("Brent", "BZ=F"),
        ]
    )

    data = OrderedDict()
    for name, symbol in indices.items():
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period="3d").tail(2)
        if len(hist) < 2:
            continue

        price_today = hist["Close"].iloc[1]
        price_yesterday = hist["Close"].iloc[0]

        change = price_today - price_yesterday
        percent_change = (change / price_yesterday) * 100

        sign = "▲" if change > 0 else "▼" if change < 0 else "-"
        data[name] = {
            "price": round(price_today, 2),
            "change": f"{sign} {abs(percent_change):.2f}%",
        }

    json_str = json.dumps(data, ensure_ascii=False)
    return Response(json_str, mimetype="application/json")


@app.route("/top-tickers")
def top_tickers():
    df = pd.read_excel(excel_path, sheet_name="종목분석")

    df_top = df.head(15)
    unique_tickers = []
    tickers = []

    for _, row in df_top.iterrows():
        ticker = row["종목"]
        if ticker not in unique_tickers:
            unique_tickers.append(ticker)
            tickers.append({"ticker": str(ticker), "change": str(row["1개월대비"])})
        if len(tickers) == 10:
            break

    return jsonify({"tickers": tickers})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)

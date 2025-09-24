
import os
import json
from pathlib import Path
from flask import Flask, request, jsonify, Response
from flask_cors import CORS
import base64
import requests
import pandas as pd
import yfinance as yf
from collections import OrderedDict

# --- 새로 추가: 절대 경로 기반 설정 ---
BASE_DIR = Path(__file__).resolve().parent
LOCAL_DATA_DIR = BASE_DIR # 필요하면 BASE_DIR / "data" 로 바꾸고 폴더 생성
LOCAL_RECIPIENT_FILE = LOCAL_DATA_DIR / "recipients.json" # 로컬 파일은 프로젝트 루트/백엔드 이동과 무관
GITHUB_RECIPIENT_PATH = "backend/recipients.json" # 깃허브 레포 내 저장 경로(원래 의도 유지)

excel_path = BASE_DIR / "deep_fund.xlsx"

app = Flask(__name__, static_folder="dist", static_url_path="")
CORS(app, resources={r"/*": {"origins": "*"}})

def ensure_parent_dir(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)



@app.route("/")
def serve_vue():
    return app.send_static_file("index.html")

def push_recipients_json():
    repo = "pozuelodealarcon/Portfolio"
    token = os.getenv("GITHUB_TOKEN")

    if not token:
        return False, "GITHUB_TOKEN이 설정되지 않았습니다."

    # 로컬 파일 읽기
    if not LOCAL_RECIPIENT_FILE.exists():
        return False, f"로컬 파일 없음: {LOCAL_RECIPIENT_FILE}"

    content = LOCAL_RECIPIENT_FILE.read_text(encoding="utf-8")
    b64_content = base64.b64encode(content.encode()).decode()

    url = f"https://api.github.com/repos/{repo}/contents/{GITHUB_RECIPIENT_PATH}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github.v3+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }

    # 현재 sha 조회(있으면 업데이트, 없으면 생성)
    r = requests.get(url, headers=headers, timeout=15)
    sha = r.json().get("sha") if r.status_code == 200 else None

    data = {
        "message": "Update recipients.json from Railway",
        "content": b64_content,
        "branch": "main",
    }
    if sha:
        data["sha"] = sha

    r = requests.put(url, headers=headers, json=data, timeout=30)
    if r.status_code in (200, 201):
        return True, "업데이트 성공"
    else:
        return False, f"{r.status_code} - {r.text}"

@app.route("/")
def serve_vue():
    return app.send_static_file("index.html")

@app.route("/subscribe", methods=["POST"])
def subscribe():
    email = request.json.get("email")
    if not email:
        return jsonify({"message": "⚠️ 유효한 이메일이 아닙니다."}), 400

    # 로컬 recipients.json 보장
    ensure_parent_dir(LOCAL_RECIPIENT_FILE)
    if not LOCAL_RECIPIENT_FILE.exists():
        LOCAL_RECIPIENT_FILE.write_text("[]", encoding="utf-8")

    # 읽고/중복확인/추가
    with LOCAL_RECIPIENT_FILE.open("r+", encoding="utf-8") as f:
        try:
            data = json.load(f)
        except json.JSONDecodeError:
            data = []
        if email in data:
            return jsonify({"message": "⚠️ 이미 등록된 이메일입니다."}), 400
        data.append(email)
        f.seek(0)
        json.dump(data, f, indent=2, ensure_ascii=False)
        f.truncate()

    # GitHub로 푸시
    success, msg = push_recipients_json()
    if success:
        return jsonify({"message": f"✅ 구독 완료: {email}"})
    else:
        # 로컬에는 반영됐으나 깃허브 업로드 실패 시 207 비슷한 의미로 500 대신 502로 명확화
        return jsonify({"message": f"❌ GitHub 업로드 실패: {msg}"}), 502

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
        try:
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
                "price": round(float(price_today), 2),
                "change": f"{sign} {abs(percent_change):.2f}%",
            }
        except Exception:
            # 심플하게 스킵
            continue

    return Response(json.dumps(data, ensure_ascii=False), mimetype="application/json")

@app.route("/top-tickers")
def top_tickers():
    df = pd.read_excel(excel_path, sheet_name="종목분석")
    df_top = df.head(15)
    unique = set()
    tickers = []
    for _, row in df_top.iterrows():
        t = str(row["종목"])
        if t not in unique:
            unique.add(t)
            tickers.append({"ticker": t, "change": str(row.get("1개월대비", ""))})
        if len(tickers) == 10:
            break
    return jsonify({"tickers": tickers})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
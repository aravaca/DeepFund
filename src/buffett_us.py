# SPDX-FileCopyrightText: © 2025 Hyungsuk Choi <chs_3411@naver[dot]com>, University of Maryland
# SPDX-License-Identifier: MIT

import yfinance as yf
import pandas as pd
import numpy as np
import requests
import string
import datetime as dt
import openpyxl
import math
from queue import Queue
import threading
import time
import polars as pl

# import shelve
from bs4 import BeautifulSoup
from urllib.request import urlopen
import smtplib
from email.message import EmailMessage
from email.headerregistry import Address
import os
import ta  # 기술적 지표 계산 라이브러리
import re
import matplotlib.pyplot as plt
from scipy.optimize import minimize
from scipy.stats import norm
from scipy.stats import skew, kurtosis
from scipy.stats.mstats import gmean
from scipy.stats import linregress
from datetime import datetime, timedelta, date
from google import genai
from google.genai import types
import json
import markdown

# 현재 파일 (src/buffett_us.py) 기준으로 프로젝트 루트 경로
project_root = os.path.dirname(os.path.dirname(__file__))
# backend 경로로 엑셀 저장
excel_path = os.path.join(project_root, "backend", "deep_fund.xlsx")

################ DEPENDENCIES ###########################

# pip install -r requirements.txt

#########################################################
recipients = ["chs_3411@naver.com", "eljm2080@gmail.com", "hyungsukchoi3411@gmail.com"]

# JSON에서 이메일 불러오기

try:
    recipients_json_path = os.path.join(project_root, "backend", "recipients.json")
    with open(recipients_json_path, "r") as f:
        loaded_emails = json.load(f)
        for email in loaded_emails:
            if email not in recipients:
                recipients.append(email)  # append 형태로 추가
except (FileNotFoundError, json.JSONDecodeError):
    print("⚠️ recipients.json 파일이 없거나 잘못되었습니다.")

recipients = list(set(recipients))

################ PREDETERMINED FIELDS ###################

EMAIL = os.environ["EMAIL_ADDRESS"]
PASSWORD = os.environ["EMAIL_PASSWORD"]
fmp_key = os.environ["FMP_API_KEY"]
marketaux_api = os.environ["MARKETAUX_API"]
NUM_THREADS = 2  # multithreading

country = "US"
limit = 200  # max 250 requests/day #
sp500 = True

# top X tickers to optimize
opt = 20

# for news
news_lookup = 100

# for moat
moat_limit = 200
#########################################################


##########################################################################################################
# Initialize the client (picks up your API key automatically from env vars, or pass api_key explicitly)
client = genai.Client(api_key=os.environ["GEMINI_API_KEY"])


# Define the grounding tool
grounding_tool = types.Tool(google_search=types.GoogleSearch())

# Configure generation settings
config = types.GenerateContentConfig(tools=[grounding_tool])

##########################################################################################################


# print('May take up to few minutes...')

today = dt.datetime.today().weekday()
weekend = today - 4  # returns 1 for saturday, 2 for sunday
formattedDate = (
    (dt.datetime.today() - dt.timedelta(days=weekend)).strftime("%Y%m%d")
    if today >= 5
    else dt.datetime.today().strftime("%Y%m%d")
)

three_months_approx = dt.datetime.today() - dt.timedelta(days=90)
formattedDate_3m_ago = three_months_approx.strftime("%Y%m%d")

date_kr = dt.datetime.strptime(formattedDate, "%Y%m%d").strftime("%-m월 %-d일")
date_kr_month = dt.datetime.strptime(formattedDate, "%Y%m%d").strftime("%-m월")
date_kr_ymd = dt.datetime.strptime(formattedDate, "%Y%m%d").strftime(
    "%Y년 %-m월 %-d일"
)  # Unix

esg_dict = {
    "LAG_PERF": "미흡",
    "AVG_PERF": "보통",
    "LEAD_PERF": "우수",
}

data = []
data_lock = threading.Lock()


def get_tickers(country: str, limit: int, sp500: bool):
    if country is not None:
        return get_tickers_by_country(country, limit, fmp_key)  # US, JP, KR
    elif sp500:
        return pl.read_csv(
            "https://datahub.io/core/s-and-p-500-companies/r/constituents.csv"
        )["Symbol"].to_list()
    elif not sp500:
        nasdaq100_url = "https://en.wikipedia.org/wiki/NASDAQ-100"
        nasdaq100 = pd.read_html(nasdaq100_url, header=0)[
            4
        ]  # Might need to adjust index (5th table on the page)
        return nasdaq100["Ticker"].tolist()
    else:
        raise Exception("No tickers list satisfies the given parameter")


def get_tickers_by_country(country: str, limit: int, apikey: str):
    url = "https://financialmodelingprep.com/api/v3/stock-screener"
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    }

    exchanges = ["nyse", "nasdaq", "amex"]
    all_stocks = []

    try:
        for exchange in exchanges:
            params = {
                "exchange": exchange,
                "limit": 500,  # 넉넉히 가져오기
                "type": "stock",
                "isEtf": False,
                "isFund": False,
                "apikey": apikey,
            }
            r = requests.get(url, headers=headers, params=params)
            r.raise_for_status()
            data = r.json()
            all_stocks.extend(data)

        # 시가총액 기준 정렬
        sorted_stocks = sorted(
            all_stocks, key=lambda x: x.get("marketCap", 0), reverse=True
        )

        # 중복 제거 및 상위 limit만 추출
        seen = set()
        unique_sorted = []
        for stock in sorted_stocks:
            symbol = stock.get("symbol")
            if symbol and symbol not in seen:
                seen.add(symbol)
                unique_sorted.append(symbol)
            if len(unique_sorted) >= limit:
                break

        return unique_sorted

    except Exception as e:
        print(f"Error: {e}")
        return []


def safe_check(val):
    return val is not None and not (isinstance(val, float) and np.isnan(val))


def quant_style_score(
    price_vs_fair_upper=None,
    price_vs_fair_lower=None,
    fcf_yield_rank=None,
    fcf_vs_treasury_spread=None,
    per=None,
    per_rank=None,
    pbr_rank=None,
    de=None,
    cr=None,
    industry_per=None,
    roe_z=None,
    roa_z=None,
    roe=None,
    roa=None,
    icr=None,
    fcf_cagr_rank=None,
    eps_cagr_rank=None,
    div_cagr_rank=None,
    eps=None,
    div_yield=None,
    opinc_yoy=None,
    opinc_qoq=None,
    industry_roe=None,
    industry_roa=None,
):
    valuation_score = 0
    earnings_momentum_score = 0

    # === Valuation (30점) ===
    if safe_check(price_vs_fair_upper) and price_vs_fair_upper > 0:
        valuation_score += min(price_vs_fair_upper * 5, 1.5)  # DCF 5%
    if safe_check(price_vs_fair_lower) and price_vs_fair_lower > 0:
        valuation_score += min(price_vs_fair_lower * 6, 1.5)  # 보수적 DCF
    if safe_check(fcf_vs_treasury_spread):
        if fcf_vs_treasury_spread > 0:
            valuation_score += min(fcf_vs_treasury_spread * 10, 1.5)  # FCF spread
        else:
            valuation_score -= 0.5
    if safe_check(fcf_yield_rank):
        valuation_score += fcf_yield_rank * 1.5  # FCF 수익률 5%
    if safe_check(per_rank):
        valuation_score += (1 - per_rank) * 3.0  # PER/EY 10%
    if safe_check(per) and safe_check(industry_per):
        if per < industry_per * 0.7:
            valuation_score += 0.5
        elif per > industry_per * 1.3:
            valuation_score -= 0.5
    if safe_check(pbr_rank):
        valuation_score += (1 - pbr_rank) * 1.5  # PBR 5%
    if safe_check(de):
        if 0 < de <= 0.5:
            valuation_score += 0.75
        elif de > 1.0:
            valuation_score -= 0.5
    if safe_check(cr):
        if 1.5 <= cr <= 2.5:
            valuation_score += 0.75
        elif cr < 1.0:
            valuation_score -= 0.5

    # === Fundamental Momentum (20점) ===
    if safe_check(roe_z):
        earnings_momentum_score += min(max(roe_z, -2), 2) * 1.25  # ROE 개선
    if safe_check(roa_z):
        earnings_momentum_score += min(max(roa_z, -2), 2) * 1.25  # ROA 개선
    if not safe_check(roe_z) and safe_check(roe) and safe_check(industry_roe):
        if roe > industry_roe:
            earnings_momentum_score += 0.5
    if not safe_check(roa_z) and safe_check(roa) and safe_check(industry_roa):
        if roa > industry_roa:
            earnings_momentum_score += 0.5
    if safe_check(icr):
        if icr >= 10:
            earnings_momentum_score += 1
        elif icr >= 5:
            earnings_momentum_score += 0.5
        elif icr < 1:
            earnings_momentum_score -= 0.5
    if safe_check(fcf_cagr_rank):
        earnings_momentum_score += fcf_cagr_rank * 1.5
    if safe_check(eps_cagr_rank):
        earnings_momentum_score += eps_cagr_rank * 1.5
    if safe_check(div_cagr_rank):
        earnings_momentum_score += div_cagr_rank * 1.0
    if safe_check(eps):
        if eps >= 0.3:
            earnings_momentum_score += 1.5
        elif eps >= 0.1:
            earnings_momentum_score += 0.75
        elif eps < 0:
            earnings_momentum_score -= 1
    if safe_check(div_yield):
        if div_yield >= 0.1:
            earnings_momentum_score += 0.75
        elif div_yield >= 0.08:
            earnings_momentum_score += 0.5
        elif div_yield >= 0.06:
            earnings_momentum_score += 0.25
        elif div_yield < 0.02:
            earnings_momentum_score -= 0.5
    if safe_check(opinc_yoy):
        if opinc_yoy > 0.2:
            earnings_momentum_score += 1.5
        elif opinc_yoy > 0.05:
            earnings_momentum_score += 1
        elif opinc_yoy < 0:
            earnings_momentum_score -= 1
    if safe_check(opinc_qoq):
        if opinc_qoq > 0.1:
            earnings_momentum_score += 1
        elif opinc_qoq > 0:
            earnings_momentum_score += 0.5
        elif opinc_qoq < 0:
            earnings_momentum_score -= 0.5

    return round(valuation_score, 2), round(earnings_momentum_score, 2)


def get_fcf_yield_and_cagr(ticker, yf_ticker, api_key="YOUR_API_KEY"):
    def try_fmp(ticker, api_key):
        try:
            # 1. Market cap
            profile_url = f"https://financialmodelingprep.com/api/v3/profile/{ticker}?apikey={api_key}"
            profile_resp = requests.get(profile_url)
            if profile_resp.status_code != 200 or not profile_resp.json():
                return (None, None, [])

            market_cap = profile_resp.json()[0].get("mktCap")
            if market_cap is None or market_cap == 0:
                return (None, None, [])

            # 2. FCF list
            url = f"https://financialmodelingprep.com/api/v3/cash-flow-statement/{ticker}?limit=10&apikey={api_key}"
            response = requests.get(url)
            if response.status_code != 200 or not response.json():
                return (None, None, [])

            data = response.json()
            fcf_list = [
                item["freeCashFlow"]
                for item in data
                if item.get("freeCashFlow") is not None
            ]
            fcf_list = fcf_list[::-1]

            if len(fcf_list) < 2:
                return (None, None, fcf_list)

            # 3. FCF Yield
            latest_fcf = fcf_list[-1]
            fcf_yield = round((latest_fcf / market_cap) * 100, 2) if latest_fcf else 0.0

            # 4. FCF CAGR
            initial_fcf, final_fcf = fcf_list[0], fcf_list[-1]
            n_years = len(fcf_list) - 1
            if initial_fcf <= 0 or final_fcf <= 0:
                fcf_cagr = None
            else:
                fcf_cagr = round(
                    ((final_fcf / initial_fcf) ** (1 / n_years) - 1) * 100, 2
                )

            return (fcf_yield, fcf_cagr, fcf_list)

        except Exception:
            return (None, None, [])

    def try_yf(ticker):
        try:

            market_cap = ticker.info.get("marketCap")
            if market_cap is None or market_cap == 0:
                return (None, None, [])

            cashflow_df = ticker.cashflow
            if cashflow_df.empty or "Free Cash Flow" not in cashflow_df.index:
                return (None, None, [])

            fcf_series = cashflow_df.loc["Free Cash Flow"].dropna()[::-1]
            if len(fcf_series) < 2:
                return (None, None, fcf_series.tolist())

            fcf_list = fcf_series.tolist()
            latest_fcf = fcf_series.iloc[-1]
            fcf_yield = (
                round((latest_fcf / market_cap) * 100, 2) if latest_fcf else None
            )

            initial_fcf, final_fcf = fcf_series.iloc[0], fcf_series.iloc[-1]
            n_years = len(fcf_series) - 1
            if initial_fcf <= 0 or final_fcf <= 0:
                fcf_cagr = None
            else:
                fcf_cagr = round(
                    ((final_fcf / initial_fcf) ** (1 / n_years) - 1) * 100, 2
                )

            return (fcf_yield, fcf_cagr, fcf_list)

        except Exception:
            return (None, None, [])

    result = try_yf(yf_ticker)
    if result is not None and (
        result[0] is not None
        or result[1] is not None
        or (result[2] and len(result[2]) > 0)
    ):
        return result

    result = try_fmp(ticker, api_key)
    # FMP 결과가 모두 None/빈 리스트면 yfinance 시도
    if result is not None and (
        result[0] is not None
        or result[1] is not None
        or (result[2] and len(result[2]) > 0)
    ):
        return result

    return (None, None, [])  # Always return a tuple


def get_10yr_treasury_yield():
    try:
        tnx = yf.Ticker("^TNX")
        tnx_data = tnx.history(period="1d")
        latest_yield = tnx_data["Close"].iloc[-1]
        return round(latest_yield, 2)  # Already in percent
    except Exception as e:
        return f"Error fetching yield: {e}"


def dcf_valuation(
    fcf_history,
    discount_rate,
    long_term_growth,
    years=10,
    shares_outstanding=None,
    cagr=0.05,
):

    if not fcf_history or len(fcf_history) < 2:
        return (None, None)

    start_fcf = fcf_history[0]  # oldest
    end_fcf = fcf_history[-1]  # most recent

    if start_fcf <= 0 or end_fcf <= 0:
        return (None, None)

    if discount_rate <= long_term_growth:
        return (None, None)  # Invalid terminal growth assumption

    if cagr is None or cagr <= 0:
        return (None, None)  # Invalid CAGR assumption
    est_cagr = cagr / 100.0 * 0.6  # conservative
    growth_rate = min(est_cagr, discount_rate)  # Ensure growth rate is reasonable

    # Project FCFs
    projected_fcfs = [end_fcf * (1 + growth_rate) ** i for i in range(1, years + 1)]

    # Terminal Value
    terminal_value = (
        projected_fcfs[-1] * (1 + long_term_growth) / (discount_rate - long_term_growth)
    )

    # Discounting
    discounted_fcfs = [
        fcf / ((1 + discount_rate) ** i) for i, fcf in enumerate(projected_fcfs, 1)
    ]
    discounted_terminal_value = terminal_value / ((1 + discount_rate) ** years)

    enterprise_value = sum(discounted_fcfs) + discounted_terminal_value

    if shares_outstanding is None or shares_outstanding <= 0:
        return (None, None)

    intrinsic_value = enterprise_value / shares_outstanding

    return (
        float(intrinsic_value),
        growth_rate * 100,
    )  # Return intrinsic value and growth rate in percentage


def get_trading_volume_vs_avg20(ticker_symbol: str) -> float:
    try:
        # Fetch 21 days of data
        ticker = yf.Ticker(ticker_symbol)
        hist = ticker.history(period="21d")

        if len(hist) < 21:
            return None  # Not enough data

        today_volume = hist["Volume"].iloc[-1]
        avg_volume_20 = hist["Volume"].iloc[:-1].mean()

        if avg_volume_20 == 0 or pd.isna(today_volume) or pd.isna(avg_volume_20):
            return None  # Invalid data

        # Return ratio (e.g., 1.5 means 150% of avg volume)
        return round(today_volume / avg_volume_20, 1)

    except Exception as e:
        print(f"[Error] {ticker_symbol}: {e}")
        return None


def has_stable_dividend_growth_cagr(ticker):
    try:
        stock = ticker
        divs = stock.dividends

        if divs.empty:
            return None

        # Sum dividends annually
        annual_divs = divs.groupby(divs.index.year).sum()

        # Need at least 10 full years
        if len(annual_divs) < 10:
            return None

        # Get last full year (last year completed)
        last_year = dt.datetime.today().year - 1

        # Ensure we have dividends data up to last_year
        if last_year not in annual_divs.index:
            return None

        # Select exactly 10 years ending at last_year
        recent_years = sorted(annual_divs.index)
        recent_years = [
            year for year in recent_years if last_year - 9 <= year <= last_year
        ]

        if len(recent_years) < 10:
            return None

        last_10_divs = [annual_divs[year] for year in recent_years]

        div_start = last_10_divs[0]
        div_end = last_10_divs[-1]

        # Validate data
        if div_start <= 0 or div_end <= 0:
            return None

        periods = len(last_10_divs) - 1  # 9 periods for 10 years

        cagr = (div_end / div_start) ** (1 / periods) - 1

        return cagr  # returns float (e.g., 0.05 for 5%)

    except Exception:
        return None


def compute_eps_growth_slope(ticker):
    try:
        income_stmt = ticker.financials  # Annual financials DataFrame

        if "Diluted EPS" not in income_stmt.index:
            return None

        eps_series = income_stmt.loc["Diluted EPS"].dropna()
        eps_series = eps_series.sort_index()  # Oldest to newest

        # Keep last 5 years
        current_year = dt.datetime.today().year
        eps_series = eps_series[
            [col for col in eps_series.index if col.year >= current_year - 5]
        ]

        if len(eps_series) < 2:
            return None

        eps_list = eps_series.tolist()

        x = list(range(len(eps_list)))
        slope, _, _, _, _ = linregress(x, eps_list)
        return slope

    except Exception:
        return None


# gets the most recent interest coverage ratio available
def get_interest_coverage_ratio(ticker):
    financials = (
        ticker.financials
    )  # Annual financials, columns = dates (most recent first)
    ratio = None
    if not financials.columns.empty:
        for date in financials.columns:
            if date.year < dt.datetime.today().year - 5:  # sift out old data
                return None

            try:
                ebit = financials.loc["Operating Income", date]
                interest_expense = financials.loc["Interest Expense", date]
                if (
                    math.isnan(interest_expense)
                    or math.isnan(ebit)
                    or not interest_expense
                    or ebit is None
                    or interest_expense is None
                ):
                    continue  # Avoid division by zero
                else:
                    ratio = round((ebit / abs(interest_expense)), 2)
                    break
            except KeyError:
                continue
        return ratio
    else:
        return None


### 1mo ver.
def get_percentage_change(ticker):
    ticker_obj = yf.Ticker(ticker)

    # Get last 1 month of price data
    data = ticker_obj.history(period="1mo")

    if len(data) >= 2:
        start_close = data["Close"].iloc[0]
        end_close = data["Close"].iloc[-1]

        if start_close != 0:
            percent_change = ((end_close - start_close) / start_close) * 100
            sign = "+" if percent_change >= 0 else ""
            return f" ({sign}{percent_change:.2f}%)"
        else:
            return " ()"
    else:
        return " ()"


def get_percentage_change_ttm(ticker):
    ticker_obj = yf.Ticker(ticker)
    today = date.today()
    start_of_month = today.replace(day=1)

    # Fetch price data from start of month to today

    data = ticker_obj.history(start=start_of_month, end=today + timedelta(days=1))
    print(data)

    if len(data) >= 2:
        # Use the first available trading day as the start
        start_close = data["Close"].iloc[0]
        end_close = data["Close"].iloc[-1]

        if start_close and not pd.isna(start_close):
            percent_change = ((end_close - start_close) / start_close) * 100
            sign = "+" if percent_change >= 0 else ""
            return f" ({sign}{percent_change:.2f}%)"
        else:
            return " ()"
    else:
        return " ()"


def download_industry_per():
    # FullRatio의 산업별 PER 페이지 URL
    url = "https://fullratio.com/pe-ratio-by-industry"
    headers = {"User-Agent": "Mozilla/5.0"}

    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, "html.parser")

    # 테이블 찾기 (이때 table이 None인지 체크)
    table = soup.find("table")
    if table is None:
        raise Exception(
            "테이블을 찾을 수 없습니다. 구조가 바뀌었거나 JS로 로딩될 수 있습니다."
        )

    # tbody가 있는 경우
    tbody = table.find("tbody")
    if tbody:
        rows = tbody.find_all("tr")
    else:
        rows = table.find_all("tr")[1:]  # 헤더 제외

    # 각 행에서 데이터 추출
    per_data = []
    for row in rows:
        cols = row.find_all("td")
        if len(cols) >= 2:
            industry = cols[0].text.strip()
            pe_ratio = cols[1].text.strip()
            per_data.append({"Industry": industry, "P/E Ratio": pe_ratio})

    # 결과 출력
    return pl.DataFrame(per_data)


def download_industry_roe():
    url_roe = "https://fullratio.com/roe-by-industry"
    headers_roe = {"User-Agent": "Mozilla/5.0"}

    response_roe = requests.get(url_roe, headers=headers_roe)
    soup_roe = BeautifulSoup(response_roe.text, "html.parser")

    # 테이블 찾기 (이때 table이 None인지 체크)
    table_roe = soup_roe.find("table")
    if table_roe is None:
        raise Exception(
            "테이블을 찾을 수 없습니다. 구조가 바뀌었거나 JS로 로딩될 수 있습니다."
        )

    # tbody가 있는 경우
    tbody_roe = table_roe.find("tbody")
    if tbody_roe:
        rows_roe = tbody_roe.find_all("tr")
    else:
        rows_roe = table_roe.find_all("tr")[1:]  # 헤더 제외

    # 각 행에서 데이터 추출
    roe_data = []
    for row in rows_roe:
        cols_roe = row.find_all("td")
        if len(cols_roe) >= 2:
            industry_roe = cols_roe[0].text.strip()
            roe_num = cols_roe[1].text.strip()
            roe_data.append({"Industry": industry_roe, "ROE": roe_num})

    # 결과 출력
    return pl.DataFrame(roe_data)


def download_industry_roa():
    url_roa = "https://fullratio.com/roa-by-industry"
    headers_roa = {"User-Agent": "Mozilla/5.0"}

    response_roa = requests.get(url_roa, headers=headers_roa)
    soup_roa = BeautifulSoup(response_roa.text, "html.parser")

    # 테이블 찾기 (이때 table이 None인지 체크)
    table_roa = soup_roa.find("table")
    if table_roa is None:
        raise Exception(
            "테이블을 찾을 수 없습니다. 구조가 바뀌었거나 JS로 로딩될 수 있습니다."
        )

    # tbody가 있는 경우
    tbody_roa = table_roa.find("tbody")
    if tbody_roa:
        rows_roa = tbody_roa.find_all("tr")
    else:
        rows_roa = table_roa.find_all("tr")[1:]  # 헤더 제외

    # 각 행에서 데이터 추출
    roa_data = []
    for row in rows_roa:
        cols_roa = row.find_all("td")
        if len(cols_roa) >= 2:
            industry_roa = cols_roa[0].text.strip()
            roa_num = cols_roa[1].text.strip()
            roa_data.append({"Industry": industry_roa, "ROA": roa_num})

    df_roa = pl.DataFrame(roa_data)
    return pl.DataFrame(roa_data)


#
df_per = download_industry_per()
df_roe = download_industry_roe()
df_roa = download_industry_roa()


def get_industry_per(ind):
    try:
        if ind is not None:
            ans = float(
                df_per.filter(pl.col("Industry") == ind).select("P/E Ratio").item()
            )
            return ans
        return per
    except Exception:
        spy = yf.Ticker("SPY")
        spy_info = spy.info
        per = spy_info.get("trailingPE")
        return per


def get_industry_roe(ind):
    try:
        if ind is not None:
            ans = float(df_roe.filter(pl.col("Industry") == ind).select("ROE").item())
            return ans / 100.0
        else:
            return 0.08
    except Exception:
        return 0.08


def get_industry_roa(ind):
    try:
        if ind is not None:
            ans = float(df_roa.filter(pl.col("Industry") == ind).select("ROA").item())
            return ans / 100.0
        return 0.06
    except Exception:
        return 0.06


######## LOAD TICKERS ###########
raw_tickers = get_tickers(country, limit, sp500)

prohibited = {
    "NVL",
    "SOJE",
    "RY-PT",
    "CEO",
    "DUKB",
    "ATVI",
    "SQ",
    "AED",
    "BACRP",
    "BDXA",
    "VZA",
    "AMOV",
    "ANTM",
    "SOJC",
    "ACH",
    "TBC",
    "PXD",
    "TBB",
    "SOJD",
    "AFA",
    "AEH",
}


# no data on yfinance or frequently cause errors
def keep_ticker(t):
    return t not in prohibited


tickers = list(filter(keep_ticker, raw_tickers))
################################################################################
from yf_cache_downloader import get_tickers_by_country_cache, update_cache

# 예) 티커 리스트 받아오기 (limit, api_key는 yf_cache_downloader.py 내부 또는 외부에서 설정 가능)
tickers_for_cache = get_tickers_by_country_cache("US", limit=300, apikey=fmp_key)

# 필요 없는 티커 제외하기 (옵션)
tickers_to_remove = {
    "NVL",
    "SOJE",
    "RY-PT",
    "CEO",
    "DUKB",
    "ATVI",
    "SQ",
    "AED",
    "BACRP",
    "BDXA",
    "VZA",
    "AMOV",
    "ANTM",
    "SOJC",
    "ACH",
    "TBC",
    "PXD",
    "TBB",
    "SOJD",
    "AFA",
    "AEH",
}

tickers_for_cache = [t for t in tickers_for_cache if t not in tickers_to_remove]

# 캐시 업데이트 (누락된 데이터만 받아서 yf_cache_multi.csv 파일 갱신)
cache = update_cache(tickers_for_cache)

# 이제 cache에는 최신으로 채워진 데이터가 들어있음
print(cache.head())
print("캐시 데이터 범위:", cache.index.min(), "~", cache.index.max())

if isinstance(cache.columns, pd.MultiIndex):
    successful_tickers = set([col[0] for col in cache.columns if col[1] == "Close"])
else:
    successful_tickers = set(cache.columns)

# ✅ 최종 티커 리스트
tickers = [t for t in tickers if t in successful_tickers]

################################################################################

# Assume cache is already loaded and up-to-date (from yf_cache_downloader)

# Define the date range for last 1 year up to today
end_date = pd.Timestamp.today().normalize()
start_date = end_date - pd.Timedelta(days=365)

# Clip start_date and end_date to cache's available index range to avoid KeyErrors
start_date = max(start_date, cache.index.min())
end_date = min(end_date, cache.index.max())

# Slice cache by this date range
cache_slice = cache.loc[start_date:end_date]

# Extract Close prices only
if isinstance(cache_slice.columns, pd.MultiIndex):
    # Select columns where second level is 'Close'
    close_cols = [col for col in cache_slice.columns if col[1] == "Close"]
    df_close = cache_slice[close_cols].copy()
    # Rename columns to just ticker symbols
    df_close.columns = [col[0] for col in df_close.columns]
else:
    # Single-level columns (unlikely if your cache is multi-indexed)
    if "Close" in cache_slice.columns:
        df_close = cache_slice[["Close"]].copy()
        df_close.columns = [tickers[0]]  # Assuming one ticker
    else:
        raise ValueError("❌ 'Close' column not found in cache.")

# Drop any columns (tickers) with all NaN Close values
df_close.dropna(axis=1, how="all", inplace=True)

print("Date range in df_close:", df_close.index.min(), "to", df_close.index.max())
print("Number of tickers with Close data:", len(df_close.columns))
print(df_close.head())
###################################################################################
# 예: cache에서 Close 가격만 추출한 후
df_momentum = df_close.copy()


def check_momentum_conditions(ticker: str) -> dict:
    result = {
        "ma_crossover": False,
        "ma_crossover_lt": False,
        "return_20d": False,
        "return_60d": False,
        "rsi_rebound": False,
        "macd_golden_cross": False,
    }

    try:
        # 티커가 데이터에 없으면 바로 반환
        if ticker not in df_momentum.columns:
            print(f"[Error] {ticker} not in df_momentum.columns")
            return result

        # 개별 종목 시계열 추출 후 'Close'로 컬럼명 통일
        df_ticker = df_momentum[[ticker]].copy()
        df_ticker.columns = ["Close"]

        # 결측치 처리
        df_ticker["Close"] = df_ticker["Close"].ffill()

        if df_ticker["Close"].isna().all():
            print(f"[Error] All 'Close' values are NaN for {ticker}")
            return result

        if len(df_ticker) < 22:
            print(
                f"[Warning] Not enough data rows for 20-day return calculation for {ticker} (rows={len(df_ticker)})"
            )
            return result

        # 이동평균선 계산
        df_ticker["MA20"] = df_ticker["Close"].rolling(window=5).mean()
        df_ticker["MA60"] = df_ticker["Close"].rolling(window=20).mean()

        if pd.notna(df_ticker["MA20"].iloc[-1]) and pd.notna(
            df_ticker["MA60"].iloc[-1]
        ):
            if df_ticker["MA20"].iloc[-1] > df_ticker["MA60"].iloc[-1]:
                result["ma_crossover"] = True

        df_ticker["MA50"] = df_ticker["Close"].rolling(window=50).mean()
        df_ticker["MA200"] = df_ticker["Close"].rolling(window=200).mean()

        if pd.notna(df_ticker["MA50"].iloc[-1]) and pd.notna(
            df_ticker["MA200"].iloc[-1]
        ):
            if df_ticker["MA50"].iloc[-1] > df_ticker["MA200"].iloc[-1]:
                result["ma_crossover_lt"] = True

        # 20일 수익률 계산
        try:
            return_20d = (
                df_ticker["Close"].iloc[-1] / df_ticker["Close"].iloc[-21] - 1
            ) * 100
            if return_20d >= 10:
                result["return_20d"] = True
        except IndexError:
            print(
                f"[Warning] Not enough data for 20-day return calculation for {ticker}"
            )

        # 60일 수익률 계산
        try:
            return_60d = (
                df_ticker["Close"].iloc[-1] / df_ticker["Close"].iloc[-61] - 1
            ) * 100
            if return_60d >= 10:
                result["return_60d"] = True
        except IndexError:
            print(
                f"[Warning] Not enough data for 60-day return calculation for {ticker}"
            )

        # RSI
        try:
            rsi = ta.momentum.RSIIndicator(df_ticker["Close"], window=14).rsi()
            if len(rsi) >= 7:
                recent = rsi.iloc[-7:]
                if (
                    all(recent > 50)
                    and recent.iloc[-1] < 70
                    and recent.is_monotonic_increasing
                ):
                    result["rsi_rebound"] = True
        except Exception as e:
            print(f"[RSI Error] {ticker}: {e}")

        # MACD
        try:
            macd_obj = ta.trend.MACD(df_ticker["Close"])
            macd_line = macd_obj.macd()
            signal_line = macd_obj.macd_signal()

            if len(macd_line) >= 7:
                macd_recent = macd_line.iloc[-7:]
                signal_recent = signal_line.iloc[-7:]
                if (macd_recent > signal_recent).sum() >= 5 and macd_recent.iloc[
                    -1
                ] > signal_recent.iloc[-1]:
                    if macd_recent.iloc[-1] > macd_recent.iloc[0]:
                        result["macd_golden_cross"] = True
        except Exception as e:
            print(f"[MACD Error] {ticker}: {e}")

    except Exception as e:
        print(f"[Download Error] Ticker {ticker}: {e}")

    return result


def check_momentum_conditions_batch(tickers: list) -> pd.DataFrame:
    results = []
    for ticker in tickers:
        # print(f"Processing {ticker} ...")
        res = check_momentum_conditions(ticker)
        res["Ticker"] = ticker
        results.append(res)
    # 결과 리스트를 DataFrame으로 변환 (Ticker 컬럼 첫 칼럼으로 이동)
    df_results = pd.DataFrame(results)
    cols = ["Ticker"] + [c for c in df_results.columns if c != "Ticker"]
    df_results = df_results[cols]
    return df_results


df_batch_result = check_momentum_conditions_batch(tickers)


def score_momentum(ma, ma_lt, ret_20d, ret_60d, rsi, macd):
    score = 0

    # 이동평균 크로스오버 (단기, 장기)
    if ma:  # 단기 MA 크로스 오버 신호 (True/False)
        score += 10
    if ma_lt:  # 장기 MA 크로스 오버 신호 (True/False)
        score += 15

    # RSI 과매도 반등 (True/False)
    if rsi:
        score += 20

    # MACD 골든크로스 (True/False)
    if macd:
        score += 20

    # 단기 수익률 반영 (예: 20일 수익률)
    if ret_20d is not None:
        if ret_20d > 0:
            score += min(ret_20d * 100, 10)  # 0~10점, 1% 상승시 1점

    # 중기 수익률 반영 (예: 60일 수익률)
    if ret_60d is not None:
        if ret_60d > 0:
            score += min(ret_60d * 100, 15)  # 0~15점
        # else:
        #     curr_score = score # 현재 점수
        #     curr_score = curr_score * 0.5
        #     score = max(score + ret_60d * 100, curr_score)

    return round(score, 2)


def get_operating_income_yoy(ticker_obj):
    try:
        financials = ticker_obj.financials

        if "Operating Income" not in financials.index:
            return None

        operating_income = financials.loc["Operating Income"].dropna()
        operating_income = operating_income.sort_index()  # 오래된 순 정렬

        if len(operating_income) < 2:
            return None

        # 최근 2년치 영업이익
        latest = operating_income.iloc[-1]
        prev = operating_income.iloc[-2]

        if prev == 0:
            return None  # 0으로 나누기 방지

        yoy_growth = (latest - prev) / abs(prev)
        return yoy_growth

    except Exception:
        return None


def get_operating_income_qoq(ticker_obj):
    try:
        financials = ticker_obj.quarterly_financials

        if "Operating Income" not in financials.index:
            return None

        operating_income = financials.loc["Operating Income"].dropna()
        operating_income = operating_income.sort_index()  # 오래된 순 정렬

        if len(operating_income) < 2:
            return None

        latest = operating_income.iloc[-1]
        prev = operating_income.iloc[-2]

        if prev == 0:
            return None

        qoq_growth = (latest - prev) / abs(prev)
        return qoq_growth

    except Exception:
        return None


def score_intrinsic_value(
    conf_lower, conf_upper, current_price, fcf_yield, tenyr_treasury_yield, fcf_cagr
):
    score = 0

    if conf_lower is not None and conf_upper is not None and current_price is not None:
        if current_price < conf_upper:
            score += 2  # price is within fair value range
            if current_price <= conf_lower:
                score += 3  # price is at or below lower bound of fair value range
        else:
            score -= 3  # price outside fair value range

    if fcf_yield is not None:
        if fcf_yield > tenyr_treasury_yield:
            score += 2
        elif fcf_yield < tenyr_treasury_yield:
            score -= 1

    if fcf_cagr is not None:
        if fcf_cagr >= 0:
            score += 2
        else:
            score -= 1

    return score


def monte_carlo_dcf_valuation(
    info,
    initial_fcf,
    wacc,
    terminal_growth_rate,
    projection_years=5,
    num_simulations=10_000,
):
    if initial_fcf <= 0:
        return (None, None)
    if wacc <= terminal_growth_rate:
        return (None, None)

    if projection_years <= 0 or num_simulations <= 0:
        return (None, None)

    shares_outstanding = info.get("sharesOutstanding")
    if not shares_outstanding or shares_outstanding <= 0:
        return (None, None)

    total_debt = info.get("totalDebt") or 0
    cash = info.get("totalCash") or 0
    net_debt = total_debt - cash

    growth_mean = 0.08
    growth_std = 0.03

    equity_values = []

    for _ in range(num_simulations):
        fcf = initial_fcf
        total_value = 0

        for year in range(1, projection_years + 1):
            growth_rate = np.random.normal(growth_mean, growth_std)
            fcf *= 1 + growth_rate
            discounted_fcf = fcf / ((1 + wacc) ** year)
            total_value += discounted_fcf

        terminal_value = (
            fcf * (1 + terminal_growth_rate) / (wacc - terminal_growth_rate)
        )
        discounted_terminal_value = terminal_value / ((1 + wacc) ** projection_years)

        enterprise_value = total_value + discounted_terminal_value
        equity_value = enterprise_value - net_debt

        equity_values.append(equity_value)

    equity_values = np.array(equity_values)
    fair_value_per_share = equity_values / shares_outstanding

    # mean_val = np.mean(fair_value_per_share)
    # median_val = np.median(fair_value_per_share)
    # std_val = np.std(fair_value_per_share)
    conf_lower = np.percentile(fair_value_per_share, 2.5)
    conf_upper = np.percentile(fair_value_per_share, 97.5)

    return (float(conf_lower), float(conf_upper))


def analyze_moat(company_name: str, date_kr_ymd: str) -> str:
    prompt = f"""
당신은 경제적 해자(Moat) 분석과 가치 함정(Value Trap) 탐지에 특화된 전문 투자 분석가입니다. 반드시 한국어로 답변하십시오.

{date_kr_ymd} 기준 "{company_name}"의 정보를 검색하여, 아래 네 가지 기준을 바탕으로 해당 기업의 **중장기 핵심 경쟁 우위(Moat)** 및 **Value Trap 리스크**를 모두 정성적·정량적으로 분석해 주세요.

---

🧱 [기준 1] {date_kr_ymd} 기준 경제적 해자 분석  
- 브랜드, 네트워크 효과, 특허/IP, 전환 비용 등 해자의 유형과 강도  
- 산업 내 지배력 또는 구조적 진입 장벽 존재 여부  
- 모방 또는 파괴적 혁신의 위협 가능성  

📉 [기준 2] {date_kr_ymd} 기준 실적 민감도 및 Value Trap 리스크  
- 최근 실적 발표에서 매출, 이익, 성장률 추세의 안정성  
- ROIC가 WACC를 초과하며 유지되는지 여부  
- 시장점유율, 마진, FCF 등 주요 지표의 하락 조짐  
- 일회성 수익 또는 비영업 항목 의존 여부  

⚔️ [기준 3] {date_kr_ymd} 기준 경쟁사 대비 해자 방어력  
- 경쟁사 대비 기술력, 제품력, 가격 경쟁력 우위 여부  
- 신제품 출시 속도, 규제 대응력, 유통력, 글로벌 진출력 비교  
- 산업 내 시장점유율 변화 추세  

⛳ [기준 4] {date_kr_ymd} 기준 경영진의 전략 대응력 및 자본 배분  
- 자사주 매입, 배당, 인수합병, R&D 등 자본 배분의 주주 친화성  
- 구조적 위기 대응 전략 보유 여부  
- CEO, CFO 등 경영진 리더십의 실행력  

---

⚠️ [감점 요인: {date_kr_ymd} 기준 Value Trap 시그널 하나라도 존재 시 강한 감점]  
- 본질적 펀더멘털 붕괴 징후  
- 경쟁사의 기술 혁신에 밀려 시장 점유율 하락
- 성장 산업 내 수익성·현금흐름·점유율 동반 하락  

---

📤 **반드시 아래 형식의 JSON으로만 간결하게 출력하십시오. 추가 설명이나 상세 분석은 포함하지 마십시오.**

```json
{{
  "moat_analysis": "기업의 중장기 핵심 경쟁 우위 및 Value Trap 위험성 요약 (반드시 2~3줄 이내 요약)",
  "moat_score": 0,  // 0에서 10 사이 정수 (아래 기준 참고)
}}

Moat Score 기준 (0~10):

0: 완전한 Commodity, 가격 경쟁 외 경쟁 우위 없음  
1-3: 경쟁 우위 미미~낮음, 차별화 부족, 시장 내 방어력 약함  
4-5: 부분적 경쟁력 보유, 일시적 우위 혹은 유지 불확실  
6-7: 상당한 경쟁 우위, 구조적 우위 있으나 대체 가능성 존재  
8-9: 뚜렷하고 장기적 경쟁 우위, 강력한 진입 장벽과 네트워크 효과 존재  
10: 절대적 독점 우위, 대체 불가능하며 진입 불가 수준  

※ 경쟁 우위가 약하거나 Value Trap 위험이 하나라도 감지되면 점수를 강하게 감점하고, 보수적으로 산정하십시오.  
"""
    return prompt.strip()


def parse_moat_response(response_text: str) -> dict:
    """
    LLM 응답에서 moat_analysis와 moat_score를 안전하게 추출합니다.
    JSON이 혼합되어 있거나 형식이 불완전할 경우에도 처리합니다.
    """
    # 기본값
    result = {"moat_analysis": response_text.strip(), "moat_score": None}

    # JSON 형식 추출 시도
    try:
        # 중괄호로 된 JSON 블럭 추출
        match = re.search(r"\{.*?\}", response_text, re.DOTALL)
        if match:
            json_block = match.group(0)
            parsed = json.loads(json_block)
            result["moat_analysis"] = parsed.get(
                "moat_analysis", result["moat_analysis"]
            ).strip()
            result["moat_score"] = (
                int(parsed.get("moat_score"))
                if parsed.get("moat_score") is not None
                else None
            )
            return result
    except (json.JSONDecodeError, ValueError, TypeError):
        pass  # continue to fallback logic

    # fallback 점수 추정 로직 (텍스트 기반 추론)
    lower_text = response_text.lower()
    if any(
        kw in lower_text
        for kw in ["절대적 독점", "완전한 독점", "대체 불가", "진입 불가", "특허 보호"]
    ):
        result["moat_score"] = 10
    elif any(
        kw in lower_text
        for kw in ["지속적 독점", "지속적인 독점", "강력한 진입 장벽", "규제 보호"]
    ):
        result["moat_score"] = 9
    elif any(
        kw in lower_text
        for kw in ["뚜렷한 경쟁 우위", "브랜드 파워", "규모의 경제", "전환 비용"]
    ):
        result["moat_score"] = 8
    elif any(
        kw in lower_text
        for kw in ["강한 경쟁력", "기술력", "유통망", "경쟁사 대비 우위"]
    ):
        result["moat_score"] = 7
    elif any(
        kw in lower_text for kw in ["상당한 경쟁 우위", "우위 요소 존재", "대체 가능성"]
    ):
        result["moat_score"] = 6
    elif any(
        kw in lower_text for kw in ["평균 이상의 경쟁력", "차별화 미약", "유지 불확실"]
    ):
        result["moat_score"] = 5
    elif any(
        kw in lower_text for kw in ["부분적 경쟁력", "일시적 수익성", "대체재 존재"]
    ):
        result["moat_score"] = 4
    elif any(
        kw in lower_text for kw in ["경쟁 우위 낮음", "차별화 거의 없음", "방어력 낮음"]
    ):
        result["moat_score"] = 3
    elif any(
        kw in lower_text for kw in ["미미한 경쟁 우위", "단기 유행", "구조적 우위 없음"]
    ):
        result["moat_score"] = 2
    elif any(
        kw in lower_text
        for kw in [
            "경쟁 우위 없음",
            "진입 장벽 없음",
            "브랜드 없음",
            "기술력 없음",
            "commoditized",
        ]
    ):
        result["moat_score"] = 1
    elif any(
        kw in lower_text for kw in ["commodity", "완전한 commodity", "완전 경쟁 시장"]
    ):
        result["moat_score"] = 0
    else:
        result["moat_score"] = -1  # 판단 불가 (예외 처리용)

    return result


def query_gemini(prompt: str) -> str:
    response = client.models.generate_content(
        model="gemini-2.5-flash",
        contents=prompt,
    )
    return response.text


retried_once = set()
q = Queue()
for ticker in tickers:
    q.put(ticker)


def process_ticker_quantitatives():
    while not q.empty():
        ticker = q.get()
        try:
            yf_ticker = yf.Ticker(ticker)
            info = yf_ticker.info
            beta = info.get("beta", None)
            name = info.get("shortName", ticker)
            industry = info.get("industry", None)
            currentPrice = info.get("currentPrice", None)
            percentage_change = get_percentage_change_ttm(ticker)

            # Valuation & Liquidity
            debtToEquity = info.get("debtToEquity", None)
            debtToEquity = debtToEquity / 100 if debtToEquity is not None else None
            currentRatio = info.get("currentRatio", None)
            pbr = info.get("priceToBook", None)
            per = info.get("trailingPE", None)

            # Industry
            industry_per = get_industry_per(industry)
            industry_per = round(industry_per) if industry_per is not None else None
            industry_roe = get_industry_roe(industry)
            industry_roa = get_industry_roa(industry)

            # Profitability
            roe = info.get("returnOnEquity", None)
            roa = info.get("returnOnAssets", None)
            icr = get_interest_coverage_ratio(yf_ticker)

            # Growth
            eps_cagr = compute_eps_growth_slope(yf_ticker)
            div_cagr = has_stable_dividend_growth_cagr(yf_ticker)
            opinc_yoy = get_operating_income_yoy(yf_ticker)
            opinc_qoq = get_operating_income_qoq(yf_ticker)

            # FCF & Valuation
            fcf_yield, fcf_cagr, fcf_list = get_fcf_yield_and_cagr(
                ticker, yf_ticker, api_key=fmp_key
            )
            tenyr_treasury_yield = get_10yr_treasury_yield()
            discount_rate = (
                (tenyr_treasury_yield + (beta * 5.0)) / 100.0
                if beta is not None
                else (tenyr_treasury_yield + 5.0) / 100.0
            )
            terminal_growth_rate = 0.02

            initial_fcf = (
                fcf_list[-1] if fcf_list and fcf_list[-1] is not None else None
            )

            intrinsic_value_range = (
                monte_carlo_dcf_valuation(
                    info,
                    initial_fcf,
                    discount_rate,
                    terminal_growth_rate,
                    projection_years=5,
                    num_simulations=10_000,
                )
                if initial_fcf is not None
                else (None, None)
            )

            result = {
                "ticker": ticker,
                "name": name,
                "price": currentPrice,
                "price_vs_fair_upper": (
                    ((intrinsic_value_range[1] - currentPrice) / currentPrice)
                    if intrinsic_value_range[1] and currentPrice
                    else None
                ),
                "price_vs_fair_lower": (
                    ((intrinsic_value_range[0] - currentPrice) / currentPrice)
                    if intrinsic_value_range[0] and currentPrice
                    else None
                ),
                "fcf_yield": fcf_yield,
                "per": per,
                "pbr": pbr,
                "de": debtToEquity,
                "cr": currentRatio,
                "roe": roe,
                "roa": roa,
                "icr": icr,
                "fcf_cagr": fcf_cagr,
                "eps_cagr": eps_cagr if isinstance(eps_cagr, float) else None,
                "div_cagr": div_cagr if isinstance(div_cagr, float) else None,
                "eps": eps_cagr if isinstance(eps_cagr, float) else None,
                "div_yield": (
                    info.get("dividendYield", div_cagr)
                    if div_cagr is not None
                    else None
                ),
                "opinc_yoy": opinc_yoy if isinstance(opinc_yoy, float) else None,
                "opinc_qoq": opinc_qoq if isinstance(opinc_qoq, float) else None,
                # Industry benchmarks
                "industry_per": industry_per,
                "industry_roe": industry_roe,
                "industry_roa": industry_roa,
                # NEW: fields needed for quant_style_score (will be filled later)
                "fcf_yield_rank": None,
                "per_rank": None,
                "pbr_rank": None,
                "fcf_cagr_rank": None,
                "eps_cagr_rank": None,
                "div_cagr_rank": None,
                "roe_z": None,
                "roa_z": None,
                # Misc.
                "industry": industry,
                "1M_Change": percentage_change,
            }

            with data_lock:
                data.append(result)

        except Exception as e:
            print(f"Error processing {ticker}: {e}")
            with data_lock:
                if ticker not in retried_once:
                    retried_once.add(ticker)
                    q.put(ticker)
            if "429" in str(e):
                print("Too many requests! Waiting 10 seconds...")
                time.sleep(10)

        finally:
            q.task_done()


threads = []

for _ in range(NUM_THREADS):
    t = threading.Thread(target=process_ticker_quantitatives)
    t.start()
    threads.append(t)

for t in threads:
    t.join()

q.join()

df = pd.DataFrame(data)

# Rank 계산
df["fcf_yield_rank"] = df["fcf_yield"].rank(pct=True)
df["per_rank"] = 1 - df["per"].rank(pct=True)
df["pbr_rank"] = 1 - df["pbr"].rank(pct=True)
df["fcf_cagr_rank"] = df["fcf_cagr"].rank(pct=True)
df["eps_cagr_rank"] = df["eps_cagr"].rank(pct=True)
df["div_cagr_rank"] = df["div_cagr"].rank(pct=True)
# 업종별 통계 계산
industry_stats = df.groupby("industry").agg(
    {"roe": ["mean", "std"], "roa": ["mean", "std"]}
)

industry_stats.columns = [
    "_".join(col).strip() for col in industry_stats.columns.values
]
industry_stats.index.name = "industry"

df = df.merge(industry_stats, left_on="industry", right_index=True, how="left")


def safe_z(x, mean, std):
    if pd.isna(x) or pd.isna(mean) or pd.isna(std) or std == 0:
        return 0
    return (x - mean) / std


df["roe_z"] = df.apply(
    lambda row: safe_z(row["roe"], row["roe_mean"], row["roe_std"]), axis=1
)
df["roa_z"] = df.apply(
    lambda row: safe_z(row["roa"], row["roa_mean"], row["roa_std"]), axis=1
)


def compute_quant_scores(df, tenyr_yield):
    scores = []
    for _, row in df.iterrows():
        valuation_score, momentum_score = quant_style_score(
            price_vs_fair_upper=row["price_vs_fair_upper"],
            price_vs_fair_lower=row["price_vs_fair_lower"],
            fcf_yield_rank=row["fcf_yield_rank"],
            fcf_vs_treasury_spread=(
                row["fcf_yield"] - tenyr_yield if row["fcf_yield"] is not None else None
            ),
            per=row["per"],
            per_rank=row["per_rank"],
            pbr_rank=row["pbr_rank"],
            de=row["de"],
            cr=row["cr"],
            industry_per=row["industry_per"],
            roe_z=row["roe_z"],
            roa_z=row["roa_z"],
            roe=row["roe"],  # 추가
            roa=row["roa"],  # 추가
            icr=row["icr"],
            fcf_cagr_rank=row["fcf_cagr_rank"],
            eps_cagr_rank=row["eps_cagr_rank"],
            div_cagr_rank=row["div_cagr_rank"],
            eps=row["eps"],
            div_yield=row["div_yield"],
            opinc_yoy=row["opinc_yoy"],
            opinc_qoq=row["opinc_qoq"],
            industry_roe=row["industry_roe"],
            industry_roa=row["industry_roa"],
        )
        scores.append(
            {
                "ticker": row["ticker"],
                "valuation_score": valuation_score,
                "momentum_score": momentum_score,
                "total_score": valuation_score + momentum_score,
            }
        )
    return pd.DataFrame(scores)


###############
def compute_price_flow_scores(df_main, df_batch_result):
    scores = []
    for ticker in df_main["ticker"]:
        row = df_batch_result.loc[df_batch_result["Ticker"] == ticker]
        if row.empty:
            scores.append(None)
            continue

        ma = bool(row["ma_crossover"].values[0])
        ma_lt = bool(row["ma_crossover_lt"].values[0])
        ret20 = row["return_20d"].values[0]
        ret60 = row["return_60d"].values[0]
        rsi = bool(row["rsi_rebound"].values[0])
        macd = bool(row["macd_golden_cross"].values[0])

        score = score_momentum(ma, ma_lt, ret20, ret60, rsi, macd)
        scores.append(score)
    return scores


# 메인 df에 price_flow_score 컬럼 추가
# Step 1: price_flow_score 먼저 계산
df["price_flow_score"] = compute_price_flow_scores(df, df_batch_result)

# Step 2: 퀀트 점수 계산 (valuation_score, momentum_score, total_score 등)
tenyr_yield = get_10yr_treasury_yield()
score_df = compute_quant_scores(df, tenyr_yield)

# Step 3: 두 결과 merge (이때 total_score가 생김)
final_df = df.merge(score_df, on="ticker", how="left")

# Step 4: total_score에 price_flow_score 더하기
final_df["total_score"] = final_df["total_score"].fillna(0) + final_df[
    "price_flow_score"
].fillna(0)


def normalize_series(series):
    min_val = series.min()
    max_val = series.max()
    if pd.isna(min_val) or pd.isna(max_val) or min_val == max_val:
        return pd.Series([0.0] * len(series), index=series.index)
    return (series - min_val) / (max_val - min_val) * 100  # ✅ Scale to 0–100


# Normalize each category to 0–100
final_df["valuation_score_norm"] = normalize_series(final_df["valuation_score"])
final_df["momentum_score_norm"] = normalize_series(final_df["momentum_score"])
final_df["price_flow_score_norm"] = normalize_series(
    final_df["price_flow_score"].fillna(0)
)

# Buffett-style
valuation_weight = 0.4
momentum_weight = 0.3
price_flow_weight = 0.3

# quant fund
# valuation_weight = 0.4
# momentum_weight = 0.4
# price_flow_weight = 0.2

final_df["total_score"] = (
    final_df["valuation_score_norm"] * valuation_weight
    + final_df["momentum_score_norm"] * momentum_weight
    + final_df["price_flow_score_norm"] * price_flow_weight
)

# Round the normalized scores and total
score_cols = [
    "valuation_score_norm",
    "momentum_score_norm",
    "price_flow_score_norm",
    "total_score",
]

final_df[score_cols] = final_df[score_cols].round()

# 1) rename_dict 정의
rename_dict = {
    "ticker": "티커",
    "name": "종목",  # 실제 final_df에 name 컬럼이 있으면
    "industry": "업종",
    "price": "현재가",
    "1M_Change": "1개월대비",
    "valuation_score_norm": "밸류에이션",
    "momentum_score_norm": "실적모멘텀",
    "price_flow_score_norm": "가격/수급",
    "total_score": "총점수",
}

# 2) 컬럼명 변경
final_df = final_df.rename(columns=rename_dict)

# 3) 내보낼 컬럼 리스트 (원하는 순서 및 컬럼만)
export_columns_kr = [
    "티커",
    "종목",
    "총점수",
    "업종",
    "현재가",
    "1개월대비",
    "밸류에이션",
    "실적모멘텀",
    "가격/수급",
]

# 4) 정렬
df = pd.DataFrame()
# 컬럼을 필터링한 새로운 df로 overwrite
df = (
    final_df[export_columns_kr]
    .sort_values(by="총점수", ascending=False)
    .reset_index(drop=True)
)
df = df.drop(columns=[col for col in df.columns if col not in export_columns_kr])

# 3️⃣ 컬럼 순서 맞추기 (혹시 순서 틀어졌을 수도 있으니)
df = df[export_columns_kr]
# 티커 기준으로 중복 제거 (첫 번째 항목만 남김)
df = df.drop_duplicates(subset="티커", keep="first")


# 그리고 그대로 저장
df.to_excel(excel_path, index=False)


# 6) 상위 티커 리스트 추출

top_tickers_news = df["티커"].head(news_lookup).tolist()


#################################################################
# Gemini 2.5 Flash API rate limits:
# rpm: 10 (requests per minute)
# tpm: 250,000 (tokens per minute)
# rpd: 250 (requests per day)

# 적절한 sleep_time 계산:
# - 1분에 10회 요청 가능 → 1회 요청 후 최소 6초 대기 필요 (60초 / 10회 = 6초)
# - 하루 250회 제한 → 250개 초과 시 추가 대기 필요
# - 토큰 제한은 일반 뉴스/모트 프롬프트에서는 거의 도달하지 않음

# 따라서 moat/news batch 함수에서 sleep_time=6~7초 권장
# 예시:
# moat_df = generate_moat_summary_batch(df, moat_limit, batch_size=10, sleep_time=7)
# (batch_size=10, sleep_time=7 → 1분에 최대 8~9회 요청, 안전)

# 만약 여러 스레드/프로세스에서 동시에 Gemini 호출 시, 반드시 전체 요청 합산이 rpm/rpd를 넘지 않도록 조정 필요


# 참고: 실제 운영 환경에서는 7초 이상(예: 8~10초)로 여유 있게 설정하면 더 안전함


def generate_moat_summary_batch(
    df: pd.DataFrame, batch_size: int = 10, sleep_time: int = 8
) -> pd.DataFrame:
    top_tickers = df["종목"].tolist()
    moat_data = []

    for i in range(0, len(top_tickers), batch_size):
        batch = top_tickers[i : i + batch_size]
        for ticker in batch:
            try:
                prompt = analyze_moat(ticker, date_kr_ymd)
                moat_text = query_gemini(prompt)
                parsed_response = parse_moat_response(moat_text)
                moat_data.append(
                    {
                        "기업명": ticker,
                        "경쟁 우위 분석": parsed_response["moat_analysis"],
                        "Moat 점수": parsed_response["moat_score"],
                    }
                )
                time.sleep(1)
            except Exception as e:
                moat_data.append(
                    {
                        "기업명": f"❌ 오류: {str(e)}",
                        "경쟁 우위 분석": "분석 실패",
                        "Moat 점수": "분석 실패",
                    }
                )
        if i + batch_size < len(top_tickers):
            print(
                f"Batch {i // batch_size + 1} completed. Sleeping {sleep_time} seconds to avoid rate limit..."
            )
            time.sleep(sleep_time)
    return pd.DataFrame(moat_data)


moat_df = generate_moat_summary_batch(df, batch_size=10, sleep_time=8)


#################################################################
# 1. ticker / 기업명 기준으로 moat_df를 df에 merge
df = df.merge(
    moat_df[["기업명", "Moat 점수"]],
    left_on="종목",  # final_df / df에서 기업명을 나타내는 컬럼
    right_on="기업명",  # moat_df에서 기업명 컬럼
    how="left",
)

# 2. Moat 점수 결측값은 0으로 채움
df["Moat 점수"] = df["Moat 점수"].fillna(0).astype(float)


df["moat_score_norm"] = normalize_series(df["Moat 점수"])


# 4. 기존 가중치 설정 (예: Buffett 스타일에 Moat 포함)
valuation_weight = 0.35
moat_weight = 0.35  # Moat 가중치 (조절 가능)
momentum_weight = 0.2
price_flow_weight = 0.1

# 5. 새 total_score 계산
df["총점수"] = (
    df["밸류에이션"] * valuation_weight
    + df["실적모멘텀"] * momentum_weight
    + df["가격/수급"] * price_flow_weight
    + df["moat_score_norm"] * moat_weight
)


score_cols = ["밸류에이션", "실적모멘텀", "가격/수급", "moat_score_norm", "총점수"]
df[score_cols] = df[score_cols].round()

# 7. 필요하면 정렬
df = df.sort_values(by="총점수", ascending=False).reset_index(drop=True)
df = df.drop(columns=["기업명", "moat_score_norm"])


#################################################################
def get_news_for_tickers(tickers, api_token):
    all_news = []

    for ticker in tickers:
        try:
            company_info = yf.Ticker(ticker).info
            full_name = company_info.get("shortName", "")
        except Exception as e:
            print(f"[{ticker}] ⚠️ Failed to retrieve company info: {e}")
            continue

        if not full_name:
            print(f"[{ticker}] ⚠️ No company name found, skipping.")
            continue

        published_after = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")
        url = "https://api.marketaux.com/v1/news/all"
        params = {
            "api_token": api_token,
            "symbols": ticker.upper(),
            "language": "en",
            "published_after": published_after,
            "limit": 5,  # Fetch extra to allow filtering
            "sort": "relevance",
        }

        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            articles = response.json().get("data", [])
        except Exception as e:
            print(f"[{ticker}] ❌ API request failed: {e}")
            continue

        filtered_articles = []

        for article in articles:
            relevant = False
            sentiment_score = None

            for entity in article.get("entities", []):
                if entity.get("symbol", "").upper() == ticker.upper():
                    relevant = True
                    score = entity.get("sentiment_score")
                    try:
                        sentiment_score = round(float(score), 2)
                    except (TypeError, ValueError):
                        sentiment_score = None
                    break

            if not relevant:
                continue

            filtered_articles.append(
                {
                    "기업명": full_name,
                    "기사 제목": article.get("title"),
                    "감정지수": sentiment_score,
                    "뉴스 요약": article.get("description"),
                    "발행일": article.get("published_at", "")[:10],
                    "URL": article.get("url"),
                }
            )

            if len(filtered_articles) >= 3:
                break

        if filtered_articles:
            all_news.extend(filtered_articles)
        else:
            print(f"[{ticker}] ℹ️ No relevant news articles found.")

    return pd.DataFrame(all_news)


#################################################################
news_df = get_news_for_tickers(top_tickers_news, api_token=marketaux_api)
#################################################################

# Seleccionar Criterio de Optimización
optimization_criterion = "sortino"  # Cambia a 'sharpe', 'cvar', 'sortino' o 'variance' para optimizar esos criterios
df = df.sort_values(by="총점수", ascending=False).reset_index(drop=True)
top_tickers = df["티커"].head(opt).tolist()
symbols = top_tickers

# 오늘 날짜
end_date = dt.datetime.today() - dt.timedelta(days=weekend)

# 1년 전 날짜 (365일 전)
start_date = end_date - timedelta(days=365)

# 문자열 포맷으로 변환 (yfinance에 맞게)
start_str = start_date.strftime("%Y-%m-%d")
end_str = end_date.strftime("%Y-%m-%d")

# 1. 'Close' 컬럼만 추출 (MultiIndex 전용)
if isinstance(cache.columns, pd.MultiIndex):
    # 'Close' 컬럼만 선택
    close_columns = [col for col in cache.columns if col[1] == "Close"]
    close_df = cache[close_columns].copy()
    close_df.columns = [col[0] for col in close_columns]  # → ('AAPL', 'Close') → 'AAPL'
else:
    raise ValueError(
        "Expected MultiIndex columns in cache, but got single-index DataFrame."
    )

# 2. 유효한 종목(symbols)만 추출
symbols_in_data = [s for s in symbols if s in close_df.columns]
if not symbols_in_data:
    raise ValueError("No valid symbols found in cached data.")

data = close_df[symbols_in_data]

# 3. 모두 NaN인 종목 제거
data = data.dropna(axis=1, how="all")

# 4. 제거된 티커 로깅
removed = [s for s in symbols if s not in data.columns]
for r in removed:
    print(f"⚠️  Removed due to all NaN: {r}")

# 5. 최종 검증
if data.empty or data.shape[1] == 0:
    raise ValueError("No valid data left after NaN filtering.")

returns = data.pct_change(fill_method="pad").dropna()


# Sharpe Ratio 최적화 함수
def objective_sharpe(weights):
    port_return = np.dot(weights, returns.mean()) * 252
    port_vol = np.sqrt(np.dot(weights.T, np.dot(returns.cov() * 252, weights)))
    return -port_return / port_vol  # 최대화 위해 음수


# CVaR 최적화 함수 (5% VaR 기준)
def objective_cvar(weights):
    portfolio_returns = returns.dot(
        weights
    )  # 수정: np.dot(returns, weights)도 가능하지만 DataFrame이면 .dot이 더 안전
    alpha = 0.05
    var = np.percentile(portfolio_returns, 100 * alpha)
    cvar = portfolio_returns[portfolio_returns <= var].mean()
    return cvar  # minimize에서 최소화(손실 최대화) → 부호 바꿔야 함
    # return -cvar  # CVaR 최대화하려면 음수로 반환


# Sortino Ratio 최적화 함수
def objective_sortino(weights):
    portfolio_returns = returns.dot(
        weights
    )  # 수정: np.dot(weights) → returns.dot(weights)
    mean_return = portfolio_returns.mean() * 252
    downside_returns = portfolio_returns[portfolio_returns < 0]
    downside_std = downside_returns.std() * np.sqrt(252)
    if downside_std == 0:
        return 0  # 또는 큰 값 반환
    sortino_ratio = mean_return / downside_std
    return -sortino_ratio  # 최대화 위해 음수


# 분산 최소화 함수
def objective_variance(weights):
    return np.dot(weights.T, np.dot(returns.cov() * 252, weights))


# Las restricciones
cons = {"type": "eq", "fun": lambda x: np.sum(x) - 1}

# Los límites para los pesos
bounds = tuple((0, 1) for x in range(len(symbols)))


# Optimización
init_guess = np.array(
    len(symbols)
    * [
        1.0 / len(symbols),
    ]
)
if optimization_criterion == "sharpe":
    opt_results = minimize(
        objective_sharpe, init_guess, method="SLSQP", bounds=bounds, constraints=cons
    )
elif optimization_criterion == "cvar":
    opt_results = minimize(
        objective_cvar, init_guess, method="SLSQP", bounds=bounds, constraints=cons
    )
elif optimization_criterion == "sortino":
    opt_results = minimize(
        objective_sortino, init_guess, method="SLSQP", bounds=bounds, constraints=cons
    )
elif optimization_criterion == "variance":
    opt_results = minimize(
        objective_variance, init_guess, method="SLSQP", bounds=bounds, constraints=cons
    )

# Los pesos óptimos
optimal_weights = opt_results.x


# Optimizar todos los criterios
opt_results_cvar = minimize(
    objective_cvar, init_guess, method="SLSQP", bounds=bounds, constraints=cons
)
opt_results_sortino = minimize(
    objective_sortino, init_guess, method="SLSQP", bounds=bounds, constraints=cons
)
opt_results_variance = minimize(
    objective_variance, init_guess, method="SLSQP", bounds=bounds, constraints=cons
)
opt_results_sharpe = minimize(
    objective_sharpe, init_guess, method="SLSQP", bounds=bounds, constraints=cons
)

# Pesos óptimos para cada criterio
optimal_weights_cvar = opt_results_cvar.x
optimal_weights_sortino = opt_results_sortino.x
optimal_weights_variance = opt_results_variance.x
optimal_weights_sharpe = opt_results_sharpe.x

# Graficar la frontera eficiente
port_returns = []
port_volatility = []
sharpe_ratio = []
all_weights = []  # almacena los pesos de todas las carteras simuladas

num_assets = len(symbols)
num_portfolios = 50000

np.random.seed(101)

for single_portfolio in range(num_portfolios):
    weights = np.random.random(num_assets)
    weights /= np.sum(weights)
    returns_portfolio = np.dot(weights, returns.mean()) * 252
    volatility = np.sqrt(np.dot(weights.T, np.dot(returns.cov() * 252, weights)))
    sr = returns_portfolio / volatility
    sharpe_ratio.append(sr)
    port_returns.append(returns_portfolio)
    port_volatility.append(volatility)
    all_weights.append(weights)  # registra los pesos para esta cartera

plt.figure(figsize=(12, 8))
plt.scatter(port_volatility, port_returns, c=sharpe_ratio, cmap="viridis")
plt.colorbar(label="Sharpe Ratio")
plt.xlabel("Volatility")
plt.ylabel("Return")

# Calcular y graficar los retornos y la volatilidad del portafolio óptimo para cada criterio
opt_returns_cvar = np.dot(optimal_weights_cvar, returns.mean()) * 252
opt_volatility_cvar = np.sqrt(
    np.dot(optimal_weights_cvar.T, np.dot(returns.cov() * 252, optimal_weights_cvar))
)
opt_portfolio_cvar = plt.scatter(
    opt_volatility_cvar, opt_returns_cvar, color="hotpink", s=50, label="CVaR"
)

opt_returns_sortino = np.dot(optimal_weights_sortino, returns.mean()) * 252
opt_volatility_sortino = np.sqrt(
    np.dot(
        optimal_weights_sortino.T, np.dot(returns.cov() * 252, optimal_weights_sortino)
    )
)
opt_portfolio_sortino = plt.scatter(
    opt_volatility_sortino, opt_returns_sortino, color="g", s=50, label="Sortino"
)

opt_returns_variance = np.dot(optimal_weights_variance, returns.mean()) * 252
opt_volatility_variance = np.sqrt(
    np.dot(
        optimal_weights_variance.T,
        np.dot(returns.cov() * 252, optimal_weights_variance),
    )
)
opt_portfolio_variance = plt.scatter(
    opt_volatility_variance, opt_returns_variance, color="b", s=50, label="Variance"
)

opt_returns_sharpe = np.dot(optimal_weights_sharpe, returns.mean()) * 252
opt_volatility_sharpe = np.sqrt(
    np.dot(
        optimal_weights_sharpe.T, np.dot(returns.cov() * 252, optimal_weights_sharpe)
    )
)
opt_portfolio_sharpe = plt.scatter(
    opt_volatility_sharpe, opt_returns_sharpe, color="r", s=50, label="Sharpe"
)

plt.legend(loc="upper right")

plt.show()


# Función para calcular el drawdown máximo
def max_drawdown(return_series):
    comp_ret = (1 + return_series).cumprod()
    peak = comp_ret.expanding(min_periods=1).max()
    dd = (comp_ret / peak) - 1
    return dd.min()


def detailed_portfolio_statistics(weights):
    portfolio_returns = returns.dot(weights)
    mean_return_annualized = gmean(portfolio_returns + 1) ** 252 - 1
    std_dev_annualized = portfolio_returns.std() * np.sqrt(252)
    skewness = skew(portfolio_returns)
    kurt = kurtosis(portfolio_returns)
    max_dd = max_drawdown(portfolio_returns)
    count = len(portfolio_returns)

    # ✅ Safe TNX fetch with fallback
    try:
        tnx = yf.Ticker("^TNX")
        tnx_data = tnx.history(period="1d")
        latest_yield = tnx_data["Close"].iloc[-1]
        risk_free_rate = round(latest_yield / 100.0, 2)
    except Exception as e:
        print(f"⚠️ Failed to fetch TNX: {e}")
        risk_free_rate = 0.04  # default 4% fallback

    sharpe_ratio = (mean_return_annualized - risk_free_rate) / std_dev_annualized

    # CVaR 계산 (5% 수준)
    alpha = 0.05
    sorted_returns = np.sort(portfolio_returns)
    var_index = int(np.floor(alpha * len(sorted_returns)))
    var = sorted_returns[var_index]
    cvar = sorted_returns[:var_index].mean()
    cvar_annualized = (1 + cvar) ** 252 - 1  # 연율화

    downside_returns = portfolio_returns[portfolio_returns < 0]
    downside_std_dev = downside_returns.std() * np.sqrt(252)
    sortino_ratio = (
        mean_return_annualized / downside_std_dev if downside_std_dev != 0 else np.nan
    )
    variance = std_dev_annualized**2

    return (
        mean_return_annualized,
        std_dev_annualized,
        skewness,
        kurt,
        max_dd,
        count,
        sharpe_ratio,
        cvar_annualized,
        sortino_ratio,
        variance,
    )


# Calcular estadísticas para cada portafolio
statistics_cvar = detailed_portfolio_statistics(optimal_weights_cvar)
statistics_sortino = detailed_portfolio_statistics(optimal_weights_sortino)
statistics_variance = detailed_portfolio_statistics(optimal_weights_variance)
statistics_sharpe = detailed_portfolio_statistics(optimal_weights_sharpe)

# Nombres de las estadísticas
statistics_names = [
    "Retorno anualizado",
    "Volatilidad anualizada",
    "Skewness",
    "Kurtosis",
    "Max Drawdown",
    "Conteo de datos",
    "Sharpe Ratio",
    "CVaR",
    "Ratio Sortino",
    "Varianza",
]

# Diccionario que asocia los nombres de los métodos de optimización con los pesos óptimos y las estadísticas
portfolio_data = {
    "CVaR": {
        "weights": optimal_weights_cvar,
        "statistics": detailed_portfolio_statistics(optimal_weights_cvar),
    },
    "Sortino": {
        "weights": optimal_weights_sortino,
        "statistics": detailed_portfolio_statistics(optimal_weights_sortino),
    },
    "Variance": {
        "weights": optimal_weights_variance,
        "statistics": detailed_portfolio_statistics(optimal_weights_variance),
    },
    "Sharpe": {
        "weights": optimal_weights_sharpe,
        "statistics": detailed_portfolio_statistics(optimal_weights_sharpe),
    },
}

# 1. 포트폴리오 비중 표 (각 방법별, 티커별 비중)
weight_rows = []
for method, data in portfolio_data.items():
    for symbol, weight in zip(symbols, data["weights"]):
        weight_rows.append(
            {"최적화 기준": method, "티커": symbol, "비중(%)": round(weight * 100, 2)}
        )
df_weights = pd.DataFrame(weight_rows)

# 2. 포트폴리오 통계 표 (각 방법별 통계 한 줄)
statistics_names_kr = [
    "연환산 수익률",
    "연환산 변동성",
    "왜도",
    "첨도",
    "최대 낙폭",
    "데이터 개수",
    "샤프 비율",
    "CVaR",
    "소르티노 비율",
    "분산",
]
stats_rows = []
for method, data in portfolio_data.items():
    stats_dict = {"최적화 기준": method}
    for name_kr, stat in zip(statistics_names_kr, data["statistics"]):
        # 숫자는 모두 소수점 둘째자리로 반올림, 데이터 개수는 정수로
        if name_kr == "데이터 개수":
            stats_dict[name_kr] = int(stat)
        else:
            stats_dict[name_kr] = np.round(stat, 2)
    stats_rows.append(stats_dict)
df_stats = pd.DataFrame(stats_rows)


def autofit_columns_and_wrap(ws, df: pd.DataFrame, workbook):
    # 픽셀 -> 문자 수 환산 (0.1428 배율 기준)
    pixel_widths = [92, 200, 50, 500, 85, 150]
    char_widths = [round(p * 0.1428) for p in pixel_widths]

    # wrap + top-align 포맷
    wrap_format = workbook.add_format({"text_wrap": True, "valign": "top"})

    # 헤더 작성 및 열 너비 설정
    for i, col in enumerate(df.columns):
        width = char_widths[i] if i < len(char_widths) else 20
        ws.set_column(i, i, width)
        ws.write(0, i, str(col), wrap_format)

    # 데이터 셀 작성
    for row in range(1, len(df) + 1):
        for col in range(len(df.columns)):
            val = df.iat[row - 1, col]

            # NaN / inf / None -> 문자열 변환
            if isinstance(val, float):
                if math.isnan(val) or math.isinf(val):
                    val = str(val)
            elif val is None:
                val = ""

            # Excel 쓰기 실패 대비 안전 write
            try:
                ws.write(row, col, val, wrap_format)
            except Exception:
                ws.write(row, col, str(val), wrap_format)


def autofit_columns_and_wrap_moat(ws, df: pd.DataFrame, workbook):

    # 열 너비 설정 (픽셀 기준 → 문자 기준으로 변환)
    pixel_widths = [92, 500]
    char_widths = [round(p * 0.1428) for p in pixel_widths]  # = [13, 71]

    # wrap + top-align 포맷
    wrap_format = workbook.add_format({"text_wrap": True, "valign": "top"})

    # 열 너비 및 헤더 설정
    for i, col in enumerate(df.columns):
        width = char_widths[i] if i < len(char_widths) else 20
        ws.set_column(i, i, width)
        ws.write(0, i, str(col), wrap_format)

    # 데이터 셀 작성
    for row in range(1, len(df) + 1):
        for col in range(len(df.columns)):
            val = df.iat[row - 1, col]

            # NaN / inf / None 처리
            if isinstance(val, float):
                if math.isnan(val) or math.isinf(val):
                    val = str(val)
            elif val is None:
                val = ""

            try:
                ws.write(row, col, val, wrap_format)
            except Exception:
                ws.write(row, col, str(val), wrap_format)


with pd.ExcelWriter(excel_path, engine="xlsxwriter") as writer:

    # 종목분석 시트 먼저 생성해야 함
    df.to_excel(
        writer, index=False, sheet_name="종목분석"
    )  # df_analysis는 종목분석 데이터프레임

    # 경쟁우위(Moat) 시트 저장 및 표 적용
    moat_df.to_excel(writer, index=False, sheet_name="경쟁우위분석")
    ws_moat = writer.sheets["경쟁우위분석"]
    (mr_moat, mc_moat) = moat_df.shape
    ws_moat.add_table(
        0,
        0,
        mr_moat,
        mc_moat - 1,
        {
            "columns": [{"header": col} for col in moat_df.columns],
            "style": "Table Style Medium 9",
        },
    )
    autofit_columns_and_wrap_moat(ws_moat, moat_df, writer.book)

    # 기존 포트폴리오비중 시트 대신 각 기준별로 나눠서 저장 (엑셀 표로)
    for method in ["CVaR", "Sortino", "Variance", "Sharpe"]:
        df_method = df_weights[df_weights["최적화 기준"] == method]

        df_method = df_method[df_method["비중(%)"] != 0]

        df_method.to_excel(writer, index=False, sheet_name=f"포트비중_{method}")
        ws = writer.sheets[f"포트비중_{method}"]
        (mr, mc) = df_method.shape
        ws.add_table(
            0,
            0,
            mr,
            mc - 1,
            {
                "columns": [{"header": col} for col in df_method.columns],
                "style": "Table Style Medium 9",
            },
        )

    # 포트폴리오통계 시트도 엑셀 표로
    df_stats.to_excel(writer, index=False, sheet_name="포트폴리오통계")
    ws_stats = writer.sheets["포트폴리오통계"]
    (mr_stats, mc_stats) = df_stats.shape
    ws_stats.add_table(
        0,
        0,
        mr_stats,
        mc_stats - 1,
        {
            "columns": [{"header": col} for col in df_stats.columns],
            "style": "Table Style Medium 9",
        },
    )

    # 뉴스 데이터프레임 시트 생성 및 표 적용
    news_df.to_excel(writer, index=False, sheet_name="종목뉴스")
    ws_news = writer.sheets["종목뉴스"]
    (nr, nc) = news_df.shape
    ws_news.add_table(
        0,
        0,
        nr,
        nc - 1,
        {
            "columns": [{"header": col} for col in news_df.columns],
            "style": "Table Style Medium 9",
        },
    )
    autofit_columns_and_wrap(ws_news, news_df, writer.book)

    workbook = writer.book
    # 1) df로 통일
    worksheet = writer.sheets["종목분석"]

    currency_format = workbook.add_format({"num_format": "$#,##.00"})

    # 4️⃣ "현재가" 컬럼 위치 구해서 서식 적용
    price_col_idx = df.columns.get_loc("현재가")  # 0부터 시작하는 인덱스
    for row in range(1, len(df) + 1):  # 헤더 제외, 1부터 시작
        value = df.at[row - 1, "현재가"]
        if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
            value = 0
        worksheet.write_number(row, price_col_idx, value, currency_format)

    start_row = 0  # data starts after header row 1
    end_row = len(df)
    start_col = 0
    end_col = len(df.columns) - 1

    def xl_col(col_idx):
        div = col_idx + 1
        string = ""
        while div > 0:
            div, mod = div // 26, div % 26
            if mod == 0:
                mod = 26
                div -= 1
            string = chr(64 + mod) + string
        return string

    first_cell = f"{xl_col(start_col)}{start_row + 1}"
    last_cell = f"{xl_col(end_col)}{end_row + 1}"
    data_range = f"{first_cell}:{last_cell}"

    # 1) Add Excel table for the data
    worksheet.add_table(
        data_range,
        {
            "columns": [{"header": col} for col in df.columns],
            "style": "Table Style Medium 9",
        },
    )

    # 5) 컬럼별 너비 지정
    col_widths = {
        "티커": 6,
        "종목": 25,
        "업종": 25,
        "현재가": 10,
        "1개월대비": 10,
    }
    for col_name, width in col_widths.items():
        if col_name in df.columns:
            col_idx = df.columns.get_loc(col_name)
            worksheet.set_column(col_idx, col_idx, width)

    # 6) 그라데이션 포맷팅 적용 (총점수 컬럼)
    total_score_col_idx = df.columns.get_loc("총점수")
    total_score_col_letter = xl_col(total_score_col_idx)
    total_score_range = (
        f"{total_score_col_letter}{start_row + 1}:{total_score_col_letter}{end_row + 1}"
    )

    worksheet.conditional_format(
        total_score_range,
        {
            "type": "3_color_scale",
            "min_type": "min",
            "mid_type": "percentile",
            "mid_value": 50,
            "max_type": "max",
            "min_color": "#FF0000",
            "mid_color": "#FFFF00",
            "max_color": "#00FF00",
        },
    )


def generate_prompt(df_news: pd.DataFrame) -> str:

    news_summary = []
    if {"기업명", "감정지수", "뉴스 요약"}.issubset(df_news.columns):
        grouped = df_news.groupby("기업명")
        for comp, group in grouped:
            avg_sent = group["감정지수"].mean()
            recent_summaries = (
                group.sort_values(by="발행일", ascending=False)["뉴스 요약"]
                .head(3)
                .tolist()
            )
            summaries_text = " / ".join([s for s in recent_summaries if s])
            news_summary.append(
                f"{comp}: 평균 감정지수 {avg_sent:.2f}, 최근 뉴스 요약: {summaries_text}"
            )

    prompt = f"""
당신은 기업 분석과 거시경제 분석에 능숙한 전문 주식 분석가입니다.
항상 한국어로 응답해 주세요.

다음은 {date_kr_ymd} 기준으로 수집된 {limit}개 기업의 뉴스 요약과 감정 분석 지수입니다.  
---

📌 뉴스 요약 및 감정 지수:
{chr(10).join(news_summary)}

---

### 분석 요청:

1. {date_kr_ymd} 기준 이번 주 주목할 만한 기업 뉴스 (3~5개)  
- 반드시 **위 뉴스 요약에서 언급된 기업 및 내용만 사용**해 주세요.  
- 기업명과 핵심 뉴스, 그리고 **투자 관점에서의 의미**를 간결히 요약해 주세요.  

**예시 형식:**  
- 엔비디아: 2분기 실적 예상 상회. 반도체 업황 회복 기대감 반영.

2. {date_kr_ymd} 기준 거시경제 환경 요약  
- 관세, 금리, 인플레이션, 고용, 소비, 원-달러 환율 등 주요 지표를 기반으로 간결히 정리해 주세요.  
- 숫자나 방향성 위주로 작성해 주세요.
- 반드시 검색 정보를 바탕으로 작성해 주세요.

3. 미국 증시에 대한 영향 분석  
- 위 거시경제 환경이 **미국 증시에 미치는 영향**을 간결히 설명해 주세요.  
- 금리 방향성, 기술주/가치주 선호, 투자자 심리 변화 등을 중심으로 요약해 주세요.
- 반드시 검색 정보를 바탕으로 작성해 주세요.
"""

    return prompt.strip()


##########################################################################################################


def main(df_news):
    prompt = generate_prompt(df_news)
    print("Prompt sent to Gemini:\n", prompt)

    answer = query_gemini(prompt)
    return answer


answer = main(news_df)

#########################################################################################################

msg = EmailMessage()
msg["Subject"] = f"DeepFund Weekly Insights | {date_kr}"
msg["From"] = Address(display_name="Hyungsuk Choi", addr_spec=EMAIL)
msg["To"] = ""  # or '' or a single address to satisfy the 'To' header requirement

content = (
    f"귀하의 중장기 투자 참고를 위해 {date_kr} 기준, "
    f"시가총액 상위 {limit}개 상장기업에 대한 최신 퀀트 분석 자료를 전달드립니다. "
    "각 기업의 총점수는 밸류에이션 점수, 실적모멘텀 점수, 그리고 가격/수급 점수를 반영하였습니다.\n\n"
    "본 자료는 워런 버핏의 투자 철학을 기반으로, "
    "기업의 재무 건전성 및 실적을 수치화하여 평가한 결과입니다. "
    "투자 판단 시에는 정성적 요소에 대한 별도의 면밀한 검토도 "
    "함께 병행하시기를 권장드립니다.\n\n"
    "📌주요 재무지표 해설\n"
    "D/E 부채비율 (Debt to Equity): 자본 대비 부채의 비율로, 재무 건전성을 나타냅니다. 낮을수록 안정적입니다.\n"
    "CR 유동비율 (Current Ratio): 유동자산이 유동부채를 얼마나 커버할 수 있는지를 보여줍니다.\n"
    "PBR 주가순자산비율 (Price to Book Ratio): 주가가 장부가치 대비 얼마나 높은지를 나타내며, 1보다 낮으면 저평가로 해석되기도 합니다.\n"
    "PER 주가수익비율 (Price to Earnings Ratio): 이익 대비 주가 수준을 나타냅니다. 낮을수록 이익 대비 저렴한 기업입니다. 높을수록 시장의 기대치가 높습니다.\n"
    "ROE 자기자본이익률 (Return on Equity): 자본을(부채 미포함) 얼마나 효율적으로 운용해 이익을 냈는지를 나타냅니다.\n"
    "ROA 총자산이익률 (Return on Assets): 총자산(부채 포함) 대비 수익률로, 보수적인 수익성 지표입니다.\n"
    "ICR 이자보상비율 (Interest Coverage Ratio): 영업이익으로 이자비용을 얼마나 감당할 수 있는지 나타냅니다.\n"
    "EPS 주당순이익 (Earnings Per Share): 최근 5년간 1주당 기업이 창출한 순이익의 성장률로, 수익성과 성장성 판단에 유용합니다.\n"
    "배당성장률: 최근 10년간 배당금의 성장률을 나타내는 지표입니다.\n"
    "영업이익률: 최근 5개 영업년도/분기의 평균 영업이익률 성장률로, 기업의 수익성 수준을 보여줍니다.\n"
    "모멘텀: 주가의 중장기 상승 흐름을 반영한 지표로, 주가의 탄력과 추세를 평가합니다.\n\n"
    "해당 메일은 매주 평일 오후 5시에 자동 발송되며, 안정적이고 현명한 투자를 위한 참고 자료로 제공됩니다.\n\n"
    "귀하의 성공적인 투자를 응원합니다."
)

msg.set_content(content)
html_content = f"""
<html>
  <body>

    <p><strong>지금 무료 구독하고 AI 투자 인사이트를 매주 받아보세요:</strong> <a href="https://portfolio-production-54cf.up.railway.app/" target="_blank">구독하러 가기</a></p>
    
    <p>귀하의 중장기 투자 참고를 위해 <b>{date_kr}</b> 기준, 
    시가총액 상위 <b>{limit}</b>개, 뉴욕증권거래소(NYSE), 나스닥(NASDAQ), 아멕스(AMEX)에 상장된 기업들의 최신 퀀트 데이터를 전달드립니다.</p>

    <p>각 기업의 총점수는 밸류에이션 점수, 실적모멘텀 점수, 가격/수급 점수, 그리고 경쟁 우위의 지속 가능성을 반영하였습니다. 자세한 내용은 아래 해설을 참고해주시기 바랍니다.</p>

    <h3 style="margin-top: 30px;"><strong>{date_kr} AI 선정 주요 뉴스 및 거시경제 분석</strong></h3>

    {markdown.markdown(answer)}

    <h3>📌 주요 재무지표 해설</h3>
    <table border="1" cellpadding="8" cellspacing="0" style="border-collapse: collapse; font-family: sans-serif;">
      <thead style="background-color: #f2f2f2;">
        <tr>
          <th>지표</th>
          <th>한글명</th>
          <th>설명</th>
        </tr>
      </thead>
      <tbody>
        <tr><td><b>FCF</b></td><td>자유현금흐름</td><td>기업이 영업활동을 통해 벌어들인 현금에서 설비 투자 등 사업 유지를 위해 지출한 자금을 제외한 후, 실제로 기업이 자유롭게 사용할 수 있는 현금입니다. 이 현금은 신규 투자 등 다양한 용도로 활용될 수 있습니다.</td></tr>
        <tr><td><b>추정DCF범위</b></td><td>할인된 현금흐름</td><td>미래 예상 자유현금흐름(FCF)을 보수적인 할인율로 현재 가치로 환산하여 산출한 기업의 내재가치입니다. 본 내재가치는 몬테카를로 시뮬레이션을 통해 여러 성장 시나리오를 고려하며, 95% 신뢰구간 범위 내에서 내재가치 변동성을 평가하여 기업의 저평가 여부를 보다 정밀하게 판단합니다.</td></tr>
        <tr><td><b>D/E</b></td><td>부채비율</td><td>자본 대비 부채의 비율로, 재무 건전성을 나타냅니다. 낮을수록 안정적입니다.</td></tr>
        <tr><td><b>CR</b></td><td>유동비율</td><td>유동자산이 유동부채를 얼마나 커버할 수 있는지를 보여줍니다.</td></tr>
        <tr><td><b>PBR</b></td><td>주가순자산비율</td><td>주가가 장부가치 대비 얼마나 높은지를 나타내며, 1보다 낮으면 저평가로 해석되기도 합니다.</td></tr>
        <tr><td><b>PER</b></td><td>주가수익비율</td><td>이익 대비 주가 수준을 나타냅니다. 낮을수록 이익 대비 저렴한 기업입니다. 높을수록 시장의 기대치가 높습니다.</td></tr>
        <tr><td><b>ROE</b></td><td>자기자본이익률</td><td>자본을(부채 미포함) 얼마나 효율적으로 운용해 이익을 냈는지를 나타냅니다.</td></tr>
        <tr><td><b>ROA</b></td><td>총자산이익률</td><td>총자산(부채 포함) 대비 수익률로, 보수적인 수익성 지표입니다.</td></tr>
        <tr><td><b>ICR</b></td><td>이자보상비율</td><td>영업이익으로 이자비용을 얼마나 감당할 수 있는지 나타냅니다.</td></tr>
        <tr><td><b>FCF수익률</b></td><td>-</td><td>자유현금흐름(FCF)을 시가총액으로 나눈 비율로, 이 비율이 높을수록 기업이 창출하는 현금 대비 주가가 저평가되었음을 의미합니다.</td></tr>
        <tr><td><b>FCF성장률</b></td><td>-</td><td>최근 5년간 자유현금흐름의 성장률을 나타내는 지표입니다.</td></tr>
        <tr><td><b>EPS</b></td><td>주당순이익</td><td>최근 5년간 1주당 기업이 창출한 순이익의 성장률로, 수익성과 성장성 판단에 유용합니다.</td></tr>
        <tr><td><b>배당성장률</b></td><td>-</td><td>최근 10년간 배당금의 성장률을 나타내는 지표입니다.</td></tr>
        <tr><td><b>영업이익률</b></td><td>-</td><td>최근 4개 영업년도/분기의 평균 영업이익률 성장률로, 기업의 수익성 수준을 보여줍니다.</td></tr>
        <tr><td><b>모멘텀</b></td><td>-</td><td>주가의 중장기 상승 흐름을 반영한 지표로, 주가의 탄력과 추세를 평가합니다.</td></tr>
        <tr><td><b>ESG</b></td><td>-</td><td>기업의 지속가능성을 나타내는 지표로, 동종업계 대비 수준과 함께 평가합니다.</td></tr>
        <tr><td><b>CVaR</b></td><td>조건부 위험가치</td><td>포트폴리오가 극단적인 손실을 겪을 경우, 손실이 발생하는 최악 5% 구간 내에서 평균적으로 얼마나 손실이 발생하는지를 나타내는 지표입니다.</td></tr>
        <tr><td><b>Sortino Ratio</b></td><td>소티노 지수</td><td>수익률 대비 하방 위험(손실 변동성)을 고려한 위험 조정 수익률을 나타냅니다. 값이 높을수록 하방 위험 대비 수익률이 우수함을 의미합니다.</td></tr>
        <tr><td><b>Variance</b></td><td>분산</td><td>포트폴리오 수익률의 변동성을 나타내는 지표로, 위험 수준 평가에 사용됩니다. 값이 낮을수록 안정적인 포트폴리오임을 뜻합니다.</td></tr>
        <tr><td><b>Sharpe Ratio</b></td><td>샤프 지수</td><td>포트폴리오의 초과 수익률을 표준편차로 나눈 지표로, 위험 대비 수익률을 평가합니다. 값이 클수록 효율적인 투자임을 나타냅니다.</td></tr>
        <tr><td><b>Sentiment Score</b></td><td>감성 점수</td><td>텍스트의 긍정 또는 부정 정도를 수치화한 지표로, 투자 심리나 뉴스 반응을 정량적으로 평가합니다. 값이 높을수록 긍정적인 정서임을 나타냅니다.</td></tr>
      </tbody>
    </table>

    <p style="margin-top: 20px; font-size: 14px; color: #444;">
    본 자료는 <strong>워런 버핏의 '가치투자'</strong> 철학을 기반으로,<br>
    기업의 재무 건전성과 주가의 추세를 수치화하여 평가한 결과입니다.<br>
    본 자료는 정보 제공 목적으로만 사용되며, 투자 손실에 대한 법적 책임은 지지 않습니다.
    </p>

    <p><em>해당 메일은 매주 화, 토 오전 8시에 자동 발송됩니다.</em></p>
  </body>
</html>
"""

msg.add_alternative(html_content, subtype="html")

with open(excel_path, "rb") as f:
    msg.add_attachment(
        f.read(),
        maintype="application",
        subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename="DeepFund_Weekly_Insights.xlsx",
    )

with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
    smtp.login(EMAIL, PASSWORD)
    smtp.send_message(
        msg, to_addrs=recipients
    )  # send_message's to_addrs param controls actual recipients

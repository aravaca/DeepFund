import os
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
import requests
import io
from yahooquery import Ticker
CACHE_FILE = "backend/yf_cache_multi.csv"
# api_key = os.environ["FMP_API_KEY"]
api_key = ""


limit = 230 
NYSE = 165
NASDAQ = 65

NASDAQ_LISTED_URL = "https://www.nasdaqtrader.com/dynamic/symdir/nasdaqlisted.txt"
OTHER_LISTED_URL = "https://www.nasdaqtrader.com/dynamic/symdir/otherlisted.txt"

def _read_symbol_file(url: str) -> pd.DataFrame:
    txt = requests.get(url, timeout=20).text
    # drop footer lines like "File Creation Time: ..."
    lines = [ln for ln in txt.splitlines() if not ln.startswith("File Creation")]
    return pd.read_csv(io.StringIO("\n".join(lines)), sep="|")

def load_nyse_symbols() -> list[str]:
    df = _read_symbol_file(OTHER_LISTED_URL)
    # 'N' = NYSE, exclude ETFs/test issues
    nyse = df[(df["Exchange"] == "N") & (df["ETF"] == "N") & (df["Test Issue"] == "N")]
    return sorted(nyse["ACT Symbol"].dropna().astype(str).unique().tolist())

def load_nasdaq_symbols() -> list[str]:
    df = _read_symbol_file(NASDAQ_LISTED_URL)
    # exclude ETFs/test issues; NASDAQ file has flags
    # Some files have "ETF" column; if missing, default False
    if "ETF" in df.columns:
        nas = df[(df["ETF"] == "N") & (df["Test Issue"] == "N")]
    else:
        nas = df[df["Test Issue"] == "N"]
    return sorted(nas["Symbol"].dropna().astype(str).unique().tolist())

def _chunks(lst, size=400):
    for i in range(0, len(lst), size):
        yield lst[i:i+size]

def fetch_market_caps(tickers: list[str]) -> pd.DataFrame:
    rows = []
    for chunk in _chunks(tickers, 400):
        yq = Ticker(chunk, asynchronous=True)
        price = yq.price  # dict: {symbol: {...}}
        for sym, info in (price or {}).items():
            if not isinstance(info, dict):
                continue
            mc = info.get("marketCap")
            if isinstance(mc, (int, float)) and mc > 0:
                rows.append((sym, mc))
    return pd.DataFrame(rows, columns=["Ticker", "MarketCap"])

def top_by_marketcap(symbols: list[str], n: int | None = None) -> pd.DataFrame:
    df = fetch_market_caps(symbols)
    df = df.sort_values("MarketCap", ascending=False).reset_index(drop=True)
    return df if n is None else df.head(n).copy()

def get_ordered_lists(n_nyse: int = 500, n_nasdaq: int = 500):
    nyse_syms = load_nyse_symbols()
    nasdaq_syms = load_nasdaq_symbols()

    nyse_top = top_by_marketcap(nyse_syms, n_nyse) # DataFrame: Ticker, MarketCap
    nasdaq_top = top_by_marketcap(nasdaq_syms, n_nasdaq) # DataFrame: Ticker, MarketCap

    # If you want one combined ranking across both:
    combined = pd.concat([
        nyse_top.assign(Exchange="NYSE"),
        nasdaq_top.assign(Exchange="NASDAQ")
    ], ignore_index=True).sort_values("MarketCap", ascending=False).reset_index(drop=True)

    # Also return as Python lists if you only need tickers
    nyse_list = nyse_top["Ticker"].tolist()
    nasdaq_list = nasdaq_top["Ticker"].tolist()
    return nyse_list, nasdaq_list, nyse_top, nasdaq_top, combined

# Example:
def get_tickers_by_country_cache(country: str, limit: int, apikey: str):

    nyse_500, nasdaq_500, nyse_df, nasdaq_df, combined_df = get_ordered_lists(NYSE, NASDAQ)

    return nyse_500+nasdaq_500

def get_business_days(start_date, end_date):
    return pd.bdate_range(start=start_date, end=end_date)


def download_yf_data(tickers, start_date, end_date, interval="1d"):
    df = yf.download(
        tickers,
        start=start_date,
        end=end_date,
        interval=interval,
        auto_adjust=True,
        progress=False,
        group_by="ticker",
    )
    # If only one ticker, add a top-level column for consistency
    if isinstance(tickers, str) or len(tickers) == 1:
        df = pd.concat(
            {tickers[0] if isinstance(tickers, list) else tickers: df}, axis=1
        )
    df.index = pd.to_datetime(df.index)
    return df


def update_cache(tickers, cache_file=CACHE_FILE):
    today = datetime.today().date()
    one_year_ago = today - timedelta(days=365)
    business_days = get_business_days(one_year_ago, today)

    if os.path.exists(cache_file):
        cache = pd.read_csv(cache_file, header=[0, 1], index_col=0, parse_dates=True)
    else:
        cache = pd.DataFrame()

    # 누락된 티커와 날짜별로 저장할 set
    missing_tickers = set()
    missing_dates = set()

    for ticker in tickers:
        if cache.empty or (ticker, "Close") not in cache.columns:
            # 티커 자체가 없으면 전 기간 다운로드
            missing_tickers.add(ticker)
            missing_dates.update(business_days)
            continue
        # 날짜별로 결측 체크
        for d in business_days:
            if d not in cache.index or pd.isna(cache.loc[d, (ticker, "Close")]):
                missing_tickers.add(ticker)
                missing_dates.add(d)

    if missing_tickers:
        start = min(missing_dates).strftime("%Y-%m-%d")
        end = (max(missing_dates) + timedelta(days=1)).strftime("%Y-%m-%d")
        print(
            f"Downloading missing data for {len(missing_tickers)} tickers from {start} to {end}"
        )
        new_data = download_yf_data(list(missing_tickers), start, end)

        # 합치기 전 인덱스, 컬럼 중복 문제 처리
        if cache.empty:
            cache = new_data
        else:
            cache = pd.concat([cache, new_data])
            cache = cache[~cache.index.duplicated(keep="last")]
            cache = cache.sort_index()

        cache.to_csv(cache_file)

    # 모든 영업일로 인덱스 재설정 (결측치는 NaN으로)
    cache = cache.reindex(business_days)

    return cache


def remove_empty_columns(csv_file):
    if not os.path.exists(csv_file):
        print("CSV file does not exist.")
        return

    df = pd.read_csv(csv_file, header=[0, 1], index_col=0, parse_dates=True)

    # Drop columns where all values are NaN
    df.dropna(axis=1, how="all", inplace=True)

    # Save back to CSV
    df.to_csv(csv_file)
    print("Empty columns removed.")


if __name__ == "__main__":
    tickers = get_tickers_by_country_cache("US", limit, api_key)  # Example tickers
    tickers_to_remove = [
        "ANTM",
        "ACH",
        "RY-PT",
        "VZA",
        "AED",
        "AEH",
        "BDXA",
        "AMOV",
        "PXD",
        "ATVI",
        "SQ",
        "CEO",
    ]
    tickers = [t for t in tickers if t not in tickers_to_remove]
    print(len(tickers))
    update_cache(tickers)
    # remove_empty_columns(CACHE_FILE)

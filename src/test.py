 
import io, requests, pandas as pd
from yahooquery import Ticker

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
def get_tickers_by_country(country: str, limit: int, apikey: str):

    nyse_500, nasdaq_500, nyse_df, nasdaq_df, combined_df = get_ordered_lists(200,  100)

    return nyse_500+nasdaq_500
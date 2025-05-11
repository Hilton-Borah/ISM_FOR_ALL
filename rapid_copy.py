import requests
import json
import pandas as pd
import numpy as np
from time import sleep
from typing import List, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import StringIO

# --- CONFIG ---
RAPIDAPI_KEY = "0647ea9793msh984c37d473cc60bp19a511jsn3ed48f52c0a2"
HEADERS = {
    "x-rapidapi-host": "yahoo-finance-real-time1.p.rapidapi.com",
    "x-rapidapi-key": RAPIDAPI_KEY
}
SUMMARY_URL = "https://yahoo-finance-real-time1.p.rapidapi.com/stock/get-summary"
SLEEP_SECONDS = 0.2  # prevent rate limiting

# --- TICKER FETCHING FUNCTIONS ---
def get_sp500_tickers() -> List[str]:
    url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    df = pd.read_html(url)[0]
    tickers = df['Symbol'].str.replace('.', '-', regex=False).tolist()
    pd.DataFrame(tickers, columns=['Ticker']).to_csv('sp500_tickers.csv', index=False)
    return tickers

def get_russell2000_tickers() -> List[str]:
    url = "https://www.ishares.com/us/products/239710/ishares-russell-2000-etf/1467271812596.ajax?fileType=csv&fileName=IWM_holdings&dataType=fund"
    response = requests.get(url)
    response.raise_for_status()
    lines = response.text.splitlines()
    header_index = next(i for i, line in enumerate(lines) if 'Ticker' in line)
    csv_data = "\n".join(lines[header_index:])
    df = pd.read_csv(StringIO(csv_data))
    tickers = df['Ticker'].dropna().astype(str).unique().tolist()
    pd.DataFrame(tickers, columns=["Ticker"]).to_csv("russell2000_tickers.csv", index=False)
    return tickers
def get_nasdaq_us_tickers() -> List[str]:
    url = "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt"
    response = requests.get(url)
    lines = response.text.strip().split("\n")
    csv_data = "\n".join(lines[:-1])  # Remove footer
    df = pd.read_csv(StringIO(csv_data), sep="|")
    
    df = df[(df['Test Issue'] == 'N') & 
            (df['ETF'] == 'N') & 
            (df['NextShares'] == 'N')]
    
    tickers = df['Symbol'].dropna().unique().tolist()
    pd.DataFrame(tickers, columns=["Ticker"]).to_csv("nasdaq_us_tickers.csv", index=False)
    return tickers

# --- DATA PROCESSING FUNCTIONS ---
def fetch_summary(symbol: str) -> Dict:
    params = {"lang": "en-US", "symbol": symbol, "region": "US"}
    try:
        resp = requests.get(SUMMARY_URL, headers=HEADERS, params=params)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"Error fetching {symbol}: {e}")
        return {"error": str(e)}

def parse_summary(symbol: str, data: Dict) -> Dict:
    try:
        row = {
            'Ticker': symbol,
            'Last Price': data['price']['regularMarketPrice'],
            'Market Cap (B)': data['summaryDetail']['marketCap'],
            'Shares Outstanding (B)': data['defaultKeyStatistics'].get('sharesOutstanding'),
            'Float (B)': data['defaultKeyStatistics'].get('floatShares'),
            'Sector': data['summaryProfile'].get('sector'),
            'Industry': data['summaryProfile'].get('industry'),
            '50D MA': data['summaryDetail'].get('fiftyDayAverage'),
            '200D MA': data['summaryDetail'].get('twoHundredDayAverage'),
            'Avg Volume 10D (M)': data['summaryDetail'].get('averageVolume10days'),
            'Avg Volume 3M (M)': data['price'].get('averageDailyVolume3Month'),
            'Shares Short (M)': data['defaultKeyStatistics'].get('sharesShort'),
            'Earning Dates': data['calendarEvents']['earnings'].get('earningsDate'),
            'Short Ratio': data['defaultKeyStatistics'].get('shortRatio'),
            'Short % Float': data['defaultKeyStatistics'].get('shortPercentOfFloat'),
            'Price Targets Low': data['financialData'].get('targetLowPrice'),
            'Price Targets Avg': data['financialData'].get('targetMeanPrice'),
            'Price Targets High': data['financialData'].get('targetHighPrice'),
            'Last Updated': data['price'].get('regularMarketTime')
        }

        trends = data.get('recommendationTrend', {}).get('trend', [])
        trend_0m = next((t for t in trends if t.get('period') == '0m'), {})
        row.update({
            'strong_buy': trend_0m.get('strongBuy'),
            'buy': trend_0m.get('buy'),
            'hold': trend_0m.get('hold'),
            'sell': trend_0m.get('sell'),
            'strong_sell': trend_0m.get('strongSell'),
        })
        return row
    except KeyError as ke:
        print(f"Missing key in {symbol}: {ke}")
        return {}

def process_tickers(tickers: List[str], output_file: str, index_name: str) -> None:
    all_data = []
    error_symbols = []

    def fetch_and_parse(symbol):
        summary = fetch_summary(symbol)
        if 'error' in summary:
            return (symbol, None)
        return (symbol, parse_summary(symbol, summary))

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(fetch_and_parse, symbol): symbol for symbol in tickers}

        for i, future in enumerate(as_completed(futures)):
            symbol = futures[future]
            try:
                symbol, parsed = future.result()
                print(f"[{index_name}] [{i+1}/{len(tickers)}] Fetched {symbol}")
                if parsed:
                    all_data.append(parsed)
                else:
                    error_symbols.append(symbol)
            except Exception as e:
                print(f"Error with {symbol}: {e}")
                error_symbols.append(symbol)
            sleep(SLEEP_SECONDS)

    if all_data:
        df_summary = pd.DataFrame(all_data)
        
        # Calculate derived columns
        df_summary['Days to cover'] = np.where(
            df_summary['Avg Volume 10D (M)'] != 0,
            df_summary['Shares Short (M)'] / df_summary['Avg Volume 10D (M)'],
            np.nan
        ).round(2)
        
        df_summary['Number of recommendations'] = df_summary[['sell', 'buy', 'hold', 'strong_sell', 'strong_buy']].sum(axis=1)

        def format_percent(series):
            return series.round(2).astype(str) + '%'

        # Price vs Moving Averages
        for days in [50, 200]:
            ma_col = f'{days}D MA'
            pct_col = f'% Price vs {days} Days Moving average'
            df_summary[pct_col] = np.where(
                df_summary[ma_col] != 0,
                ((df_summary['Last Price'] - df_summary[ma_col]) / df_summary[ma_col]) * 100,
                np.nan
            )
            df_summary[pct_col] = format_percent(df_summary[pct_col])

        # Recommendation percentages
        for rec in ['sell', 'strong_sell', 'hold', 'buy', 'strong_buy']:
            pct_col = f'% of {rec}'
            df_summary[pct_col] = format_percent(
                (df_summary[rec] / df_summary['Number of recommendations']) * 100
            )

        # Price target percentages
        for target, col in [('Low', 'Low Below current Abs %'), ('High', 'High above current Abs %')]:
            df_summary[col] = np.where(
                df_summary['Last Price'] != 0,
                ((df_summary[f'Price Targets {target}'] - df_summary['Last Price']) / df_summary['Last Price']) * 100,
                np.nan
            )
            df_summary[col] = format_percent(df_summary[col])

        df_summary.to_csv(output_file, index=False)
        print(f"✅ Saved {output_file}")

    if error_symbols:
        error_file = f"failed_symbols_{index_name.lower()}.csv"
        pd.DataFrame(error_symbols, columns=["Failed Tickers"]).to_csv(error_file, index=False)
        print(f"⚠️ Errors with {len(error_symbols)} symbols. See {error_file}")

# --- MAIN EXECUTION ---
if __name__ == "__main__":
    # Process S&P 500 (first 10 for testing)
    sp500_tickers = get_sp500_tickers()[:10]
    process_tickers(sp500_tickers, "Summary_output_SP500.csv", "SP500")

    # Process Russell 2000 (first 10 for testing)
    russell2000_tickers = get_russell2000_tickers()[:10]
    process_tickers(russell2000_tickers, "Summary_output_Russell2000.csv", "Russell2000")

    # Process NASDAQ US (first 10 for testing)
    nasdaq_us_tickers = get_nasdaq_us_tickers()[:10]
    process_tickers(nasdaq_us_tickers, "Summary_output_NASDAQ.csv", "NASDAQ")
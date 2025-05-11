import requests
import json
import pandas as pd
import numpy as np
from time import sleep
from typing import List, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed


# --- CONFIG ---
RAPIDAPI_KEY = "0647ea9793msh984c37d473cc60bp19a511jsn3ed48f52c0a2"
HEADERS = {
    "x-rapidapi-host": "yahoo-finance-real-time1.p.rapidapi.com",
    "x-rapidapi-key": RAPIDAPI_KEY
}
SUMMARY_URL = "https://yahoo-finance-real-time1.p.rapidapi.com/stock/get-summary"
SLEEP_SECONDS = 0.2  # prevent rate limiting

# --- STEP 1: Fetch S&P 500 Tickers ---
def get_sp500_tickers() -> List[str]:
    url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    df = pd.read_html(url)[0]
    tickers = df['Symbol'].str.replace('.', '-', regex=False).tolist()
    pd.DataFrame(tickers, columns=['Ticker']).to_csv('sp500_tickers.csv', index=False)
    return tickers

# --- STEP 2: Fetch Stock Summaries ---
def fetch_summary(symbol: str) -> Dict:
    params = {"lang": "en-US", "symbol": symbol, "region": "US"}
    try:
        resp = requests.get(SUMMARY_URL, headers=HEADERS, params=params)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"Error fetching {symbol}: {e}")
        return {"error": str(e)}

# --- STEP 3: Parse Summary Data ---
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

        # Recommendation trend for '0m'
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

# --- STEP 4: Main Execution ---
def main():
    tickers = get_sp500_tickers()[:10]  # Limit to 20 for testing
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
                print(f"[{i+1}/{len(tickers)}] Fetched {symbol}")
                if parsed:
                    all_data.append(parsed)
                else:
                    error_symbols.append(symbol)
            except Exception as e:
                print(f"Error with {symbol}: {e}")
                error_symbols.append(symbol)

    df_summary = pd.DataFrame(all_data)

    df_summary['Days to cover'] = np.where(
        df_summary['Avg Volume 10D (M)'] != 0,
        df_summary['Shares Short (M)'] / df_summary['Avg Volume 10D (M)'],
        np.nan
    ).round(2)
    df_summary['Number of recommendations'] = df_summary[['sell', 'buy', 'hold', 'strong_sell', 'strong_buy']].sum(axis=1)


    # Helper function to format percentage with % symbol
    def format_percent(series):
        return series.round(2).astype(str) + '%'

    # Price vs Moving Averages
    df_summary['% Price vs 50 Days Moving average'] = np.where(
        df_summary['50D MA'] != 0,
        ((df_summary['Last Price'] - df_summary['50D MA']) / df_summary['50D MA']) * 100,
        np.nan
    )
    df_summary['% Price vs 50 Days Moving average'] = format_percent(df_summary['% Price vs 50 Days Moving average'])

    df_summary['% Price vs 200 Days Moving average'] = np.where(
        df_summary['200D MA'] != 0,
        ((df_summary['Last Price'] - df_summary['200D MA']) / df_summary['200D MA']) * 100,
        np.nan
    )
    df_summary['% Price vs 200 Days Moving average'] = format_percent(df_summary['% Price vs 200 Days Moving average'])

    
    df_summary['% of sell'] = format_percent(np.where(
        df_summary['Number of recommendations'] != 0,
        (df_summary['sell'] / df_summary['Number of recommendations']) * 100,
        np.nan
    ))

    df_summary['% of underperform'] = format_percent(np.where(
        df_summary['Number of recommendations'] != 0,
        (df_summary['strong_sell'] / df_summary['Number of recommendations']) * 100,
        np.nan
    ))

    df_summary['% of hold'] = format_percent(np.where(
        df_summary['Number of recommendations'] != 0,
        (df_summary['hold'] / df_summary['Number of recommendations']) * 100,
        np.nan
    ))

    df_summary['% of buy'] = format_percent(np.where(
        df_summary['Number of recommendations'] != 0,
        (df_summary['buy'] / df_summary['Number of recommendations']) * 100,
        np.nan
    ))

    df_summary['% of strong buy'] = format_percent(np.where(
        df_summary['Number of recommendations'] != 0,
        (df_summary['strong_buy'] / df_summary['Number of recommendations']) * 100,
        np.nan
    ))

    # Price Target Differences
    df_summary['Low Below current Abs %'] = np.where(
        df_summary['Price Targets Low'] != 0,
        ((df_summary['Price Targets Low'] - df_summary['Last Price']) / df_summary['Last Price']) * 100,
        np.nan
    )
    df_summary['Low Below current Abs %'] = format_percent(df_summary['Low Below current Abs %'])

    df_summary['High above current Abs %'] = np.where(
        df_summary['Last Price'] != 0,
        ((df_summary['Price Targets High'] - df_summary['Last Price']) / df_summary['Last Price']) * 100,
        np.nan
    )
    df_summary['High above current Abs %'] = format_percent(df_summary['High above current Abs %'])
    df_summary.to_csv("Summary_output_SP500.csv", index=False)
    print("✅ Saved Summary_output_SP500.csv")

    if error_symbols:
        pd.DataFrame(error_symbols, columns=["Failed Tickers"]).to_csv("failed_symbols.csv", index=False)
        print(f"⚠️ Errors with {len(error_symbols)} symbols. See failed_symbols.csv")

if __name__ == "__main__":
    main()

import yfinance as yf
import pandas as pd
import time
from datetime import datetime

def format_market_cap(value):
    """Format market cap with appropriate suffix (B, M, T)"""
    if pd.isna(value) or value is None:
        return None
    
    abs_value = abs(value)
    if abs_value >= 1e12:  # Trillions
        return f"{value/1e12:,.2f}T"
    elif abs_value >= 1e9:  # Billions
        return f"{value/1e9:,.2f}B"
    elif abs_value >= 1e6:  # Millions
        return f"{value/1e6:,.2f}M"
    else:
        return f"{value:,.2f}"

def get_next_earnings_date(stock):
    """Extract next earnings date using get_earnings_dates()"""
    try:
        earnings_dates = stock.get_earnings_dates()
        if earnings_dates is None or earnings_dates.empty:
            return None

        # Convert to naive datetime objects
        earnings_dates.index = earnings_dates.index.tz_localize(None)
        
        # Get current datetime
        now = datetime.now()
        
        # Find future dates
        future_dates = earnings_dates.index[earnings_dates.index > now]
        
        if not future_dates.empty:
            return min(future_dates).strftime('%Y-%m-%d')
        return None
        
    except Exception as e:
        print(f"Earnings date error for {stock.ticker}: {str(e)}")
        return None

def get_analyst_recommendations(ticker):
    """Get analyst recommendations using yfinance's built-in methods"""
    try:
        stock = yf.Ticker(ticker)
        
        # Try getting full recommendations history
        rec_df = stock.get_recommendations()
        if rec_df is not None and not rec_df.empty:
            latest = rec_df.iloc[0]
            return {
                'Strong Buy': latest.get('strongBuy', 0),
                'Buy': latest.get('buy', 0),
                'Hold': latest.get('hold', 0),
                'Sell': latest.get('sell', 0),
                'Strong Sell': latest.get('strongSell', 0),
                'total': latest[['strongBuy', 'buy', 'hold', 'sell', 'strongSell']].sum()
            }
        
        # Fallback to summary recommendations
        info = stock.info
        return {
            'Strong Buy': info.get('strongBuy', 0),
            'Buy': info.get('buy', 0),
            'Hold': info.get('hold', 0),
            'Sell': info.get('sell', 0),
            'Strong Sell': info.get('strongSell', 0),
            'total': sum([
                info.get('strongBuy', 0),
                info.get('buy', 0),
                info.get('hold', 0),
                info.get('sell', 0),
                info.get('strongSell', 0)
            ])
        }
        
    except Exception as e:
        print(f"Recommendation error for {ticker}: {str(e)}")
        return None

def get_analyst_price_targets(ticker):
    """Get analyst price targets in horizontal format"""
    try:
        stock = yf.Ticker(ticker)
        
        # Get current price first
        try:
            current_price = stock.history(period="1d")['Close'].iloc[-1]
        except:
            current_price = stock.info.get('currentPrice', None)
        
        # Try to get analyst targets
        try:
            targets_df = stock.get_analyst_price_target()
            if not targets_df.empty:
                latest = targets_df.iloc[0]
                return {
                    'Current': current_price,
                    'Low': latest.get('Low', None),
                    'Average': latest.get('Mean', None),
                    'High': latest.get('High', None)
                }
        except AttributeError:
            pass
        
        # Fallback to info dictionary
        info = stock.info
        return {
            'Current': current_price,
            'Low': info.get('targetLowPrice'),
            'Average': info.get('targetMeanPrice'),
            'High': info.get('targetHighPrice')
        }
        
    except Exception as e:
        print(f"Price target error for {ticker}: {str(e)}")
        return {
            'Current': current_price if 'current_price' in locals() else None,
            'Low': None,
            'Average': None,
            'High': None
        }

def format_currency(value, decimals=2):
    """Format value as currency with commas"""
    if pd.isna(value) or value is None:
        return None
    return f"${value:,.{decimals}f}"

def format_percentage(value, decimals=2):
    """Format value as percentage with % symbol"""
    if pd.isna(value) or value is None:
        return None
    return f"{value:,.{decimals}f}%"

def get_yahoo_finance_data(ticker):
    """Get comprehensive financial data with robust error handling"""
    try:
        stock = yf.Ticker(ticker)
        info = stock.info
        history = stock.history(period="200d")
        
        if history.empty:
            print(f"No historical data for {ticker}")
            return None

        # Price data with multiple fallbacks
        current_price = info.get('currentPrice') or \
                       info.get('regularMarketPrice') or \
                       history['Close'].iloc[-1]

        # Get analyst recommendations
        recommendations = get_analyst_recommendations(ticker) or {
            'Strong Buy': 0,
            'Buy': 0,
            'Hold': 0,
            'Sell': 0,
            'Strong Sell': 0,
            'total': 0
        }

        # Calculate recommendation percentages
        total = recommendations['total']
        if total > 0:
            strong_buy_pct = (recommendations['Strong Buy'] / total) * 100
            buy_pct = (recommendations['Buy'] / total) * 100
            hold_pct = (recommendations['Hold'] / total) * 100
            sell_pct = (recommendations['Sell'] / total) * 100
            strong_sell_pct = (recommendations['Strong Sell'] / total) * 100
            # Calculate net recommendation percentage
            recommendation_pct = (strong_buy_pct + buy_pct) - (strong_sell_pct + sell_pct)
        else:
            strong_buy_pct = buy_pct = hold_pct = sell_pct = strong_sell_pct = 0
            recommendation_pct = 0

        # Get next earnings date
        next_earnings = get_next_earnings_date(stock)

        # Technical indicators calculations
        ma50 = history['Close'].rolling(50).mean().iloc[-1] if len(history) >= 50 else None
        ma200 = history['Close'].rolling(200).mean().iloc[-1] if len(history) >= 200 else None
        
        # Volume analysis
        avg_volume_10d = history['Volume'].tail(10).mean()
        avg_volume_3m = history['Volume'].tail(63).mean()

        # Short interest calculations
        shares_short = info.get('sharesShort')
        float_shares = info.get('floatShares')
        short_ratio = info.get('shortRatio')
        
        short_pct_float = (shares_short / float_shares * 100) if shares_short and float_shares else None
        days_to_cover = (shares_short / avg_volume_3m) if shares_short and avg_volume_3m else None

        # Get analyst price targets
        price_targets = get_analyst_price_targets(ticker) or {
            'Current': current_price,
            'Low': None,
            'Average': None,
            'High': None
        }

        # Calculate target percentages
        try:
            current = float(price_targets['Current']) if price_targets['Current'] else None
            low = float(price_targets['Low']) if price_targets['Low'] else None
            high = float(price_targets['High']) if price_targets['High'] else None
            
            low_below_pct = ((low - current) / current * 100) if all([low, current]) else None
            high_above_pct = ((high - current) / current * 100) if all([high, current]) else None
        except:
            low_below_pct = None
            high_above_pct = None

        # Prepare final data dictionary
        data = {
            'Ticker': ticker,
            'Last Price': format_currency(current_price),
            'Market Cap': format_market_cap(info.get('marketCap')) if info.get('marketCap') else None,
            'Shares Outstanding': format_market_cap(info.get('sharesOutstanding')) if info.get('sharesOutstanding') else None,
            'Float': format_market_cap(float_shares) if float_shares else None,
            'Sector': info.get('sector'),
            'Industry': info.get('industry'),
            '50D MA': format_currency(ma50) if ma50 else None,
            '% vs 50D MA': format_percentage(((current_price - ma50)/ma50 * 100) if ma50 else None),
            '200D MA': format_currency(ma200) if ma200 else None,
            '% vs 200D MA': format_percentage(((current_price - ma200)/ma200 * 100) if ma200 else None),
            'Avg Volume 10D': format_market_cap(avg_volume_10d),
            'Avg Volume 3M': format_market_cap(avg_volume_3m),
            'Shares Short': format_market_cap(shares_short) if shares_short else None,
            'Short Ratio': short_ratio,
            'Short % Float': format_percentage(short_pct_float) if short_pct_float else None,
            'Days to Cover': format_currency(days_to_cover, 1) if days_to_cover else None,
            'Next Earnings Date': next_earnings,
            'Recommendations Total': recommendations['total'],
            'Strong Sell': recommendations['Strong Sell'],
            'Sell': recommendations['Sell'],
            'Hold': recommendations['Hold'],
            'Buy': recommendations['Buy'],
            'Strong Buy': recommendations['Strong Buy'],
            '% of Strong Sell': format_percentage(strong_sell_pct),
            '% of Sell': format_percentage(sell_pct),
            '% Hold': format_percentage(hold_pct),
            '% Buy': format_percentage(buy_pct),
            '% Strong Buy': format_percentage(strong_buy_pct),
            'Recommendation %': format_percentage(recommendation_pct),
            'Current Price': format_currency(price_targets['Current']),
            'Low Target': format_currency(price_targets['Low']),
            'Avg Target': format_currency(price_targets['Average']),
            'High Target': format_currency(price_targets['High']),
            'Low Below Current Abs %': format_percentage(low_below_pct),
            'High Above Current Abs %': format_percentage(high_above_pct),
            'Last Updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        return data
        
    except Exception as e:
        print(f"Processing error for {ticker}: {str(e)}")
        return None

def main():
    # Example tickers
    tickers = ["BFAM", "SPSC", "RELY"]
    
    results = []
    
    for ticker in tickers:
        print(f"Processing {ticker}...")
        try:
            data = get_yahoo_finance_data(ticker)
            if data:
                results.append(data)
            time.sleep(1)  # Respectful delay between requests
        except Exception as e:
            print(f"Critical error with {ticker}: {str(e)}")
        
    if results:
        df = pd.DataFrame(results)
        
        # Define column order
        column_order = [
            'Ticker', 'Last Price', 'Market Cap', 'Shares Outstanding', 'Float',
            'Sector', 'Industry', '50D MA', '% vs 50D MA', '200D MA', '% vs 200D MA',
            'Avg Volume 10D', 'Avg Volume 3M', 'Shares Short', 'Short Ratio',
            'Short % Float', 'Days to Cover', 'Next Earnings Date', 'Recommendations Total',
            'Strong Sell', 'Sell', 'Hold', 'Buy', 'Strong Buy',
            '% of Strong Sell', '% of Sell', '% Hold', '% Buy', '% Strong Buy',
            'Recommendation %', 'Current Price', 'Low Target', 'Avg Target', 'High Target',
            'Low Below Current Abs %', 'High Above Current Abs %', 'Last Updated'
        ]
        
        # Configure pandas display
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', 1000)
        pd.set_option('display.max_colwidth', 20)
        
        print("\nFinal Results:")
        print(df[column_order].to_string(index=False))
        
        # Save to CSV
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"stock_analysis_{timestamp}.csv"
        df.to_csv(filename, index=False)
        print(f"\nData saved to {filename}")
    else:
        print("No data was collected")

if __name__ == "__main__":
    main()
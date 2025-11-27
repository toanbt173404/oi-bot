import ccxt
import pandas as pd
import asyncio
import os
import logging
import json
import argparse
import requests
import time
from datetime import datetime, timedelta
from telegram.ext import Application
from dotenv import load_dotenv
import logging
import concurrent.futures

logger = logging.getLogger(__name__)

load_dotenv()

# Parse command line arguments
def parse_args():
    parser = argparse.ArgumentParser(description='Open Interest Monitoring Bot')
    parser.add_argument('--config', type=str, default='config.json',
                        help='Path to configuration file (default: config.json)')
    return parser.parse_args()

# Load configuration from JSON file
def load_config(config_path=None):
    if config_path is None:
        config_path = os.path.join(os.path.dirname(__file__), 'config.json')
    
    if not os.path.isabs(config_path):
        # If relative path, make it relative to script directory
        config_path = os.path.join(os.path.dirname(__file__), config_path)
    
    try:
        with open(config_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        logger.error(f"Configuration file not found: {config_path}")
        raise
    except json.JSONDecodeError:
        logger.error(f"Invalid JSON in configuration file: {config_path}")
        raise

# Parse arguments and load config
args = parse_args()
config = load_config(args.config)

# Initialize exchange based on config
def get_exchange(exchange_name):
    """Initialize exchange based on config"""
    if exchange_name.lower() == 'bybit':
        return ccxt.bybit({'options': {'defaultType': 'swap'}})
    elif exchange_name.lower() == 'binance':
        return ccxt.binance({'options': {'defaultType': 'swap'}})
    elif exchange_name.lower() == 'gateio':
        return ccxt.gateio({'options': {'defaultType': 'swap'}})
    elif exchange_name.lower() == 'bitget':
        return ccxt.bitget({'options': {'defaultType': 'swap'}})
    else:
        raise ValueError(f"Unsupported exchange: {exchange_name}")

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Extract configuration values
TELEGRAM_BOT_TOKEN = config['telegram']['bot_token']
TELEGRAM_CHAT_ID = config['telegram']['chat_id']
THREAD_ID = config['telegram']['thread_id'] if config['telegram']['thread_id'] is not None else None

# Support both single exchange and multiple exchanges
if 'exchanges' in config:
    EXCHANGE_NAMES = config['exchanges']
elif 'exchange' in config:
    EXCHANGE_NAMES = [config['exchange']]
else:
    raise ValueError("Configuration must contain either 'exchange' or 'exchanges' field")

OI_CHANGE_THRESHOLD = config['oi_alerts']['change_threshold_percent']
OI_CHANGE_THRESHOLD_HIGH = config['oi_alerts']['change_threshold_percent_high']
OI_CHANGE_THRESHOLD_PREVIOUS = config['oi_alerts']['change_threshold_percent_previous']
OI_CHANGE_THRESHOLD_PREVIOUS_HIGH = config['oi_alerts']['change_threshold_percent_previous_high']
PRICE_CHANGE_THRESHOLD = config['oi_alerts']['price_change_threshold_percent']
INTERVAL_MINUTES = config['timing']['interval_minutes']
BATCH_SIZE = config['oi_alerts']['batch_size']
MIN_OI_VALUE = config['oi_alerts']['min_oi_value_usd']
MIN_VOLUME_24H = config['oi_alerts'].get('min_volume_24h_usd', 100000)  # Default 100k USD

# Initialize exchanges
exchanges = {name: get_exchange(name) for name in EXCHANGE_NAMES}

# Store alerts from last 24 hours with detailed tracking and rolling snapshots
sent_alerts_24h = {}  # Format: {symbol_exchange: {'timestamp': datetime, 'oi_change_pct': float, 'high_alert_count': int, 'last_high_alert': datetime, 'last_oi_value': float, 'total_count': int, 'snapshot_5m_rolling': float, 'snapshot_6h_rolling': float, 'first_price': float}}

def get_5min_window(timestamp):
    """Get the 5-minute window for a given timestamp"""
    minute = timestamp.minute
    window_minute = (minute // 5) * 5
    return timestamp.replace(minute=window_minute, second=0, microsecond=0)

async def send_telegram_message(message, topic_id):
    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    try:
        await app.bot.send_message(
            chat_id=TELEGRAM_CHAT_ID,
            message_thread_id=topic_id,
            text=message,
            parse_mode='HTML'
        )
        logger.info("Message sent successfully")
    except Exception as e:
        logger.error(f"Telegram API error: {str(e)}")

def get_all_perps(exchange):
    """Get all perpetual contracts from exchange"""
    print(f"Fetching all perpetual contracts from {exchange.id}...")
    markets = exchange.load_markets()
    perp_markets = [symbol for symbol, market in markets.items() if market['type'] == 'swap' and market['quote'] in ['USDT']]
    logger.info(f"Found {len(perp_markets)} perpetual markets")
    return perp_markets

def get_exchange_symbols():
    """Get all symbols from configured exchanges"""
    logger.info(f"Fetching all symbols from {', '.join(EXCHANGE_NAMES)}...")

    # Create symbol mapping
    symbol_exchange_map = {}

    # Extract base symbol (remove /USDT suffix) for comparison
    def get_base_symbol(symbol):
        return symbol.split('/')[0]

    # Process each exchange
    for exchange_name in EXCHANGE_NAMES:
        exchange_obj = exchanges[exchange_name]

        # Get all symbols from this exchange
        exchange_symbols = get_all_perps(exchange_obj)

        # Add exchange symbols with exchange-specific key
        for symbol in exchange_symbols:
            base_symbol = get_base_symbol(symbol)
            # Use base_symbol + exchange as unique key to support same symbol on multiple exchanges
            key = f"{base_symbol}_{exchange_name.lower()}"
            symbol_exchange_map[key] = {
                'base_symbol': base_symbol,
                'symbol': symbol,
                'exchange': exchange_name.lower()
            }

        logger.info(f"Total {exchange_name} symbols: {len([k for k in symbol_exchange_map if k.endswith(f'_{exchange_name.lower()}')])}")

    logger.info(f"Total symbols across all exchanges: {len(symbol_exchange_map)}")
    return symbol_exchange_map

def fetch_long_short_ratio(exchange, symbol, timeframe='5min', limit=1):
    """Fetch long/short ratio from exchange and calculate top/all ratio for Binance"""
    try:
        # Check if exchange supports fetchLongShortRatioHistory
        if not hasattr(exchange, 'fetchLongShortRatioHistory'):
            logger.debug(f"{exchange.id} does not support fetchLongShortRatioHistory")
            return None
            
        # Convert symbol format if needed for specific exchanges
        if exchange.id == 'binance' and ':' not in symbol:
            symbol = symbol.replace('/USDT', '/USDT:USDT')
        
        logger.debug(f"Fetching long/short ratio for {symbol} on {exchange.id}")
        
        # For Binance, fetch both all accounts ratio and top accounts ratio
        if exchange.id == 'binance':
            try:
                # Fetch all accounts long/short ratio
                all_ratio_history = exchange.fetchLongShortRatioHistory(symbol, "5m", limit=limit)
                
                # Fetch top accounts long/short ratio using custom API call
                # Convert symbol format for API call (remove :USDT suffix)
                api_symbol = symbol.replace('/USDT:USDT', 'USDT').replace('/', '')
                
                # Make direct API call for top trader long/short ratio
                
                url = "https://fapi.binance.com/futures/data/topLongShortPositionRatio"
                params = {
                    'symbol': api_symbol,
                    'period': '5m',
                    'limit': 1
                }
                
                response = requests.get(url, params=params, timeout=10)
                
                if response.status_code == 200:
                    top_data = response.json()
                    
                    if all_ratio_history and len(all_ratio_history) > 0 and top_data and len(top_data) > 0:
                        latest_all = all_ratio_history[-1]
                        latest_top = top_data[-1]
                        
                        # Extract all accounts ratio
                        if 'info' in latest_all and 'longAccount' in latest_all['info']:
                            all_long = float(latest_all['info']['longAccount'])
                            all_short = float(latest_all['info']['shortAccount'])
                            all_long_short_ratio = all_long / all_short if all_short > 0 else 0
                        else:
                            all_long_short_ratio = latest_all.get('longShortRatio', 0)
                        
                        # Extract top accounts ratio
                        top_long = float(latest_top['longAccount'])
                        top_short = float(latest_top['shortAccount'])
                        top_long_short_ratio = top_long / top_short if top_short > 0 else 0
                        
                        # Calculate final ratio: (top l/s ratio) / (all l/s ratio)
                        final_ratio = top_long_short_ratio / all_long_short_ratio if all_long_short_ratio > 0 else 0
                        
                        logger.debug(f"Got Binance ratios for {symbol}: Top L/S={top_long_short_ratio:.3f}, All L/S={all_long_short_ratio:.3f}, Final={final_ratio:.3f}")
                        
                        return {
                            'top_long_ratio': top_long,
                            'top_short_ratio': top_short,
                            'top_long_short_ratio': top_long_short_ratio,
                            'all_long_short_ratio': all_long_short_ratio,
                            'long_short_ratio': final_ratio,  # Final calculated ratio
                            'timestamp': latest_all.get('timestamp', None),
                            'source': exchange.id
                        }
                else:
                    logger.warning(f"Failed to fetch top trader data from Binance API: {response.status_code}")
                    
            except Exception as e:
                logger.warning(f"Failed to fetch Binance top trader data for {symbol}: {e}")
                # Fallback to regular ratio fetching
                pass
        
        # Handle different timeframe formats for different exchanges
        if exchange.id == 'binance':
            ratio_history = exchange.fetchLongShortRatioHistory(symbol,"5m", limit=limit)
        else:
            # For other exchanges, use the timeframe parameter
            ratio_history = exchange.fetchLongShortRatioHistory(symbol, "5min", limit=limit)

        
        if ratio_history and len(ratio_history) > 0:
            latest_ratio = ratio_history[-1]

            # Extract data based on what fields are available
            # Check for longAccount/shortAccount (Binance long/short format)
            if 'info' in latest_ratio and 'longAccount' in latest_ratio['info']:
                long_account = float(latest_ratio['info']['longAccount'])
                short_account = float(latest_ratio['info']['shortAccount'])
                long_short_ratio = latest_ratio.get('longShortRatio', long_account/short_account if short_account > 0 else 0)
                
                logger.debug(f"Got L/S ratio for {symbol}: {long_account}/{short_account} = {long_short_ratio}")
                return {
                    'long_ratio': long_account,
                    'short_ratio': short_account,
                    'long_short_ratio': long_short_ratio,
                    'timestamp': latest_ratio.get('timestamp', None),
                    'source': exchange.id
                }
            
            # Check for buyRatio/sellRatio (Binance buy/sell format)
            elif 'info' in latest_ratio and 'buyRatio' in latest_ratio['info']:
                buy_ratio = float(latest_ratio['info']['buyRatio'])
                sell_ratio = float(latest_ratio['info']['sellRatio'])
                
                logger.debug(f"Got B/S ratio for {symbol}: {buy_ratio}/{sell_ratio}")
                return {
                    'buy_ratio': buy_ratio,
                    'sell_ratio': sell_ratio,
                    'long_short_ratio': latest_ratio.get('longShortRatio', buy_ratio/sell_ratio if sell_ratio > 0 else 0),
                    'timestamp': latest_ratio.get('timestamp', None),
                    'source': exchange.id
                }
            
            # Fallback to long/short ratio fields
            long_ratio = latest_ratio.get('longRatio', latest_ratio.get('long', None))
            short_ratio = latest_ratio.get('shortRatio', latest_ratio.get('short', None))
            
            if long_ratio is not None and short_ratio is not None:
                logger.debug(f"Got L/S ratio for {symbol}: {long_ratio}/{short_ratio}")
                return {
                    'long_ratio': long_ratio,
                    'short_ratio': short_ratio,
                    'long_short_ratio': long_ratio/short_ratio if short_ratio > 0 else 0,
                    'timestamp': latest_ratio.get('timestamp', None),
                    'source': exchange.id
                }
            else:
                logger.debug(f"L/S ratio fields not found in response: {latest_ratio}")
        
    except AttributeError as e:
        logger.debug(f"{exchange.id} does not support fetchLongShortRatioHistory: {e}")
    except Exception as e:
        logger.warning(f"Failed to fetch long/short ratio for {symbol} on {exchange.id}: {e}")
    
    return None

def process_symbol_oi(symbol_info):
    """Process a single symbol for OI data using configured exchange with current vs previous candle detection"""
    base_symbol = symbol_info['base_symbol']
    symbol = symbol_info['symbol']
    exchange_name = symbol_info['exchange']

    # Get the correct exchange object
    exchange = exchanges[exchange_name]

    try:
        # Get OI history for both current vs previous and 6-hour rolling analysis
        oi_history = exchange.fetchOpenInterestHistory(symbol, "5m", limit=73) 
        
        if oi_history and len(oi_history) >= 3:
            current_oi = oi_history[-1]  # Latest data
            previous_oi = oi_history[-2]  # Previous candle
            
            # Check if latest OI data is fresh (within last 10 minutes)
            current_time = datetime.now()
            oi_datetime = datetime.fromtimestamp(current_oi['timestamp'] / 1000)
            time_diff = current_time - oi_datetime

            # Skip if OI data is older than 10 minutes
            if time_diff.total_seconds() > 400:  # 5 minutes = 300 seconds
                logger.info(f"Skipping {symbol} - OI data is {time_diff.total_seconds():.0f}s old")
                return None
            
            # Current and previous OI values
            current_contracts = current_oi['openInterestValue'] if current_oi['openInterestValue'] is not None else current_oi['openInterestAmount']
            previous_contracts = previous_oi['openInterestValue'] if previous_oi['openInterestValue'] is not None else previous_oi['openInterestAmount']
            
            # Calculate current vs previous candle change
            oi_change_pct_previous = ((current_contracts - previous_contracts) / previous_contracts) * 100 if previous_contracts > 0 else 0
            # Calculate 6-hour rolling average if we have enough data
            rolling_avg_oi = None
            oi_change_pct_rolling = 0
            if len(oi_history) >= 73:
                historical_oi_values = [oi['openInterestValue'] if oi['openInterestValue'] is not None else oi['openInterestAmount'] for oi in oi_history[:-1]]  # Last 72 points (6 hours)
                rolling_avg_oi = sum(historical_oi_values) / len(historical_oi_values)
                oi_change_pct_rolling = ((current_contracts - rolling_avg_oi) / rolling_avg_oi) * 100 if rolling_avg_oi > 0 else 0
            
            # Check if OI meets minimum threshold and has significant change (either type)
            if current_contracts > 0:
                significant_previous_change = abs(oi_change_pct_previous) >= OI_CHANGE_THRESHOLD_PREVIOUS
                significant_rolling_change = abs(oi_change_pct_rolling) >= OI_CHANGE_THRESHOLD
                
                if significant_previous_change or significant_rolling_change:
                    # Fetch OHLCV data for price calculation only for eligible symbols
                    ohlcv = exchange.fetch_ohlcv(symbol, "5m")
                    
                    if ohlcv and len(ohlcv) >= 2:
                        # Get actual prices from OHLCV data
                        current_price = ohlcv[-1][4]  # Close price of latest candle
                        previous_price = ohlcv[-2][4]  # Close price of previous candle
                        current_high = ohlcv[-1][2]  # High price of current candle
                        
                        # Calculate signal price using max(previous_close, current_high)
                        signal_price = max(previous_price, current_high)
                        
                        # Calculate 5-minute rolling average price and comparison
                        if len(ohlcv) >= 5:
                            # Calculate 5-candle (25-minute) rolling average for current period
                            recent_prices = [candle[4] for candle in ohlcv[-5:]]  # Last 5 candles
                            rolling_5m_price = sum(recent_prices) / len(recent_prices)
                            
                            # Calculate 5-candle rolling average for previous period (5 candles ago)
                            if len(ohlcv) >= 10:
                                prev_prices = [candle[4] for candle in ohlcv[-10:-5]]  # 5 candles from 10-5 ago
                                prev_rolling_5m_price = sum(prev_prices) / len(prev_prices)
                                rolling_price_change_pct = ((rolling_5m_price - prev_rolling_5m_price) / prev_rolling_5m_price) * 100 if prev_rolling_5m_price > 0 else 0
                            else:
                                # Fallback: compare with single candle 5 periods ago
                                price_5m_ago = ohlcv[-6][4] if len(ohlcv) >= 6 else previous_price
                                rolling_price_change_pct = ((rolling_5m_price - price_5m_ago) / price_5m_ago) * 100 if price_5m_ago > 0 else 0
                        else:
                            # Not enough data for rolling average, use simple average
                            rolling_5m_price = (current_price + previous_price) / 2
                            price_5m_ago = ohlcv[-3][4] if len(ohlcv) >= 3 else previous_price
                            rolling_price_change_pct = ((rolling_5m_price - price_5m_ago) / price_5m_ago) * 100 if price_5m_ago > 0 else 0
                        
                        # For signal analysis, use current vs signal_price (max of previous close and current high)
                        price_change_pct = ((current_price - signal_price) / signal_price) * 100 if signal_price > 0 else 0
                        
                        # Calculate 6-hour price statistics if we have enough data
                        price_stats = None
                        if len(ohlcv) >= 72:  # 6 hours of 5-minute candles
                            six_hour_data = ohlcv[-72:]  # Last 6 hours
                            six_hour_prices = [candle[4] for candle in six_hour_data]  # Close prices
                            six_hour_highs = [candle[2] for candle in six_hour_data]  # High prices
                            six_hour_lows = [candle[3] for candle in six_hour_data]   # Low prices
                            
                            price_stats = {
                                'avg_price': sum(six_hour_prices) / len(six_hour_prices),
                                'low_price': min(six_hour_lows),
                                'high_price': max(six_hour_highs)
                            }
                        
                        # Fetch long/short ratio from configured exchange
                        long_short_data = fetch_long_short_ratio(exchange, symbol)

                        # Get current price and volume for USD calculation
                        ticker = exchange.fetch_ticker(symbol)
                        current_price = ticker['last']
                        current_volume_24h = ticker.get('quoteVolume', 0) or 0  # 24h USD volume
                        
                        # Calculate volume change using 5-minute comparison
                        volume_change_pct = 0
                        if len(ohlcv) >= 3:  # Need at least 3 candles for 5-minute comparison
                            # Current 5-minute volume
                            current_volume = ohlcv[-1][5]  # Volume of latest candle
                            # Previous 5-minute volume (5 minutes ago)
                            prev_volume = ohlcv[-2][5]
                            
                            if prev_volume > 0:
                                volume_change_pct = ((current_volume - prev_volume) / prev_volume) * 100

                        # Calculate USD values
                        current_value = current_contracts * current_price if exchange_name == 'bybit' else current_contracts
                        previous_value = previous_contracts * current_price if exchange_name == 'bybit' else previous_contracts
                        rolling_avg_value = rolling_avg_oi * current_price if exchange_name == 'bybit' else rolling_avg_oi
                
                        result = {
                            'symbol': symbol,
                            'base_symbol': base_symbol,
                            'exchange': exchange_name,
                            'oi_contracts': current_contracts,
                            'oi_value': current_value,
                            'price': current_price,
                            'oi_change_pct': oi_change_pct_rolling,  # 6-hour rolling change
                            'oi_change_pct_previous': oi_change_pct_previous,  # Current vs previous candle
                            'price_change_pct': price_change_pct,
                            'volume_24h': current_volume_24h,
                            'volume_change_pct': volume_change_pct,
                            'previous_oi_value': previous_value,
                            'rolling_avg_oi_value': rolling_avg_value,
                            'previous_price': previous_price,
                            'rolling_5m_price': rolling_5m_price,
                            'rolling_price_change_pct': rolling_price_change_pct,
                            'has_rolling_data': rolling_avg_oi is not None,
                            'price_stats': price_stats
                        }
                        
                        # Add long/short ratio data if available
                        if long_short_data:
                            if 'buy_ratio' in long_short_data:
                                # Buy/Sell ratio format
                                result.update({
                                    'buy_ratio': long_short_data['buy_ratio'],
                                    'sell_ratio': long_short_data['sell_ratio'],
                                    'long_short_ratio': long_short_data.get('long_short_ratio', 0),
                                    'ls_source': long_short_data.get('source', '')
                                })
                            else:
                                # Long/Short ratio format
                                result.update({
                                    'long_ratio': long_short_data.get('long_ratio', 0),
                                    'short_ratio': long_short_data.get('short_ratio', 0),
                                    'long_short_ratio': long_short_data.get('long_short_ratio', 0),
                                    'ls_source': long_short_data.get('source', '')
                                })
                        
                        return result
        
        return None
        
    except Exception as e:
        logger.warning(f"Failed to fetch data for {symbol} on {exchange_name}: {e}")
        return None

async def get_top_perps_by_oi(symbol_exchange_map):
    """Get top perpetual contracts by open interest from merged symbols using multithreading"""
    logger.info(f"Fetching OI data for {len(symbol_exchange_map)} symbols using multithreading...")
    
    # Convert symbol_exchange_map to list with base_symbol included
    symbols_list = [
        {'base_symbol': info['base_symbol'], 'symbol': info['symbol'], 'exchange': info['exchange']}
        for key, info in symbol_exchange_map.items()
    ]
    
    oi_data = []
    batch_size = 20
    
    # Process symbols in batches using ThreadPoolExecutor
    for i in range(0, len(symbols_list), batch_size):
        batch = symbols_list[i:i+batch_size]
        batch_number = i//batch_size + 1
        
        logger.info(f"Processing batch {batch_number}/{(len(symbols_list) + batch_size - 1)//batch_size}")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            # Submit all tasks in the batch
            futures = {executor.submit(process_symbol_oi, symbol_info): symbol_info for symbol_info in batch}
            
            # Collect results
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                if result is not None:
                    oi_data.append(result)
        
        # Sleep between batches
        await asyncio.sleep(0.1)
        logger.info(f"Completed batch {batch_number}, found {len(oi_data)} eligible symbols so far")
    
    # Sort by OI value
    df = pd.DataFrame(oi_data)
    if not df.empty:
        df_sorted = df.sort_values('oi_value', ascending=False)
        logger.info(f"Total eligible symbols: {len(df_sorted)}")
        return df_sorted.to_dict('records')
    return []

def format_number(num):
    """Format number to human readable format"""
    if num >= 1e9:
        return f"{num/1e9:.2f}B"
    elif num >= 1e6:
        return f"{num/1e6:.2f}M"
    elif num >= 1e3:
        return f"{num/1e3:.2f}K"
    else:
        return f"{num:.2f}"

def get_change_icon(oi_change_pct):
    """Get icon based on OI change percentage"""
    abs_change = abs(oi_change_pct)
    if abs_change >= OI_CHANGE_THRESHOLD_HIGH:
        return "🔥" if oi_change_pct > 0 else "❄️"
    elif abs_change >= OI_CHANGE_THRESHOLD:
        return "🚀" if oi_change_pct > 0 else "📉"
    else:
        return "" if oi_change_pct > 0 else "⬇️"

def calculate_price_change_pct(current_price, compare_price):
    """Calculate percentage change between two prices"""
    if compare_price and compare_price > 0:
        return ((current_price - compare_price) / compare_price) * 100
    return 0

def get_oi_direction_icon(oi_change):
    """Get directional icon for OI change"""
    if oi_change > 0:
        return "📈"  # Increasing OI
    else:
        return "📉"  # Decreasing OI

def get_price_direction_icon(price_change):
    """Get directional icon for price change"""
    if price_change > 0:
        return "🟢"  # Price up
    else:
        return "🔴"  # Price down

def get_main_icon(oi_change_6h, oi_change_5m, alert_key, current_oi_value, sent_alerts_24h, current_time):
    """Get main icon - show 🚀🚀🚀 only for first high occurrence in 24h and track OI direction"""
    abs_6h = abs(oi_change_6h)
    abs_5m = abs(oi_change_5m)
    
    # Check for very high changes
    is_high_change = abs_6h >= OI_CHANGE_THRESHOLD_HIGH or abs_5m >= OI_CHANGE_THRESHOLD_PREVIOUS_HIGH
    
    if is_high_change:
        # Check if this is the first high alert in 24h
        if alert_key in sent_alerts_24h:
            last_alert = sent_alerts_24h[alert_key]
            # If we had a high alert before, don't show 🚀🚀🚀 again
            if 'last_high_alert' in last_alert and last_alert['last_high_alert']:
                return ""
        
        # This is the first high alert in 24h
        return "🚀🚀🚀"
    else:
        return ""  # Default for regular significant changes

def analyze_market_signal(price_change, volume_change, oi_change):
    """
    Analyze market signal based on price, volume, and OI changes
    Returns signal type and description
    """
    # Define thresholds for "increase" vs "decrease"
    threshold = 0.5  # 0.5% threshold for meaningful change
    
    price_up = price_change > threshold
    price_down = price_change < -threshold
    volume_up = volume_change > threshold
    oi_up = oi_change > threshold
    oi_down = oi_change < -threshold
    
    # Analyze based on the table provided
    if price_up and volume_up and oi_up:
        return "🟢 BULLISH", "New Longs Opening (Strong Trend)"
    elif price_up and volume_up and oi_down:
        return "🟡 WEAK BULLISH", "Short Covering (Price Up but Weakening)"
    elif price_down and volume_up and oi_up:
        return "🔴 BEARISH", "New Shorts Opening (Strong Selling)"
    elif price_down and volume_up and oi_down:
        return "🟡 POTENTIAL BOTTOM", "Long Liquidation (May Form Bottom)"
    elif price_up and oi_up:
        return "🟢 BULLISH", "Longs Building"
    elif price_down and oi_up:
        return "🔴 BEARISH", "Shorts Building"
    elif price_up and oi_down:
        return "⚪ NEUTRAL", "Position Closing"
    elif price_down and oi_down:
        return "⚪ NEUTRAL", "Position Closing"
    else:
        return "", ""

def format_long_short_ratio(data):
    """Format long/short or buy/sell ratio"""
    source = data.get('ls_source', '')
    
    # Check for Buy/Sell ratio format
    if 'buy_ratio' in data and 'sell_ratio' in data:
        buy_ratio = data['buy_ratio']
        sell_ratio = data['sell_ratio']
        long_short_ratio = data.get('long_short_ratio', 0)
        
        # Handle NaN values
        if pd.isna(buy_ratio) or buy_ratio is None or pd.isna(sell_ratio) or sell_ratio is None:
            return ""
        
        # Format as B/S: 0.609 | L/S: 1.55
        # Always show source since we're using Binance for L/S
        return f" | 📊 B/S: {buy_ratio:.3f} | L/S: {long_short_ratio:.2f}"
    
    # Check for Long/Short ratio format
    elif 'long_ratio' in data and 'short_ratio' in data:
        long_ratio = data['long_ratio']
        short_ratio = data['short_ratio']
        long_short_ratio = data.get('long_short_ratio', 0)
        
        # Handle NaN values
        if pd.isna(long_ratio) or long_ratio is None or pd.isna(short_ratio) or short_ratio is None:
            return ""
        
        return f" | 📊 L/S: {long_short_ratio:.2f}"
    
    return ""

def calculate_oi_changes(current_data):
    """Calculate OI changes and format alerts with improved formatting"""
    global sent_alerts_24h
    alerts = []
    current_time = datetime.now()
    
    # Clean up alerts older than 24 hours
    cutoff_time = current_time - timedelta(hours=24)
    sent_alerts_24h = {k: v for k, v in sent_alerts_24h.items() 
                       if v['timestamp'] > cutoff_time}
    
    for data in current_data:
        base_symbol = data['base_symbol']
        exchange_name = data['exchange'].capitalize()
        
        # Get both types of changes
        oi_change_pct = data.get('oi_change_pct', 0)  # 6-hour rolling
        oi_change_pct_previous = data.get('oi_change_pct_previous', 0)  # Current vs previous
        current_price = data['price']
        previous_price = data['previous_price']
        
        # Check if current OI meets minimum threshold
        current_oi_value = data['oi_value']
        if current_oi_value < MIN_OI_VALUE:
            continue
        
        # Check if significant change (either type)
        significant_rolling = abs(oi_change_pct) >= OI_CHANGE_THRESHOLD
        significant_previous = abs(oi_change_pct_previous) >= OI_CHANGE_THRESHOLD_PREVIOUS
        
        if significant_rolling or significant_previous:
            alert_key = f"{base_symbol}_{exchange_name}"
            should_send_alert = False
            star = ""
            
            # Use the more significant change for alerting logic
            primary_change = oi_change_pct if abs(oi_change_pct) > abs(oi_change_pct_previous) else oi_change_pct_previous
            
            # Get current 5-minute window
            current_window = get_5min_window(current_time)
            
            # Check if this symbol was alerted before
            if alert_key in sent_alerts_24h:
                last_alert_data = sent_alerts_24h[alert_key]
                
                # Get rolling values for comparison
                current_5m_rolling = data.get('oi_change_pct_previous', 0)  # 5m OI change
                current_6h_rolling = data.get('oi_change_pct', 0)  # 6h OI change
                
                # Get last snapshots
                last_5m_snapshot = last_alert_data.get('snapshot_5m_rolling', 0)
                last_6h_snapshot = last_alert_data.get('snapshot_6h_rolling', 0)
                
                # Calculate absolute changes from snapshots
                change_5m_from_snapshot = abs(abs(current_5m_rolling) - abs(last_5m_snapshot))
                change_6h_from_snapshot = abs(abs(current_6h_rolling) - abs(last_6h_snapshot))
                
                # Check thresholds: 5% for 5m rolling, 10% for 6h rolling
                is_5m_threshold_met = change_5m_from_snapshot >= 5.0
                is_6h_threshold_met = change_6h_from_snapshot >= 10.0
                
                if is_5m_threshold_met or is_6h_threshold_met:
                    should_send_alert = True
                    triggers = []
                    if is_5m_threshold_met:
                        triggers.append(f"5m Δ{change_5m_from_snapshot:.1f}%")
                    if is_6h_threshold_met:
                        triggers.append(f"6h Δ{change_6h_from_snapshot:.1f}%")
                    star = f" 🔥 ({', '.join(triggers)})"
            else:
                # First time seeing this symbol, send alert
                should_send_alert = True
            
            if should_send_alert:
                # Check if OI decreased after a high alert (skip if so)
                if alert_key in sent_alerts_24h:
                    last_alert = sent_alerts_24h[alert_key]
                    if ('last_high_alert' in last_alert and 
                        last_alert['last_high_alert'] and 
                        'last_oi_value' in last_alert and 
                        current_oi_value < last_alert['last_oi_value']):
                        # OI decreased after high alert, skip this alert
                        continue
                
                # Get main icon
                main_icon = get_main_icon(oi_change_pct, oi_change_pct_previous, alert_key, current_oi_value, sent_alerts_24h, current_time)
                
                # Get occurrence count
                occurrence_count = sent_alerts_24h.get(alert_key, {}).get('total_count', 0) + 1
                
                # Analyze market signal
                signal_type, signal_desc = analyze_market_signal(
                    data.get('price_change_pct', 0),
                    data.get('volume_change_pct', 0),
                    oi_change_pct_previous  # Use 5m OI change for immediate signal
                )
                
                # Format long/short ratio
                long_short_text = format_long_short_ratio(data)
                
                # Build main header line with signal
                main_signal_text = f" | 🔥{signal_desc}🔥" if signal_desc else ""
                icon_text = f"{main_icon} " if main_icon else ""
                # Show ⭐ for first occurrence, otherwise show count
                count_display = "⭐" if occurrence_count == 1 else f"({occurrence_count})"

                # Calculate price change from first notification
                price_from_first = 0
                first_price = current_price
                if alert_key in sent_alerts_24h and 'first_price' in sent_alerts_24h[alert_key]:
                    first_price = sent_alerts_24h[alert_key]['first_price']
                    price_from_first = ((current_price - first_price) / first_price) * 100 if first_price > 0 else 0

                # Build price change display
                price_change_icon = "🟢" if price_from_first > 0 else "🔴" if price_from_first < 0 else "⚪"
                price_change_text = f" {price_change_icon}({price_from_first:+.1f}%)" if occurrence_count > 1 else ""

                alert = f"{icon_text}{base_symbol} {count_display} | {exchange_name.upper()}{main_signal_text}\n\n"
                alert += f"💰 OI: ${format_number(data['oi_value'])} | 💲 ${current_price:.4f}{price_change_text}{long_short_text} | \n"

                # Build simplified OI change line with 6H and 5m data
                oi_6h_icon = "🟢" if oi_change_pct > 0 else "🔴"
                oi_5m_icon = "🟢" if oi_change_pct_previous > 0 else "🔴"

                # Get 6H OI value if available
                if data.get('has_rolling_data', False):
                    alert += f"📈 6H: ${format_number(data['rolling_avg_oi_value'])} {oi_6h_icon}({oi_change_pct:+.0f}%) | 5m: ${format_number(data['previous_oi_value'])} {oi_5m_icon}({oi_change_pct_previous:+.0f}%)\n"
                else:
                    alert += f"📈 5m: ${format_number(data['previous_oi_value'])} {oi_5m_icon}({oi_change_pct_previous:+.0f}%)\n"

                # Determine if this is a high alert
                is_high_alert = (abs(oi_change_pct) >= OI_CHANGE_THRESHOLD_HIGH or 
                               abs(oi_change_pct_previous) >= OI_CHANGE_THRESHOLD_PREVIOUS_HIGH)
                
                # Update alert tracking
                if alert_key in sent_alerts_24h:
                    last_alert = sent_alerts_24h[alert_key]
                    high_count = last_alert.get('high_alert_count', 0)
                    total_count = last_alert.get('total_count', 0) + 1
                    if is_high_alert and not last_alert.get('last_high_alert'):
                        high_count += 1
                else:
                    high_count = 1 if is_high_alert else 0
                    total_count = 1
                
                # Record this alert with enhanced tracking including rolling snapshots
                # Save current rolling values as new snapshots after sending alert
                current_5m_rolling = data.get('oi_change_pct_previous', 0)  # 5m OI change
                current_6h_rolling = data.get('oi_change_pct', 0)  # 6h OI change

                # Preserve first_price if it exists, otherwise set it now
                preserved_first_price = sent_alerts_24h.get(alert_key, {}).get('first_price', first_price)

                sent_alerts_24h[alert_key] = {
                    'timestamp': current_time,
                    'oi_change_pct': primary_change,
                    'high_alert_count': high_count,
                    'last_high_alert': current_time if is_high_alert else sent_alerts_24h.get(alert_key, {}).get('last_high_alert'),
                    'last_oi_value': current_oi_value,
                    'last_rolling_avg_value': data.get('rolling_avg_oi_value', 0),
                    'total_count': total_count,
                    'snapshot_5m_rolling': current_5m_rolling,  # Save current 5m rolling as snapshot
                    'snapshot_6h_rolling': current_6h_rolling,  # Save current 6h rolling as snapshot
                    'first_price': preserved_first_price  # Save the price from first alert
                }
                
                alerts.append({
                    'text': alert,
                    'oi_change': primary_change,
                    'symbol': base_symbol,
                    'exchange': exchange_name
                })
    
    return alerts

async def process_exchange_symbols():
    """Process exchange symbols for OI changes"""
    try:
        logger.info(f"Processing {', '.join(EXCHANGE_NAMES)} symbols for OI changes...")

        # Get exchange symbols
        symbol_exchange_map = get_exchange_symbols()

        # Get OI data for all symbols
        current_data = await get_top_perps_by_oi(symbol_exchange_map)

        # Calculate changes and generate alerts
        alerts = calculate_oi_changes(current_data)

        return alerts

    except Exception as e:
        logger.error(f"Error processing symbols: {e}")
        return []

async def check_oi_changes():
    """Main loop to check OI changes"""
    while True:
        try:
            logger.info(f"Running OI check at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            
            # Process exchange symbols
            all_alerts = await process_exchange_symbols()
            
            # Sort alerts by OI change magnitude
            all_alerts.sort(key=lambda x: abs(x['oi_change']), reverse=True)
            
            # Send alerts in batches with separators
            if all_alerts:
                # Add separators between alerts for better readability
                alert_texts = []
                for i, alert in enumerate(all_alerts):
                    alert_texts.append(alert['text'])
                    # Add separator line after each alert
                    alert_texts.append("━━━━━━━━━━━━━━━━━━━━━━")
                
                message = "\n\n".join(alert_texts)
                await send_telegram_message(message, THREAD_ID)
                logger.info(f"Sent {len(all_alerts)} OI alerts")
            else:
                logger.info("No significant OI changes detected")
            
            # Sleep for 1 minute before next check
            logger.info("Sleeping for 1 minute...")
            time.sleep(30)
            
        except Exception as e:
            logger.error(f"Error in check_oi_changes: {e}")
            # Sleep even on error to prevent rapid retries

async def main():
    logger.info(f"Starting Open Interest Monitoring Bot with config: {args.config}")
    logger.info(f"Configuration: OI Threshold={OI_CHANGE_THRESHOLD}%, Price Threshold={PRICE_CHANGE_THRESHOLD}%")
    logger.info(f"Exchanges={', '.join(EXCHANGE_NAMES)}, Interval={INTERVAL_MINUTES}min")

    # Load markets for all exchanges
    logger.info("Loading markets...")
    for exchange_name, exchange_obj in exchanges.items():
        logger.info(f"Loading {exchange_name} markets...")
        exchange_obj.load_markets()
    logger.info("All markets loaded successfully")

    await check_oi_changes()

if __name__ == "__main__":
    asyncio.run(main())
import ccxt
import concurrent.futures
import re
from typing import Dict, List, Any

# --- Configuration ---
EXCHANGE_IDS: List[str] = ['binance', 'kucoin', 'bitget', 'bybit', 'bingx']
QUOTE_CURRENCY: str = 'USDT'

# Spread Constraints
MIN_SPREAD_PERCENT: float = 0.5   # Minimum % price difference to report
MAX_SPREAD_PERCENT: float = 15.0  # Maximum % to prevent "Ticker Collision"

# Funding Rate Constraints
MIN_FUNDING_RATE_PERCENT: float = 0.01 # Minimum positive funding rate % (Shorts receive funding)

# Volume Constraints
MIN_VOLUME_USDT: float = 10000.0
MAX_VOLUME_USDT: float = 90000000.0

# Blacklist for specific tokens (Use the BASE token name here, e.g., 'PEPE')
BLACKLISTED_TOKENS: List[str] = ['ZKP'] 

class SpotPerpArbitrageScanner:
    def __init__(self, exchange_ids: List[str]):
        self.spot_exchanges: Dict[str, ccxt.Exchange] = {}
        self.swap_exchanges: Dict[str, ccxt.Exchange] = {}
        
        print("Initializing CCXT Exchange Instances...")
        for eid in exchange_ids:
            try:
                ex_class = getattr(ccxt, eid)
                self.spot_exchanges[eid] = ex_class({
                    'enableRateLimit': True, 
                    'options': {'defaultType': 'spot'}
                })
                self.swap_exchanges[eid] = ex_class({
                    'enableRateLimit': True, 
                    'options': {'defaultType': 'swap'}
                })
            except AttributeError:
                print(f"Exchange {eid} not supported by CCXT.")

    def parse_symbol_multiplier(self, symbol: str):
        """
        Detects standard exchange prefixes (1000, 1M, etc.) used for perpetuals.
        Returns the base symbol (e.g., PEPE) and the multiplier (e.g., 1000).
        """
        match = re.match(r'^(100|1000|10000|100000|1000000|1M|10M|100M|1K|10K|100K)(.+)$', symbol, re.IGNORECASE)
        if match:
            prefix = match.group(1).upper()
            base_coin = match.group(2).upper()
            if 'M' in prefix:
                multiplier = int(prefix.replace('M', '')) * 1_000_000
            elif 'K' in prefix:
                multiplier = int(prefix.replace('K', '')) * 1_000
            else:
                multiplier = int(prefix)
            return base_coin, multiplier
        return symbol.upper(), 1

    def fetch_market_data(self, args) -> Dict[str, Any]:
        """Fetches tickers, funding rates (for swaps), normalizes symbols, and calculates multipliers."""
        eid, market_type, ex = args
        try:
            ex.load_markets()
            tickers = ex.fetch_tickers()
            
            funding_rates = {}
            if market_type == 'swap':
                try:
                    # Check if the exchange supports fetching all funding rates at once
                    if ex.has.get('fetchFundingRates'):
                        funding_rates = ex.fetch_funding_rates()
                except Exception as e:
                    print(f"[{eid}] Warning: Could not fetch funding rates - {e}")

            filtered_tickers = {}
            for symbol, ticker in tickers.items():
                if market_type == 'spot' and symbol.endswith(f'/{QUOTE_CURRENCY}'):
                    raw_coin = symbol.split('/')[0]
                    base_coin, mult = self.parse_symbol_multiplier(raw_coin)
                    filtered_tickers[raw_coin] = {
                        'ticker': ticker, 'base': base_coin, 'mult': mult, 'raw': raw_coin
                    }
                elif market_type == 'swap' and symbol.endswith(f'/{QUOTE_CURRENCY}:{QUOTE_CURRENCY}'):
                    raw_coin = symbol.split('/')[0]
                    base_coin, mult = self.parse_symbol_multiplier(raw_coin)
                    
                    # Extract funding rate as a percentage
                    fr_pct = 0.0
                    if symbol in funding_rates:
                        fr_raw = funding_rates[symbol].get('fundingRate')
                        if fr_raw is not None:
                            fr_pct = fr_raw * 100
                            
                    filtered_tickers[raw_coin] = {
                        'ticker': ticker, 'base': base_coin, 'mult': mult, 'raw': raw_coin, 'funding_rate': fr_pct
                    }
                    
            return {'id': eid, 'type': market_type, 'tickers': filtered_tickers}
        except Exception as e:
            return {'id': eid, 'type': market_type, 'error': str(e)}

    def scan(self):
        print(f"\nScanning {len(EXCHANGE_IDS)} exchanges for Buy Spot / Short Perp Arbitrage...")
        print(f"Spread Filter: {MIN_SPREAD_PERCENT}% to {MAX_SPREAD_PERCENT}%")
        print(f"Min Funding Rate (Short Receives): {MIN_FUNDING_RATE_PERCENT}%")
        print(f"Volume Filter: {MIN_VOLUME_USDT} - {MAX_VOLUME_USDT} {QUOTE_CURRENCY}\n")
        
        # 1. Fetch data concurrently
        tasks = []
        for eid, ex in self.spot_exchanges.items(): 
            tasks.append((eid, 'spot', ex))
        for eid, ex in self.swap_exchanges.items(): 
            tasks.append((eid, 'swap', ex))
            
        with concurrent.futures.ThreadPoolExecutor(max_workers=22) as executor:
            results = list(executor.map(self.fetch_market_data, tasks))

        # 2. Aggregate data by BASE Coin
        coin_data = {}
        for r in results:
            if 'error' in r:
                continue
            
            eid = r['id']
            m_type = r['type']
            for raw_coin, data in r['tickers'].items():
                base_coin = data['base']
                
                if base_coin in BLACKLISTED_TOKENS:
                    continue
                
                if base_coin not in coin_data:
                    coin_data[base_coin] = {'spot': {}, 'swap': {}}
                coin_data[base_coin][m_type][eid] = data

        opportunities_found = False

        # 3. Analyze Spreads and Funding Rates
        for base_coin, markets in coin_data.items():
            spots = markets['spot']
            swaps = markets['swap']
            
            if not spots or not swaps:
                continue

            valid_spots = {}
            for ex, data in spots.items():
                tick = data['ticker']
                ask = tick.get('ask')
                
                if not ask or ask <= 0:
                    continue
                    
                vol = tick.get('quoteVolume') or ((tick.get('baseVolume') or 0) * ask)
                if MIN_VOLUME_USDT <= vol <= MAX_VOLUME_USDT:
                    valid_spots[ex] = {
                        'norm_ask': ask / data['mult'], 'vol': vol, 
                        'actual_price': ask, 'raw_coin': data['raw']
                    }

            valid_swaps = {}
            for ex, data in swaps.items():
                tick = data['ticker']
                bid = tick.get('bid')
                
                if not bid or bid <= 0:
                    continue
                    
                vol = tick.get('quoteVolume') or ((tick.get('baseVolume') or 0) * bid)
                if MIN_VOLUME_USDT <= vol <= MAX_VOLUME_USDT:
                    valid_swaps[ex] = {
                        'norm_bid': bid / data['mult'], 'vol': vol, 
                        'actual_price': bid, 'raw_coin': data['raw'],
                        'funding_rate': data.get('funding_rate', 0.0)
                    }

            for spot_ex, spot_data in valid_spots.items():
                for swap_ex, swap_data in valid_swaps.items():
                    
                    spot_ask_norm = spot_data['norm_ask']
                    swap_bid_norm = swap_data['norm_bid']
                    fund_rate = swap_data['funding_rate']

                    spread = ((swap_bid_norm - spot_ask_norm) / spot_ask_norm) * 100
                    
                    # --- CORE ARBITRAGE SIGNAL CRITERIA ---
                    # 1. Spread is within our safe limits (prevents false positives)
                    # 2. Funding rate meets our minimum threshold (Ensures short position gets paid)
                    if (MIN_SPREAD_PERCENT <= spread <= MAX_SPREAD_PERCENT) and (fund_rate >= MIN_FUNDING_RATE_PERCENT):
                        self._print_opportunity(
                            base_coin, spread, fund_rate, spot_ex, swap_ex, spot_data, swap_data
                        )
                        opportunities_found = True

        if not opportunities_found:
            print("No Spot-Perp Arbitrage found matching the criteria.")

    def _print_opportunity(self, base_coin, spread, fund_rate, buy_ex, sell_ex, buy_data, sell_data):
        print("-" * 65)
        print(f"🚀 SIGNAL: {base_coin} | Spread: {spread:.2f}% | Funding: {fund_rate:.4f}%")
        print(f"Action: Buy Spot / Short Perp (Delta Neutral)")
        print(f"Execution: Buy {buy_data['raw_coin']} @ {buy_ex.upper()} ({buy_data['actual_price']}) "
              f"-> Sell {sell_data['raw_coin']} @ {sell_ex.upper()} ({sell_data['actual_price']})")
        print(f"24h Volumes: {buy_ex}: {buy_data['vol']:.0f} USDT | {sell_ex}: {sell_data['vol']:.0f} USDT")

if __name__ == "__main__":
    scanner = SpotPerpArbitrageScanner(EXCHANGE_IDS)
    scanner.scan()

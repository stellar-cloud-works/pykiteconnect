#!/usr/bin/env python3
"""
Standalone Tick to OHLC Converter
Converts real-time tick data to OHLC candlestick data with multiple timeframe support
"""
import json
from datetime import datetime, timedelta
from collections import defaultdict
import argparse


class TickToOHLCConverter:
    # Supported intervals mapping
    INTERVALS = {
        'minute': 1,
        '3minute': 3,
        '5minute': 5,
        '10minute': 10,
        '15minute': 15,
        '30minute': 30,
        '60minute': 60,
        'day': 1440  # 24 hours * 60 minutes
    }
    
    def __init__(self, interval='minute'):
        if interval not in self.INTERVALS:
            raise ValueError(f"Invalid interval. Must be one of: {list(self.INTERVALS.keys())}")
        
        self.interval = interval
        self.interval_minutes = self.INTERVALS[interval]
        self.ticks = []
        self.ohlc_candles = []
        
    def load_tick_data(self, tick_file):
        """Load tick data from JSONL file"""
        print(f"Loading tick data from {tick_file}...")
        
        with open(tick_file, 'r') as f:
            for line in f:
                if line.strip():
                    tick = json.loads(line.strip())
                    self.ticks.append(tick)
        
        # Sort ticks by timestamp (chronological order)
        self.ticks.sort(key=lambda x: x['timestamp'])
        
        print(f"Loaded {len(self.ticks)} ticks")
        return len(self.ticks)
    
    def get_window_start(self, timestamp_str):
        """
        Get the window start time for a given timestamp based on interval
        Rounds DOWN to the nearest interval boundary
        
        Examples:
          1-minute: 2025-09-22T10:39:45.123456 -> 2025-09-22T10:39:00
          5-minute: 2025-09-22T10:37:45.123456 -> 2025-09-22T10:35:00
          15-minute: 2025-09-22T10:37:45.123456 -> 2025-09-22T10:30:00
          day: 2025-09-22T10:37:45.123456 -> 2025-09-22T00:00:00
        """
        # Parse timestamp (with microseconds)
        dt = datetime.fromisoformat(timestamp_str)
        
        if self.interval == 'day':
            # Round down to start of day (00:00:00)
            window_start = dt.replace(hour=0, minute=0, second=0, microsecond=0)
        else:
            # Calculate minutes since midnight
            minutes_since_midnight = dt.hour * 60 + dt.minute
            
            # Round down to nearest interval
            interval_periods = minutes_since_midnight // self.interval_minutes
            aligned_minutes = interval_periods * self.interval_minutes
            
            # Calculate hour and minute for window start
            window_hour = aligned_minutes // 60
            window_minute = aligned_minutes % 60
            
            # Create window start time
            window_start = dt.replace(hour=window_hour, minute=window_minute, second=0, microsecond=0)
        
        return window_start
    
    def group_ticks_by_window(self):
        """Group ticks into windows based on interval"""
        print(f"Grouping ticks by {self.interval} windows...")
        
        windows = defaultdict(list)
        
        for tick in self.ticks:
            window_start = self.get_window_start(tick['timestamp'])
            windows[window_start].append(tick)
        
        print(f"Created {len(windows)} {self.interval} windows")
        return windows
    
    def calculate_ohlc_for_window(self, ticks_in_window):
        """
        Calculate OHLC for a single window
        
        Logic:
        - OPEN: last_price of FIRST tick (chronologically)
        - HIGH: maximum last_price across all ticks
        - LOW: minimum last_price across all ticks
        - CLOSE: last_price of LAST tick (chronologically)
        - VOLUME: sum of last_traded_quantity for all ticks
        """
        if not ticks_in_window:
            return None
        
        # Ensure ticks are sorted by timestamp within the window
        sorted_ticks = sorted(ticks_in_window, key=lambda x: x['timestamp'])
        
        # OHLC calculation
        open_price = sorted_ticks[0]['last_price']
        close_price = sorted_ticks[-1]['last_price']
        high_price = max(tick['last_price'] for tick in sorted_ticks)
        low_price = min(tick['last_price'] for tick in sorted_ticks)
        
        # Volume calculation (Option A: Sum of last_traded_quantity)
        volume = sum(tick['last_traded_quantity'] for tick in sorted_ticks)
        
        return {
            'open': open_price,
            'high': high_price,
            'low': low_price,
            'close': close_price,
            'volume': volume
        }
    
    def convert_to_ohlc(self):
        """Convert all ticks to OHLC candles"""
        print(f"Converting ticks to {self.interval} OHLC candles...")
        
        # Group ticks by windows
        windows = self.group_ticks_by_window()
        
        # Calculate OHLC for each window
        candles = []
        
        for window_time in sorted(windows.keys()):
            ticks_in_window = windows[window_time]
            
            ohlc = self.calculate_ohlc_for_window(ticks_in_window)
            
            if ohlc:
                # Format timestamp with IST timezone (+05:30)
                if self.interval == 'day':
                    # For daily candles, use date format
                    candle_timestamp = window_time.strftime('%Y-%m-%dT%H:%M:%S') + '+05:30'
                else:
                    candle_timestamp = window_time.strftime('%Y-%m-%dT%H:%M:%S') + '+05:30'
                
                candle = {
                    'date': candle_timestamp,
                    'open': ohlc['open'],
                    'high': ohlc['high'],
                    'low': ohlc['low'],
                    'close': ohlc['close'],
                    'volume': ohlc['volume'],
                    'oi': None
                }
                
                candles.append(candle)
        
        self.ohlc_candles = candles
        print(f"Generated {len(candles)} {self.interval} OHLC candles")
        
        return candles
    
    def create_output_json(self, instrument_token):
        """Create output JSON in historical data format"""
        if not self.ohlc_candles:
            return {
                'instrument_token': instrument_token,
                'interval': self.interval,
                'from_dt': None,
                'to_dt': None,
                'continuous': False,
                'oi': False,
                'data': [],
                'count': 0
            }
        
        # Extract from_dt and to_dt without timezone for metadata
        first_candle_dt = datetime.fromisoformat(self.ohlc_candles[0]['date'].replace('+05:30', ''))
        last_candle_dt = datetime.fromisoformat(self.ohlc_candles[-1]['date'].replace('+05:30', ''))
        
        from_dt = first_candle_dt.strftime('%Y-%m-%d %H:%M:%S')
        to_dt = last_candle_dt.strftime('%Y-%m-%d %H:%M:%S')
        
        output = {
            'instrument_token': instrument_token,
            'interval': self.interval,
            'from_dt': from_dt,
            'to_dt': to_dt,
            'continuous': False,
            'oi': False,
            'data': self.ohlc_candles,
            'count': len(self.ohlc_candles)
        }
        
        return output
    
    def save_to_file(self, output_data, output_file):
        """Save OHLC data to JSON file"""
        print(f"Saving {self.interval} OHLC data to {output_file}...")
        
        with open(output_file, 'w') as f:
            json.dump(output_data, f, indent=4)
        
        print(f"Successfully saved {output_data['count']} candles")
    
    def verify_against_historical(self, historical_file, tolerance_pct=1.0):
        """
        Verify generated OHLC against historical data
        Tolerance: ±1% for volume mismatch
        """
        print(f"\nVerifying against historical data: {historical_file}")
        
        with open(historical_file, 'r') as f:
            historical_data = json.load(f)
        
        historical_candles = historical_data['data']
        generated_candles = self.ohlc_candles
        
        print(f"Historical candles: {len(historical_candles)}")
        print(f"Generated candles: {len(generated_candles)}")
        
        if len(historical_candles) != len(generated_candles):
            print(f"⚠ WARNING: Candle count mismatch!")
            print(f"  Historical: {len(historical_candles)}, Generated: {len(generated_candles)}")
        
        # Compare candles
        mismatches = []
        matches = 0
        
        for i, (hist, gen) in enumerate(zip(historical_candles, generated_candles)):
            if hist['date'] != gen['date']:
                mismatches.append(f"Candle {i}: Timestamp mismatch - {hist['date']} vs {gen['date']}")
                continue
            
            # Check OHLC values (exact match expected)
            ohlc_match = (
                hist['open'] == gen['open'] and
                hist['high'] == gen['high'] and
                hist['low'] == gen['low'] and
                hist['close'] == gen['close']
            )
            
            # Check volume with tolerance
            volume_diff_pct = abs(hist['volume'] - gen['volume']) / hist['volume'] * 100 if hist['volume'] > 0 else 0
            volume_match = volume_diff_pct <= tolerance_pct
            
            if ohlc_match and volume_match:
                matches += 1
            else:
                mismatch_details = []
                if not ohlc_match:
                    mismatch_details.append(
                        f"OHLC: H({hist['open']},{hist['high']},{hist['low']},{hist['close']}) "
                        f"vs G({gen['open']},{gen['high']},{gen['low']},{gen['close']})"
                    )
                if not volume_match:
                    mismatch_details.append(
                        f"Volume: H({hist['volume']}) vs G({gen['volume']}) "
                        f"[{volume_diff_pct:.2f}% diff]"
                    )
                mismatches.append(f"Candle {i} ({hist['date']}): {', '.join(mismatch_details)}")
        
        # Print results
        print(f"\n{'='*60}")
        print(f"VERIFICATION RESULTS")
        print(f"{'='*60}")
        print(f"Total candles compared: {min(len(historical_candles), len(generated_candles))}")
        print(f"Exact matches: {matches}")
        print(f"Mismatches: {len(mismatches)}")
        print(f"Match rate: {matches/min(len(historical_candles), len(generated_candles))*100:.2f}%")
        
        if mismatches:
            print(f"\n{'='*60}")
            print(f"MISMATCH DETAILS (First 10):")
            print(f"{'='*60}")
            for mismatch in mismatches[:10]:
                print(f"  {mismatch}")
            if len(mismatches) > 10:
                print(f"  ... and {len(mismatches) - 10} more mismatches")
        else:
            print(f"\n✓ ALL CANDLES MATCH! Perfect conversion!")
        
        return matches == min(len(historical_candles), len(generated_candles))


def main():
    parser = argparse.ArgumentParser(
        description='Convert tick data to OHLC candlestick data with multiple timeframe support',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Supported Intervals:
  minute    - 1-minute candles
  3minute   - 3-minute candles
  5minute   - 5-minute candles
  10minute  - 10-minute candles
  15minute  - 15-minute candles
  30minute  - 30-minute candles
  60minute  - 60-minute (hourly) candles
  day       - Daily candles

Examples:
  # 1-minute candles (default)
  python tick_to_ohlc.py --tick-file tick.jsonl --output ohlc_1m.json --instrument-token 1510401
  
  # 5-minute candles
  python tick_to_ohlc.py --tick-file tick.jsonl --output ohlc_5m.json --instrument-token 1510401 --interval 5minute
  
  # 15-minute candles with verification
  python tick_to_ohlc.py --tick-file tick.jsonl --output ohlc_15m.json --instrument-token 1510401 --interval 15minute --verify historical_15m.json
  
  # Daily candles
  python tick_to_ohlc.py --tick-file tick.jsonl --output ohlc_day.json --instrument-token 1510401 --interval day
        """
    )
    
    parser.add_argument('--tick-file', required=True, help='Input tick data JSONL file')
    parser.add_argument('--output', required=True, help='Output OHLC data JSON file')
    parser.add_argument('--instrument-token', type=int, required=True, help='Instrument token')
    parser.add_argument('--interval', 
                       choices=['minute', '3minute', '5minute', '10minute', '15minute', '30minute', '60minute', 'day'],
                       default='minute',
                       help='OHLC interval (default: minute)')
    parser.add_argument('--verify', help='Historical OHLC file for verification (optional)')
    parser.add_argument('--tolerance', type=float, default=1.0, help='Volume tolerance percentage (default: 1.0)')
    
    args = parser.parse_args()
    
    # Create converter with specified interval
    print(f"Creating converter for {args.interval} interval...")
    converter = TickToOHLCConverter(interval=args.interval)
    
    # Load tick data
    tick_count = converter.load_tick_data(args.tick_file)
    
    if tick_count == 0:
        print("No ticks loaded. Exiting.")
        return
    
    # Convert to OHLC
    candles = converter.convert_to_ohlc()
    
    if not candles:
        print("No candles generated. Exiting.")
        return
    
    # Create output JSON
    output_data = converter.create_output_json(args.instrument_token)
    
    # Save to file
    converter.save_to_file(output_data, args.output)
    
    # Verify if historical file provided
    if args.verify:
        print(f"\n{'='*60}")
        is_perfect = converter.verify_against_historical(args.verify, args.tolerance)
        print(f"{'='*60}")
        
        if is_perfect:
            print(f"\n✓✓✓ SUCCESS: Generated {args.interval} OHLC matches historical data perfectly! ✓✓✓")
        else:
            print(f"\n⚠ ATTENTION: Some mismatches found. Review details above.")
    
    print(f"\n{args.interval.upper()} conversion completed!")


if __name__ == '__main__':
    main()

#!/usr/bin/env python3
"""
Quick test script for tick_to_ohlc converter
Tests the converter with the actual data files across multiple timeframes
"""
import subprocess
import sys
import os
from pathlib import Path

def run_conversion_test(interval='minute', verify_file=None):
    """Run the tick to OHLC conversion with optional verification"""
    
    print("="*70)
    print(f"TICK TO OHLC CONVERTER - {interval.upper()} VALIDATION TEST")
    print("="*70)
    print()
    
    # Test parameters
    tick_file = "2025-09-22_1510401_tick.jsonl"
    output_file = f"2025-09-22_1510401_generated_{interval}.json"
    instrument_token = 1510401
    
    print(f"Test Configuration:")
    print(f"  Tick file: {tick_file}")
    print(f"  Output file: {output_file}")
    print(f"  Interval: {interval}")
    print(f"  Instrument token: {instrument_token}")
    
    if verify_file and Path(verify_file).exists():
        print(f"  Verification file: {verify_file}")
    else:
        print(f"  Verification: SKIPPED (no historical data available)")
    
    print()
    print("="*70)
    print()
    
    # Build command
    cmd = [
        sys.executable,
        "tick_to_ohlc.py",
        "--tick-file", tick_file,
        "--output", output_file,
        "--instrument-token", str(instrument_token),
        "--interval", interval,
        "--tolerance", "1.0"
    ]
    
    # Add verification file if it exists
    if verify_file and Path(verify_file).exists():
        cmd.extend(["--verify", verify_file])
    
    try:
        result = subprocess.run(cmd, capture_output=False, text=True)
        
        if result.returncode == 0:
            print()
            print("="*70)
            print(f"{interval.upper()} TEST COMPLETED SUCCESSFULLY")
            print("="*70)
            return True
        else:
            print()
            print("="*70)
            print(f"{interval.upper()} TEST FAILED")
            print("="*70)
            return False
            
    except Exception as e:
        print(f"Error running test: {e}")
        return False

def run_all_tests():
    """Run tests for all available intervals"""
    
    print("\n")
    print("="*70)
    print("RUNNING COMPREHENSIVE TICK TO OHLC VALIDATION TESTS")
    print("="*70)
    print()
    
    # Define test cases with their potential verification files
    test_cases = [
        ('minute', '2025-09-22_1510401_minute.jsonl'),
        ('3minute', '2025-09-22_1510401_3minute.json'),
        ('5minute', '2025-09-22_1510401_5minute.json'),
        ('10minute', '2025-09-22_1510401_10minute.json'),
        ('15minute', '2025-09-22_1510401_15minute.json'),
        ('30minute', '2025-09-22_1510401_30minute.json'),
        ('60minute', '2025-09-22_1510401_60minute.json'),
        ('day', '2025-09-22_1510401_day.json'),
    ]
    
    results = {}
    
    for interval, verify_file in test_cases:
        print(f"\n{'='*70}")
        print(f"Testing {interval} interval...")
        print(f"{'='*70}\n")
        
        success = run_conversion_test(interval, verify_file)
        results[interval] = success
        
        print()
    
    # Print summary
    print("\n\n")
    print("="*70)
    print("TEST SUMMARY")
    print("="*70)
    
    for interval, success in results.items():
        status = "✓ PASSED" if success else "✗ FAILED"
        print(f"  {interval:12s} : {status}")
    
    print("="*70)
    
    all_passed = all(results.values())
    
    if all_passed:
        print("\n✓✓✓ ALL TESTS PASSED! ✓✓✓\n")
    else:
        failed_count = sum(1 for v in results.values() if not v)
        print(f"\n⚠ {failed_count} TEST(S) FAILED ⚠\n")
    
    return all_passed

def run_single_test(interval):
    """Run test for a single interval"""
    
    # Map interval to potential verification file
    verify_files = {
        'minute': '2025-09-22_1510401_minute.jsonl',
        '3minute': '2025-09-22_1510401_3minute.json',
        '5minute': '2025-09-22_1510401_5minute.json',
        '10minute': '2025-09-22_1510401_10minute.json',
        '15minute': '2025-09-22_1510401_15minute.json',
        '30minute': '2025-09-22_1510401_30minute.json',
        '60minute': '2025-09-22_1510401_60minute.json',
        'day': '2025-09-22_1510401_day.json',
    }
    
    verify_file = verify_files.get(interval)
    
    success = run_conversion_test(interval, verify_file)
    
    print()
    print("="*70)
    if success:
        print(f"✓✓✓ {interval.upper()} TEST PASSED! ✓✓✓")
    else:
        print(f"✗✗✗ {interval.upper()} TEST FAILED! ✗✗✗")
    print("="*70)
    
    return success

def main():
    """Main test runner"""
    
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Test tick to OHLC converter across different timeframes',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Test all intervals
  python test_tick_to_ohlc.py
  
  # Test specific interval
  python test_tick_to_ohlc.py --interval minute
  python test_tick_to_ohlc.py --interval 5minute
  python test_tick_to_ohlc.py --interval day
        """
    )
    
    parser.add_argument('--interval',
                       choices=['minute', '3minute', '5minute', '10minute', '15minute', 
                               '30minute', '60minute', 'day', 'all'],
                       default='all',
                       help='Interval to test (default: all)')
    
    args = parser.parse_args()
    
    # Check if tick file exists
    tick_file = "2025-09-22_1510401_tick.jsonl"
    if not Path(tick_file).exists():
        print(f"ERROR: Tick file not found: {tick_file}")
        print(f"Please ensure the file exists in the current directory.")
        return False
    
    if args.interval == 'all':
        success = run_all_tests()
    else:
        success = run_single_test(args.interval)
    
    return success

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)

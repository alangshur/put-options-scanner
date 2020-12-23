from src.executor.scan import ScanExecutor
import argparse
import os


if __name__ == '__main__':

    # build arg parser
    parser = argparse.ArgumentParser(description='Run scanner for puts and calls.')
    parser.add_argument('direction', 
        type=str,
        help='The option type direction in which to aim the scan.'
    )
    parser.add_argument('--pcontract', 
        type=str,
        help='The name of the original put contract to scan calls for.'
    )
    parser.add_argument('--focus', 
        type=str,
        help='The name of the target symbol for focused put contract search.'
    )

    # parse args
    args = parser.parse_args()

    # execute scan
    executor = ScanExecutor()
    if args.direction.lower() in ['put', 'p', 'puts'] and args.focus: executor.run_focus_put_scanner(args.focus)
    elif args.direction.lower() in ['put', 'p', 'puts']: executor.run_put_scanner(ignore_active_tickers=False)
    elif args.direction.lower() in ['call', 'c', 'calls'] and args.pcontract: executor.run_call_scanner(args.pcontract)
    else: parser.error('invalid combination, choose from [put/call] with valid additional argument.')

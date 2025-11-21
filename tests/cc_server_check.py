#!/usr/bin/env python3
"""Check CDX API endpoints from external source (e.g., GitHub action) to detect if fail2ban is working as expected.

NOTE: This is a dedicated script and NOT a unit test.

Usage:

```bash
python tests/cc_server_check.py
```

"""

import requests
import time
import sys
import json
from datetime import datetime
from typing import List, Dict

API_BASE = 'https://index.commoncrawl.org'  # Update with your actual domain
USER_AGENT = 'pypi_cdx_toolkit/fail2ban-monitor'
CRAWL_ID = 'CC-MAIN-2025-43'
DEFAULT_LIMIT = 1

DOMAINS = [
    'blogspot.com',
    'wikipedia.org',
    'wordpress.org',
    'ebay.com',
    'europa.eu',
    'app.link',
    'google.com',
    'wiktionary.org',
    'ning.com',
]  # taken from https://commoncrawl.github.io/cc-crawl-statistics/plots/domains

# Test scenarios that simulate legitimate usage patterns
test_scenarios = [
    {
        'name': 'Single user normal browsing',
        'description': 'Simulates a user making occasional requests',
        'requests': [
            {
                'url': f'{API_BASE}/{CRAWL_ID}-index',
                'params': {'url': 'example.com/*', 'output': 'json', 'limit': DEFAULT_LIMIT},
            },
            {
                'url': f'{API_BASE}/{CRAWL_ID}-index',
                'params': {'url': 'example.org/*', 'output': 'json', 'limit': DEFAULT_LIMIT},
            },
            {
                'url': f'{API_BASE}/{CRAWL_ID}-index',
                'params': {'url': 'example.net/*', 'output': 'json', 'limit': DEFAULT_LIMIT},
            },
        ],
        'delay_between': 2.0,  # seconds
        'should_succeed': True,
    },
    {
        'name': 'Moderate API usage',
        'description': 'Simulates a script making regular requests',
        'requests': [
            {
                'url': f'{API_BASE}/{CRAWL_ID}-index',
                'params': {'url': f'{DOMAINS[i]}/*', 'output': 'json', 'limit': DEFAULT_LIMIT},
            }
            for i in range(8)
        ],
        'delay_between': 8.0,  # 8 requests over ~64 seconds (within cdx limit of 10/60s)
        'should_succeed': True,
    },
    {
        'name': 'Collection info check',
        'description': 'Checking collection info (stricter limits)',
        'requests': [
            {'url': f'{API_BASE}/collinfo.json', 'params': {}},
            {'url': f'{API_BASE}/collinfo.json', 'params': {}},
        ],
        'delay_between': 6.0,  # 2 requests over 6+ seconds (within limit of 3/10s)
        'should_succeed': True,
    },
    {
        'name': 'Edge case - near limit',
        'description': 'Tests behavior near the rate limit threshold',
        'requests': [
            {'url': f'{API_BASE}/cc-index', 'params': {'url': DOMAINS[i], 'output': 'json', 'limit': DEFAULT_LIMIT}}
            for i in range(9)
        ],
        'delay_between': 7.0,  # 9 requests in ~63 seconds (just under 10/60s limit)
        'should_succeed': True,
    },
    {
        'name': 'Burst detection - collinfo',
        'description': 'Tests if legitimate burst triggers ban on collinfo endpoint',
        'requests': [{'url': f'{API_BASE}/collinfo.json', 'params': {}} for _ in range(4)],
        'delay_between': 4.0,  # 4 requests over 8 seconds (WILL trigger ban at 3/10s)
        'should_succeed': False,  # This SHOULD get banned
    },
]


def is_connection_blocked(error_msg: str) -> bool:
    """Determine if an error indicates IP blocking."""
    blocking_indicators = [
        'Connection refused',
        '[Errno 61]',  # macOS/BSD connection refused
        '[Errno 111]',  # Linux connection refused
        'Max retries exceeded',
        'NewConnectionError',
    ]
    return any(indicator in str(error_msg) for indicator in blocking_indicators)


def make_request(url: str, params: Dict, request_num: int) -> Dict:
    """Make a single request and return results."""
    result = {
        'request_num': request_num,
        'timestamp': datetime.now().isoformat(),
        'url': url,
        'params': params,
        'success': False,
        'status_code': None,
        'blocked': False,
        'error': None,
        'response_time': None,
    }

    start_time = time.time()
    try:
        response = requests.get(url, params=params, timeout=15, headers={'User-Agent': 'fail2ban-monitor/1.0'})
        result['response_time'] = time.time() - start_time
        result['status_code'] = response.status_code

        if response.status_code == 200:
            result['success'] = True
        elif response.status_code in [403, 429, 503]:
            result['blocked'] = True
            result['error'] = f'HTTP blocked: {response.status_code}'
        else:
            result['error'] = f'Unexpected status: {response.status_code}'

    except requests.exceptions.Timeout:
        result['error'] = 'Request timeout'
        result['response_time'] = time.time() - start_time
        # Timeout could indicate blocking, but not conclusive
    except requests.exceptions.ConnectionError as e:
        result['response_time'] = time.time() - start_time
        error_str = str(e)
        result['error'] = f'Connection error: {error_str}'
        # Connection refused is a strong indicator of IP blocking
        if is_connection_blocked(error_str):
            result['blocked'] = True
    except requests.exceptions.RequestException as e:
        result['error'] = f'Request error: {str(e)}'

    return result


def run_scenario(scenario: Dict) -> Dict:
    """Run a complete test scenario."""
    print(f'\n{"=" * 70}')
    print(f'Scenario: {scenario["name"]}')
    print(f'Description: {scenario["description"]}')
    print(f'Total requests: {len(scenario["requests"])}')
    print(f'{"=" * 70}')

    results = []
    blocked_at = None

    for i, request in enumerate(scenario['requests'], 1):
        print(f'\n  Request {i}/{len(scenario["requests"])}')
        result = make_request(request['url'], request['params'], i)
        results.append(result)

        if result['success']:
            print(f'    ✓ Success (200) - {result["response_time"]:.2f}s')
        elif result['blocked']:
            print(f'    ✗ BLOCKED - {result["error"]}')
            if result['status_code']:
                print(f'      Status code: {result["status_code"]}')
            blocked_at = i
            # Don't break immediately - log that we're blocked but continue to see pattern
            # Actually, we should break because we can't make more requests
            break
        else:
            print(f'    ⚠ Failed - {result["error"]}')

        # Wait before next request (except after last one or if blocked)
        if i < len(scenario['requests']) and not result['blocked']:
            print(f'    ⏱ Waiting {scenario["delay_between"]}s...')
            time.sleep(scenario['delay_between'])

    # Analyze results
    successful = sum(1 for r in results if r['success'])
    blocked = sum(1 for r in results if r['blocked'])
    failed = len(results) - successful - blocked

    summary = {
        'name': scenario['name'],
        'description': scenario['description'],
        'should_succeed': scenario['should_succeed'],
        'total_requests': len(scenario['requests']),
        'completed_requests': len(results),
        'successful': successful,
        'blocked': blocked,
        'failed': failed,
        'blocked_at_request': blocked_at,
        'unexpected_block': blocked > 0 and scenario['should_succeed'],
        'results': results,
    }

    return summary


def print_summary(all_summaries: List[Dict]):
    """Print overall test summary."""
    print(f'\n\n{"=" * 70}')
    print('OVERALL SUMMARY')
    print(f'{"=" * 70}\n')

    total_scenarios = len(all_summaries)
    problematic_scenarios = []

    for summary in all_summaries:
        # Problematic if: (1) should succeed but got blocked, OR (2) should fail but didn't get blocked
        is_problematic = False

        if summary['should_succeed'] and summary['blocked'] > 0:
            # Should have worked but got blocked = TOO STRICT
            is_problematic = True
            status = '❌ TOO STRICT'
        elif not summary['should_succeed'] and summary['blocked'] == 0:
            # Should have been blocked but wasn't = TOO LENIENT
            is_problematic = True
            status = '❌ TOO LENIENT'
        elif not summary['should_succeed'] and summary['blocked'] > 0:
            # Correctly blocked as expected
            status = '✅ BLOCKED (expected)'
        else:
            # Correctly succeeded
            status = '✅ OK'

        if is_problematic:
            problematic_scenarios.append(summary)

        print(f'{status} {summary["name"]}')
        print(f'     {summary["successful"]}/{summary["completed_requests"]} successful', end='')

        if summary['blocked'] > 0:
            print(f', {summary["blocked"]} blocked at request #{summary["blocked_at_request"]}')
        else:
            print()

        if summary['failed'] > 0:
            print(f'     ⚠ {summary["failed"]} requests failed (non-block errors)')

    print(f'\n{"=" * 70}')
    print(f'Total scenarios: {total_scenarios}')
    print(f'Problematic: {len(problematic_scenarios)}')
    print(f'{"=" * 70}\n')

    if problematic_scenarios:
        print('⚠️  FAIL2BAN CONFIGURATION ISSUES DETECTED ⚠️\n')

        too_strict = [s for s in problematic_scenarios if s['should_succeed'] and s['blocked'] > 0]
        too_lenient = [s for s in problematic_scenarios if not s['should_succeed'] and s['blocked'] == 0]

        if too_strict:
            print('🔒 TOO STRICT - Legitimate usage patterns are being blocked:\n')
            for scenario in too_strict:
                print(f'  • {scenario["name"]}')
                print(f'    Blocked at request {scenario["blocked_at_request"]}/{scenario["total_requests"]}')
            print()

        if too_lenient:
            print('🔓 TOO LENIENT - Abuse patterns are NOT being blocked:\n')
            for scenario in too_lenient:
                print(f'  • {scenario["name"]}')
                print(f'    Completed {scenario["completed_requests"]}/{scenario["total_requests"]} without ban')
            print()

        print('📋 Recommendations:')
        if too_strict:
            print('  - Increase maxretry values')
            print('  - Increase findtime windows')
            print('  - Review filter patterns for false positives')
        if too_lenient:
            print('  - Decrease maxretry values')
            print('  - Decrease findtime windows')
            print('  - Verify fail2ban is running and filters are active')
        return False
    else:
        print('✅ All test scenarios behaved as expected')
        print('   fail2ban rules appear correctly configured')
        return True


def main():
    print(f'Starting fail2ban external monitoring at {datetime.now()}')
    print(f'Target: {API_BASE}\n')

    all_summaries = []

    for scenario in test_scenarios:
        summary = run_scenario(scenario)
        all_summaries.append(summary)

        # If we got blocked, warn and wait longer before next scenario
        if summary['blocked'] > 0:
            print('\n⚠️  IP may be blocked - waiting 60s for potential unban...')
            time.sleep(60)
        else:
            print('\n⏱ Waiting 60s before next scenario...')
            time.sleep(60)

    # Print summary and determine exit code
    success = print_summary(all_summaries)

    # Save detailed results to file for GitHub Actions artifact
    with open('fail2ban_test_results.json', 'w') as f:
        json.dump(
            {
                'timestamp': datetime.now().isoformat(),
                'api_base': API_BASE,
                'overall_success': success,
                'summaries': all_summaries,
            },
            f,
            indent=2,
        )

    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()

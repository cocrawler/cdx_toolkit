import json
import os
from pathlib import Path
import functools
from typing import Dict, Optional
import requests
import responses
import base64

from unittest.mock import patch


TEST_DATA_BASE_PATH = Path(__file__).parent / 'data'


def flexible_param_matcher(expected_params):
    """Custom matcher that ignores dynamic 'from' parameter timestamps and casts all values to strings"""

    def match(request):
        actual_params = dict(request.params or {})
        expected = dict(expected_params or {})

        # Remove 'from' parameter for comparison as it's dynamically generated
        actual_params.pop('from', None)
        expected.pop('from', None)

        # Cast all values to strings for consistent comparison
        # Handle list values specially - if expected is a list with one item, match against the string value
        actual_params_normalized = {}
        expected_normalized = {}

        for k, v in actual_params.items():
            if v is not None:
                actual_params_normalized[k] = str(v)

        for k, v in expected.items():
            # Skip parameters with None values - they represent optional parameters
            if v is None or str(v) == 'None':
                continue
            if isinstance(v, list) and len(v) == 1:
                expected_normalized[k] = str(v[0])
            else:
                expected_normalized[k] = str(v)

        if actual_params_normalized == expected_normalized:
            return True, "Params match (ignoring 'from' parameter)"
        else:
            return (
                False,
                f"Params don't match: {actual_params_normalized} != {expected_normalized}",
            )

    return match


def mock_response_from_jsonl(mock_data_name, mock_data_dir: Optional[str] = None):
    """Load mock response data from JSONL file. Response match based on URL and request params.

    By default, all remote requests are mocked. To change this behaviour and actually call remote APIs
    (if you run this from a whitelisted IP address), the following environment variable can be set:

    ```bash
    export DISABLE_MOCK_RESPONSES=1
    ```

    If the remote APIs change, new mock data can be semi-automatically collected
    by setting another environment variable, running corresponding unit tests,
    and overwriting existing mock data in `tests/data/mock_responses`:

    ```bash
    # set environment variable (DISABLE_MOCK_RESPONSES should not be set)
    export SAVE_MOCK_RESPONSES=./tmp/mock_responses

    # run the test for what mock data should be saved to $SAVE_MOCK_RESPONSES/<test_file>/<test_func>.jsonl
    pytest tests/test_cli.py::test_basics
    ```

    Make sure to empty the cache before collecting mock data (~/.cache/cdx_toolkit/).

    The mock data can then be stored as fixture file in "tests/data/mock_responses/<test module>/<test func>.jsonl".
    """
    mock_data_file_path = TEST_DATA_BASE_PATH / 'mock_responses'

    if mock_data_dir:
        mock_data_file_path = mock_data_file_path / mock_data_dir

    mock_data_file_path = mock_data_file_path / f'{mock_data_name}.jsonl'

    # Read JSONL file
    with open(mock_data_file_path) as f:
        for line in f:
            if line:  # skip empty lines
                mock_data = json.loads(line)

                if mock_data['url'].endswith('.gz'):
                    # decode base64 string to gzipped bytes
                    body = base64.b64decode(mock_data['response_text'].encode('utf-8'))
                else:
                    body = mock_data['response_text']

                headers = mock_data.get('response_headers', {})

                # Remove encoding header
                if 'Content-Encoding' in headers:
                    del headers['Content-Encoding']

                responses.add(
                    mock_data['method'],
                    mock_data['url'],
                    status=mock_data['response_status_code'],
                    match=[flexible_param_matcher(mock_data['request_params'])],
                    body=body,
                    headers=headers,
                )


def conditional_mock_responses(func):
    """Conditionally applies @responses.activate and auto-loads mock data based on DISABLE_MOCK_RESPONSES env var.

    The mock data is automatically loaded from JSONL file from the tests/data directory
    and dependinng on the test module and test function.
    """

    # If the flag DISABLE_MOCK_RESPONSES is not detected, response mocking remains enabled
    if not os.environ.get('DISABLE_MOCK_RESPONSES'):
        # Add responses.activate
        func = add_mock_responses(func)

    if os.environ.get('SAVE_MOCK_RESPONSES'):
        # Mock data is saved by capturing output from requests.get
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            with patch('requests.get', side_effect=_custom_behavior_with_original(requests.get)):
                return func(*args, **kwargs)

        return wrapper

    return func


def save_response_as_mock_data(test_info: str, request_url: str, request_params: Dict, resp, output_base_dir: str):
    """Save request and response for mock data."""
    # Format: "path/to/test_file.py::TestClass::test_method (setup)"
    test_file = Path(test_info.split('::')[0]).stem
    test_name = test_info.split('::')[-1].split()[0]

    output_dir = os.path.join(output_base_dir, test_file)

    # Make sure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Append to JSONL file
    with open(os.path.join(output_dir, f'{test_name}.jsonl'), 'a') as f:
        if request_url.endswith('.gz'):
            # encode bytes as base64 string if file is gzipped
            response_text = base64.b64encode(resp.content).decode('utf-8')
        else:
            response_text = resp.text

        f.write(
            json.dumps(
                {
                    'method': 'GET',
                    'url': request_url,
                    'request_params': request_params,
                    'response_status_code': resp.status_code,
                    'response_text': response_text,
                    'response_headers': dict(resp.headers),
                }
            )
            + '\n'
        )


def _custom_behavior_with_original(original_func):
    def custom_behavior(*args, **kwargs):
        # Call the original "requests.get"
        response = original_func(*args, **kwargs)

        if 'url' in kwargs:
            request_url = kwargs.pop('url')
        else:
            request_url = args[0]

        if 'params' in kwargs:
            request_params = kwargs.pop('params')
        else:
            request_params = args[1]

        # Make sure this is run as a unit test
        if os.environ.get('PYTEST_CURRENT_TEST'):
            save_response_as_mock_data(
                test_info=os.environ['PYTEST_CURRENT_TEST'],
                request_url=request_url,
                request_params=request_params,
                resp=response,
                output_base_dir=os.environ.get('SAVE_MOCK_RESPONSES'),
            )

        return response

    return custom_behavior


def add_mock_responses(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        # Load mock data for index calls (same for many test functions)
        mock_response_from_jsonl('test_get_cc_endpoints', 'test_cc')

        # Auto-load mock data based on function name
        mock_response_from_jsonl(func.__name__, func.__module__.split('.')[-1])
        return func(*args, **kwargs)

    return responses.activate(wrapper)

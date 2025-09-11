import json
import os
from pathlib import Path
import functools
import responses

TEST_DATA_BASE_PATH = Path(__file__).parent / "data"

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
            actual_params_normalized[k] = str(v)
            
        for k, v in expected.items():
            if isinstance(v, list) and len(v) == 1:
                expected_normalized[k] = str(v[0])
            else:
                expected_normalized[k] = str(v)
        
        if actual_params_normalized == expected_normalized:
            return True, "Params match (ignoring 'from' parameter)"
        else:
            return False, f"Params don't match: {actual_params_normalized} != {expected_normalized}"
    return match

def mock_response_from_jsonl(mock_data_name, mock_data_dir: str | None = None):
    """Load mock response data from JSONL file. Response match based on URL and request params.
    
    To collect mock data, add this code snippet to the `myrequests.py` after the requests.get call:

    ```python
    import json
    with open("./request_mock_data.jsonl", "a") as f:
        f.write(json.dumps({
            "method": "GET",
            "url": url, 
            "request_params": params, 
            "response_status_code": resp.status_code,
            "response_text": resp.text,
        }) + "\n")
    ```
    Make sure to empty the cache before collecting mock data (~/.cache/cdx_toolkit/).

    The mock data can then be stored as fixture file in "tests/data/mock_responses/<test module>/<test func>.jsonl".    
    """
    mock_data_file_path = TEST_DATA_BASE_PATH / "mock_responses" 
    
    if mock_data_dir:
        mock_data_file_path = mock_data_file_path / mock_data_dir
    
    mock_data_file_path = mock_data_file_path / f"{mock_data_name}.jsonl"

    # Read JSONL file
    with open(mock_data_file_path) as f:
        for line in f:
            if line:  # skip empty lines
                mock_data = json.loads(line)
                responses.add(
                    mock_data["method"],
                    mock_data["url"],
                    status=mock_data["response_status_code"],
                    match=[flexible_param_matcher(mock_data["request_params"])],
                    body=mock_data["response_text"],
                )


def conditional_mock_responses(func):
    """Conditionally applies @responses.activate and auto-loads mock data based on DISABLE_MOCK_RESPONSES env var.
    
    The mock data is automatically loaded from JSONL file from the tests/data directory and dependinng on the test module and test function.
    """

    if os.environ.get('DISABLE_MOCK_RESPONSES'):
        return func
    
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        # Load mock data for index calls
        mock_response_from_jsonl("cc_collinfo")

        # Auto-load mock data based on function name
        mock_response_from_jsonl(func.__name__, func.__module__.split(".")[-1])
        return func(*args, **kwargs)
    
    return responses.activate(wrapper)
# Contributing to cdx_toolkit

We welcome contributions to cdx_toolkit! Whether you're adding new features, improving documentation, or fixing bugs, your help is greatly appreciated.

## Tests

To test code changes, please run our test suite before submitting pull requests:

```bash
# all tests (same as CI)
make test

# simple unit tests without integration tests
make unit
```

By default, all remote requests are mocked. To change this behaviour and actually call remote APIs (if you run this from a whitelisted IP address), the following environment variable can be set:

```bash
export DISABLE_MOCK_RESPONSES=1
```

If the remote APIs change, new mock data can be semi-automatically collected by setting another environment variable,  running corresponding unit tests, and overwriting existing mock data in `tests/data/mock_responses`:

```bash
# set environment variable (DISABLE_MOCK_RESPONSES should not be set)
export SAVE_MOCK_RESPONSES=./tmp/mock_responses
    
# run the test for what mock data should be saved to $SAVE_MOCK_RESPONSES/<test_file>/<test_func>.jsonl
pytest tests/test_cli.py::test_basics
```

## Code format & linting

Please following the definitions from `.editorconfig` and `.flake8`.
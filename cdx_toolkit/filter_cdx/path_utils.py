import logging
from typing import Optional

import fsspec


import os
import sys


logger = logging.getLogger(__name__)


def resolve_paths(input_base_path: str, input_glob: Optional[str], output_base_path: str):
    """Resolve input paths from glob pattern and generate corresponding output paths."""
    # Use fsspec to handle local and remote file systems
    input_fs, input_fs_base_path = fsspec.url_to_fs(input_base_path)

    if input_glob is None:
        # No glob pattern
        input_fs_file_paths = [input_fs_base_path]
    else:
        input_full_glob = input_fs_base_path + input_glob

        # Get input files from glob pattern
        input_fs_file_paths = sorted(input_fs.glob(input_full_glob))
        if not input_fs_file_paths:
            logger.error(f'No files found matching glob pattern: {input_full_glob}')
            sys.exit(1)

    # Generate corresponding output paths
    output_file_paths = []
    input_file_paths = []
    for input_path in input_fs_file_paths:
        # Get relative path from input_base_path without last slash
        rel_path = input_path[len(input_fs_base_path) + 1 :]

        # Create corresponding full input and output path
        # Use forward slashes for URL paths (S3, HTTP, etc.) to ensure cross-platform compatibility
        if '://' in output_base_path:
            output_file_paths.append(output_base_path + '/' + rel_path)
        else:
            # Normalize path separators for local filesystem
            normalized_rel_path = rel_path.replace('/', os.sep)
            output_file_paths.append(os.path.join(output_base_path, normalized_rel_path))

        if '://' in input_base_path:
            input_file_paths.append(input_base_path + '/' + rel_path)
        else:
            # Normalize path separators for local filesystem
            normalized_rel_path = rel_path.replace('/', os.sep)
            input_file_paths.append(os.path.join(input_base_path, normalized_rel_path))

    return input_file_paths, output_file_paths


def validate_resolved_paths(output_paths, overwrite):
    """Validate resolved output paths and create directories if needed."""
    # Check if output files exist and overwrite flag
    if not overwrite:
        output_fs, _ = fsspec.url_to_fs(output_paths[0])
        for output_path in output_paths:
            if output_fs.exists(output_path):
                logger.error(f'Output file already exists: {output_path}. Use --overwrite to overwrite existing files.')
                sys.exit(1)

            # Make sure directory exists
            output_fs.makedirs(output_fs._parent(output_path), exist_ok=True)

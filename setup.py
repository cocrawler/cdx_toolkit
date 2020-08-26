#!/usr/bin/env python

import sys
from os import path

from setuptools import setup
from setuptools.command.test import test as TestCommand


class PyTest(TestCommand):
    args = ['--doctest-modules', 'cdx_toolkit/', 'tests']
    user_options = [('pytest-args=', 'a', "Arguments to pass into py.test")]
    # python ./setup.py --pytest-args='-v -v'

    def initialize_options(self):
        TestCommand.initialize_options(self)
        self.args = PyTest.args.copy()
        self.pytest_args = ''

    def finalize_options(self):
        TestCommand.finalize_options(self)

    def run_tests(self):
        import pytest
        import shlex
        if self.pytest_args:
            self.args.extend(shlex.split(self.pytest_args))
        errno = pytest.main(self.args)
        sys.exit(errno)


packages = [
    'cdx_toolkit',
]

requires = ['requests', 'warcio']

test_requirements = ['pytest>=3.0.0']  # 'coverage', 'pytest-cov']

scripts = ['scripts/cdx_size', 'scripts/cdx_iter']

this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    description = f.read()

setup(
    name='cdx_toolkit',
    use_scm_version=True,
    description='A toolkit for working with CDX indices',
    long_description=description,
    long_description_content_type='text/markdown',
    author='Greg Lindahl and others',
    author_email='lindahl@pbm.com',
    url='https://github.com/cocrawler/cdx_toolkit',
    packages=packages,
    python_requires=">=3.5.*",
    setup_requires=['setuptools_scm'],
    install_requires=requires,
    entry_points='''
        [console_scripts]
        cdxt = cdx_toolkit.cli:main
        ccathena = cdx_toolkit.cli:main_athena
    ''',
    scripts=scripts,
    license='Apache 2.0',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Intended Audience :: Information Technology',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3 :: Only',
    ],
    cmdclass={'test': PyTest},
    tests_require=test_requirements,
)

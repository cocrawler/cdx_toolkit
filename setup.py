#!/usr/bin/env python

from os import path

from setuptools import setup


packages = [
    'cdx_toolkit',
]

# remember: keep requires synchronized with requirements.txt
requires = ['requests', 'warcio']

test_requirements = ['pytest', 'pytest-cov', 'coveralls']

package_requirements = ['twine', 'setuptools', 'setuptools-scm']

extras_require = {
    'test': test_requirements,  # setup no longer tests, so make them an extra
    'package': package_requirements,
}

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
    python_requires=">=3.6",
    extras_require=extras_require,
    setup_requires=['setuptools-scm'],
    install_requires=requires,
    entry_points='''
        [console_scripts]
        cdxt = cdx_toolkit.cli:main
        #ccathena = cdx_toolkit.cli:main_athena
    ''',
    scripts=scripts,
    license='Apache 2.0',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Environment :: Console',
        'Operating System :: POSIX :: Linux',
        'Environment :: MacOS X',
        'Intended Audience :: Information Technology',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python',
        #'Programming Language :: Python :: 3.5',  # setuptools-scm problem
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3 :: Only',
    ],
)

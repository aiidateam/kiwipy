# -*- coding: utf-8 -*-
from setuptools import setup

__author__ = "Martin Uhrin"
__license__ = "GPLv3 and MIT"
__contributors__ = "Sebastiaan P. Huber"

about = {}
with open('kiwipy/version.py') as f:
    exec(f.read(), about)

setup(
    name="kiwipy",
    version=about['__version__'],
    description='Robust, high-volume, message based communication made easy',
    long_description=open('README.rst').read(),
    url='https://github.com/aiidateam/kiwipy.git',
    author='Martin Uhrin',
    author_email='martin.uhrin.10@ucl.ac.uk',
    license=__license__,
    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: MIT License',
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
    keywords='communication messaging rpc broadcast',
    install_requires=['shortuuid', 'async_generator', 'pytray>=0.2.2, <0.3.0'],
    python_requires=">=3.5",
    extras_require={
        'rmq': ['aio-pika', 'pyyaml~=5.1'],
        'dev': [
            'pip',
            'pre-commit',
            'pytest>=4',
            'pytest-asyncio<=0.10.0',
            'twine',
            'yapf',
            'prospector',
            'pylint',
            'pytest-cov',
            'sphinx',
        ],
        'docs': [
            'nbsphinx',  # Jupyter notebooks in docs
            'notebook < 6.0',  # Jupyter w/asyncio causes problems for examples
            'tornado < 5.0',  # For same reason as above
            'jupyter',  # For running doc examples
            'sphinx-autobuild',
            "docutils==0.14",
            'pandoc'
        ]
    },
    packages=['kiwipy', 'kiwipy.rmq'],
    test_suite='test')

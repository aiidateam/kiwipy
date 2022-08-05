# -*- coding: utf-8 -*-
from setuptools import setup

__author__ = 'Martin Uhrin'
__license__ = 'GPLv3 and MIT'
__contributors__ = ['Sebastiaan P. Huber', 'Jason Yu', 'Sonia Collaud']

ABOUT = {}
with open('kiwipy/version.py') as f:
    exec(f.read(), ABOUT)  # pylint: disable=exec-used

setup(
    name='kiwipy',
    version=ABOUT['__version__'],
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
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
    ],
    keywords='communication messaging rpc broadcast',
    install_requires=['shortuuid', 'async_generator', 'pytray>=0.2.2, <0.4.0', 'deprecation'],
    python_requires='>=3.7',
    extras_require={
        'docs': [
            'docutils==0.14',
            'jupyter',  # For running doc examples
            'nbsphinx',  # Jupyter notebooks in docs
            'pandoc',
            'sphinx',
            'sphinx-autobuild',
        ],
        'pre-commit': ['pre-commit~=2.2', 'pylint==2.5.2'],
        'rmq': ['aio-pika~=6.6,<6.8.2', 'pamqp~=2.0', 'pyyaml~=5.1'],
        'tests': [
            'coverage',
            'ipykernel',
            'pytest-cov',
            'pytest~=5.4',
            'pytest-asyncio~=0.12,<0.17',
            'pytest-notebook>=0.7',
            'pytest-benchmark',
            'pika',
            'msgpack',
        ]
    },
    packages=['kiwipy', 'kiwipy.rmq'],
    test_suite='test'
)

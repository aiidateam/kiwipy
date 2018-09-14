# -*- coding: utf-8 -*-

from __future__ import absolute_import
from setuptools import setup

__author__ = "Martin Uhrin"
__license__ = "GPLv3 and MIT, see LICENSE file"
__contributors__ = "Sebastiaan Huber"

about = {}
with open('kiwipy/version.py') as f:
    exec(f.read(), about)

setup(
    name="kiwipy",
    version=about['__version__'],
    description='A python remote communications library',
    long_description=open('README.md').read(),
    url='https://github.com/muhrin/kiwipy.git',
    author='Martin Uhrin',
    author_email='martin.uhrin@gmail.com',
    license=__license__,
    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: MIT License',
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
    keywords='communication messaging rpc broadcast',
    # Abstract dependencies.  Concrete versions are listed in
    # requirements.txt
    # See https://caremad.io/2013/07/setup-vs-requirement/ for an explanation
    # of the difference and
    # http://blog.miguelgrinberg.com/post/the-package-dependency-blues
    # for a useful dicussion
    install_requires=[
        'tornado', 'future', 'topika', 'six', 'typing; python_version<"3.5"', 'enum34; python_version<"3.4"',
        'backports.tempfile; python_version<"3.2"', 'futures; python_version == "2.7"'
    ],
    extras_require={
        'rmq': ['pika', 'tornado', 'pyyaml'],
        'dev': [
            'pip',
            'pytest',
            'pytest-cov',
            'ipython',
            'twine',
            'shortuuid',
            'yapf',
            'prospector',
            'pylint<2; python_version<"3"',
            'pylint; python_version>="3"',
        ],
    },
    packages=['kiwipy', 'kiwipy.rmq'],
    test_suite='test')

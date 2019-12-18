# -*- coding: utf-8 -*-
from __future__ import absolute_import
from setuptools import setup

__author__ = "Martin Uhrin"
__license__ = "GPLv3 and MIT, see LICENSE file"
__contributors__ = "Sebastiaan Huber"

about = {}
with open('kiwipy/version.py') as f:
    exec(f.read(), about)

setup(name="kiwipy",
      version=about['__version__'],
      description='A python remote communications library',
      long_description=open('README.rst').read(),
      url='https://github.com/muhrin/kiwipy.git',
      author='Martin Uhrin',
      author_email='martin.uhrin@gmail.com',
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
      install_requires=[
          'shortuuid',
      ],
      python_requires=">=2.7,!=3.0.*, !=3.1.*, !=3.2.*, !=3.3.*, !=3.4.*",
      extras_require={
          'rmq': ['aio-pika', 'pyyaml'],
          'dev': [
              'async_generator',
              'pip',
              'pre-commit',
              'pytest>=4',
              'pytest-asyncio',
              'pytest-cov',
              'ipython<6',
              'twine',
              'yapf',
              'prospector',
              'pylint',
          ],
          "docs": [
              "Sphinx==1.8.4",
              "Pygments==2.3.1",
              "docutils==0.14",
          ],
      },
      packages=['kiwipy', 'kiwipy.rmq'],
      test_suite='test')

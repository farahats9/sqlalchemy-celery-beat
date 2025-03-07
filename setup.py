"""
    sqlalchemy-celery-beat
    ~~~~~~~~~~~~~~
    A Scheduler Based SQLalchemy For Celery.
    :Copyright (c) 2023 Mohamed Farahat
    :license: MIT, see LICENSE for more details.
"""
from os import path
from codecs import open
try:
    from setuptools import find_packages, setup
except ImportError:
    from distutils.core import setup, find_packages
# To use a consistent encoding

basedir = path.abspath(path.dirname(__file__))
# Get the long description from the README file
with open(path.join(basedir, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name="sqlalchemy_celery_beat",

    # Versions should comply with PEP440.  For a discussion on single-sourcing
    # the version across setup.py and the project code, see
    # https://packaging.python.org/en/latest/single_source_version.html
    version="0.8.2",
    # The project's main homepage.
    url="https://github.com/farahats9/sqlalchemy-celery-beat",
    # Choose your license

    license='MIT',

    description="A Scheduler Based SQLalchemy For Celery",
    long_description=long_description,
    long_description_content_type='text/markdown',
    platforms='any',
    # Author details
    author="Mohamed Farahat",
    author_email='farahats9@yahoo.com',
    home_page='https://github.com/farahats9/sqlalchemy-celery-beat',

    # See https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 3 - Alpha',

        # Indicate who your project is intended for
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Libraries',

        # Pick your license as you wish (should match "license" above)
        'License :: OSI Approved :: MIT License',

        # Specify the Python versions you support here. In particular, ensure
        # that you indicate whether you support Python 2, Python 3 or both.
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.9',
    ],

    # What does your project relate to?
    keywords="celery scheduler sqlalchemy beat",

    # You can just specify the packages manually here if your project is
    # simple. Or you can use find_packages().
    packages=find_packages(exclude=['contrib', 'docs', 'tests']),

    # Alternatively, if you want to distribute just a my_module.py, uncomment
    # this:
    #   py_modules=["my_module"],

    # List run-time dependencies here.  These will be installed by pip when
    # your project is installed. For an analysis of "install_requires" vs pip's
    # requirements files see:
    # https://packaging.python.org/en/latest/requirements.html
    entry_points={
        'celery.beat_schedulers': [
            'sqlalchemy = sqlalchemy_celery_beat.schedulers:DatabaseScheduler',
        ],
    },
    install_requires=[
        'celery>=5.0',
        'sqlalchemy>=1.4',
        'tzdata',
        'ephem'
    ],
    zip_safe=False,
)

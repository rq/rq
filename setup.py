"""
rq is a simple, lightweight, library for creating background jobs, and
processing them.
"""
import sys
import os
from setuptools import setup, find_packages


def get_version():
    basedir = os.path.dirname(__file__)
    with open(os.path.join(basedir, 'rq/version.py')) as f:
        VERSION = None
        exec(f.read())
        return VERSION
    raise RuntimeError('No version info found.')


def get_dependencies():
    deps = ['redis >= 2.4.13', 'times']
    if sys.version_info < (2, 7) or \
            (sys.version_info >= (3, 0) and sys.version_info < (3, 1)):
        deps += ['importlib']
    if sys.version_info < (2, 7) or \
            (sys.version_info >= (3, 0) and sys.version_info < (3, 2)):
        deps += ['argparse']
    return deps

setup(
    name='rq',
    version=get_version(),
    url='https://github.com/nvie/rq/',
    license='BSD',
    author='Vincent Driessen',
    author_email='vincent@3rdcloud.com',
    description='RQ is a simple, lightweight, library for creating background '
                'jobs, and processing them.',
    long_description=__doc__,
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    platforms='any',
    install_requires=get_dependencies(),
    entry_points='''\
    [console_scripts]
    rqworker = rq.scripts.rqworker:main
    rqinfo = rq.scripts.rqinfo:main
    ''',
    classifiers=[
        # As from http://pypi.python.org/pypi?%3Aaction=list_classifiers
        #'Development Status :: 1 - Planning',
        #'Development Status :: 2 - Pre-Alpha',
        #'Development Status :: 3 - Alpha',
        'Development Status :: 4 - Beta',
        #'Development Status :: 5 - Production/Stable',
        #'Development Status :: 6 - Mature',
        #'Development Status :: 7 - Inactive',
        'Intended Audience :: Developers',
        'Intended Audience :: End Users/Desktop',
        'Intended Audience :: Information Technology',
        'Intended Audience :: Science/Research',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: BSD License',
        'Operating System :: POSIX',
        'Operating System :: MacOS',
        'Operating System :: Unix',
        'Programming Language :: Python',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Internet',
        'Topic :: Scientific/Engineering',
        'Topic :: System :: Distributed Computing',
        'Topic :: System :: Systems Administration',
        'Topic :: System :: Monitoring',

    ]
)

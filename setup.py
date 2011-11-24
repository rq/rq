"""
rq is a simple, lightweight, library for creating background jobs, and
processing them.
"""
from setuptools import Command, setup

setup(
    name='rq',
    version='0.1-dev',
    url='https://github.com/nvie/rq/',
    license='BSD',
    author='Vincent Driessen',
    author_email='vincent@3rdcloud.com',
    description='rq is a simple, lightweight, library for creating background '
                'jobs, and processing them.',
    long_description=__doc__,
    packages=['rq'],
    include_package_data=True,
    zip_safe=False,
    platforms='any',
    install_requires=['redis', 'logbook', 'procname'],
    scripts=['bin/rqinfo', 'bin/rqworker'],
    classifiers=[
        # As from http://pypi.python.org/pypi?%3Aaction=list_classifiers
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ]
)


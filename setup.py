# -*- coding: utf-8 -*-
from setuptools import setup

setup(
    name='rq-scheduler',
    version='0.3.2',
    author='Selwin Ong',
    author_email='selwin.ong@gmail.com',
    packages=['rq_scheduler'],
    url='https://github.com/ui/rq-scheduler',
    license='MIT',
    description='Provides job scheduling capabilities to RQ (Redis Queue)',
    long_description=open('README.rst').read(),
    zip_safe=False,
    include_package_data=True,
    entry_points='''\
    [console_scripts]
    rqscheduler = rq_scheduler.scripts.rqscheduler:main
    ''',
    package_data = { '': ['README.rst'] },
    install_requires=['rq>=0.3'],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ]
)

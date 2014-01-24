#!/usr/bin/env python

from distutils.core import setup

with open('README.rst') as file:
    long_description = file.read()

setup(
    name="stats_consolidation",
    version="2.1.2",
    description="Statistics translator from rrd to relational db",
    long_description=long_description,
    author="Gustavo Duarte",
    author_email="gduarte@activitycentral.com",
    maintainer="Miguel Gonzalez",
    maintainer_email="migonzalvar@activitycentral.com",
    url="http://www.activitycentral.com/",

    scripts=['stats_consolidation_run', 'stats_consolidation_report', ],
    packages=[
        'stats_consolidation',
    ],
)

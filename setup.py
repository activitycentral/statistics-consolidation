#!/usr/bin/env python

from distutils.core import setup

setup(
    name="stats_consolidation",
    version="2.0",
    description="Statistics translator from rrd to relational db",
    author="Gustavo Duarte",
    author_email="gduarte@activitycentral.com",
    maintainer="Miguel Gonzalez",
    maintainer_email="migonzalvar@activitycentral.com",
    url="http://www.activitycentral.com/",

    scripts=['stats_consolidation/stats_consolidation_run',],
    packages=[
        'stats_consolidation',
    ],
)

from distutils.core import setup

setup(
    name = "stats_consolidation",
    version = "0.1.0",
    description = "Statistics translator from rrd to relational db",
    author = "Gustavo Duarte",
    author_email = "gduarte@activitycentral.com",
    url = "http://www.acrtivitycentral.com/",
    packages=[
        'sugar-stats-consolidation',
    ],
    package_dir={'': 'stats_consolidation'}
)


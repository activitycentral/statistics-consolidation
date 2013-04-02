from distutils.core import setup

setup(
    name = "stats_consolidation",
    version = "1.0.0",
    description = "Statistics translator from rrd to relational db",
    author = "Gustavo Duarte",
    author_email = "gduarte@activitycentral.com",
    url = "http://www.acrtivitycentral.com/",
    data_files=[('/usr/local/bin', ['stats_consolidation/stats_consolidation_run']),
		('/etc/cron.d', ['stats-consolidation.cron']),
		('/etc',['stats-consolidation.conf'])
    ],
    packages=[
        'stats_consolidation',
    ],
    package_dir={'': ''}
)


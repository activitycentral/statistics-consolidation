import os.path
import subprocess


BASEDIR = os.path.dirname(__file__)
CMD = 'stats_consolidation_report'
DEFAULT_CONFIG_FILE = "/etc/stats-consolidation.conf"


def test_run_without_args_no_config_exist():
    # When I execute the script without arguments
    # And the default conf file does not exits
    assert os.path.exists(DEFAULT_CONFIG_FILE) == False
    retcode = subprocess.call([os.path.join(BASEDIR, CMD)])

    # Then exits with error
    assert retcode != 0
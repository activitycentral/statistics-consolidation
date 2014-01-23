import os.path
import subprocess
import tempfile


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


def test_run_with_conf_file_args_that_not_exits():
    # When I execute the script with an argument pointing to an non existent file
    conf_file = tempfile.NamedTemporaryFile(delete=True)
    conf_file.close()

    args = ['--config_file', conf_file.name]
    retcode = subprocess.call([os.path.join(BASEDIR, CMD)])

    # Then exits with error
    assert retcode != 0


def test_run_with_conf_file_to_blank_database_outputs_headers():
    # When I execute with a blank database
    conf_file = tempfile.NamedTemporaryFile()

    args = [os.path.join(BASEDIR, CMD), '--config_file', conf_file.name]
    output = subprocess.check_output(args)

    conf_file.close()

    # Then prints one line with the header
    lines = output.strip().split('\n')
    assert len(lines) == 1
    cols = ['usages_ts',
            'usages_user_hash',
            'usages_resource_name',
            'usages_start_date',
            'usages_data_type',
            'usages_data',
            'users_hash',
            'users_uuid',
            'users_machine_sn',
            'users_age',
            'users_school',
            'users_sw_version',
            'users_ts',
            ]
    for col in cols:
        assert col in lines[0]

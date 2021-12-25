import sys
import os
import pytest

from dmsbatch import cli


def test_cli_gen_config():
    if os.path.exists('tests.config'):
        os.remove('tests.config')
    sys.argv = 'dmsbatch gen-config -f tests.config'.split()
    cli.cli()
    assert os.path.isfile('tests.config')
    try:
        cli.cli()
        pytest.fail('should not overwrite file. should throw error')
    except:
        pass

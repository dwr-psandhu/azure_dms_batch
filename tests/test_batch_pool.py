import os
from azure import batch
import pytest
import dmsbatch


@pytest.fixture(scope='session')
def pool_name():
    return 'testdmsbatchpool'


@pytest.fixture(scope='session')
def batch_client() -> dmsbatch.AzureBatch:
    return dmsbatch.create_batch_client('../tests/data/dmsbatch.config')


@pytest.fixture(scope='session')
def batch_client_with_pool(batch_client, pool_name):
    batch_client.create_pool(pool_name, 1)
    batch_client.wait_for_pool_nodes(pool_name)
    batch_client.wait_for_all_nodes_state(pool_name, "idle")
    yield batch_client
    batch_client.delete_pool(pool_name)
    batch_client.wait_for_pool_delete(pool_name)


@pytest.mark.integration
def test_create_delete_pool(batch_client: dmsbatch.AzureBatch, pool_name):
    assert not batch_client.exists_pool(pool_name)
    batch_client.create_pool(pool_name, 1)
    batch_client.wait_for_pool_nodes(pool_name)
    assert batch_client.exists_pool(pool_name)
    batch_client.wait_for_all_nodes_state(pool_name, "idle")
    batch_client.delete_pool(pool_name)
    batch_client.wait_for_pool_delete(pool_name)
    assert not batch_client.exists_pool(pool_name)

@pytest.mark.integration
def test_pool_create_twice(batch_client_with_pool: dmsbatch.AzureBatch, pool_name):
    assert not batch_client_with_pool.create_pool(pool_name, 1)
    assert not batch_client_with_pool.create_or_resize_pool(pool_name, 2)
    nodes = batch_client_with_pool.wait_for_all_nodes_state(pool_name, "idle")
    assert len(nodes) == 2

@pytest.mark.integration
@pytest.mark.parametrize('pool_size', [2, 5, 0])
def test_resize_pool(batch_client_with_pool: dmsbatch.AzureBatch, pool_name, pool_size):
    assert batch_client_with_pool.exists_pool(pool_name)
    batch_client_with_pool.resize_pool(pool_name, pool_size)
    nodes = batch_client_with_pool.wait_for_all_nodes_state(pool_name, "idle")
    assert len(nodes) == pool_size

def test_wrap_commands_in_shell(batch_client: dmsbatch.AzureBatch):
    cmd = batch_client.wrap_commands_in_shell(['ls','cd ~'], ostype='linux')
    assert cmd == '/bin/bash -c \'set -e; set -o pipefail; ls;cd ~; wait\''
    cmd = batch_client.wrap_commands_in_shell(['dir','hostname'], ostype='windows')
    assert cmd == 'cmd.exe /c "dir&hostname"'

def test_wrap_cmd_with_app_path(batch_client: dmsbatch.AzureBatch):
    cmd = batch_client.wrap_cmd_with_app_path('dir',[('pkg1','v1','bin'),('pkg2','v2','bin2')],'windows')
    assert cmd == 'cmd.exe /c "set "PATH=%AZ_BATCH_APP_PACKAGE_pkg1#v1%/bin;%AZ_BATCH_APP_PACKAGE_pkg2#v2%/bin2;%PATH%"&dir"'


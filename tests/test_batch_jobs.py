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
def batch_client_with_pool(batch_client, pool_name) -> dmsbatch.AzureBatch:
    batch_client.create_pool(pool_name, 1)
    batch_client.wait_for_pool_nodes(pool_name)
    batch_client.wait_for_all_nodes_state(pool_name,"idle")
    yield batch_client
    batch_client.delete_pool(pool_name)
    batch_client.wait_for_pool_delete(pool_name)

@pytest.mark.integration
def test_create_delete_job(batch_client: dmsbatch.AzureBatch, pool_name):
    batch_client.create_job('testjob1',pool_name)
    job = batch_client.get_job('testjob1')
    assert job.id == 'testjob1'
    assert job.state.value == 'enabling' or job.state.value == 'active'
    batch_client.delete_job('testjob1')
    with pytest.raises(Exception) as excinfo:
        batch_client.get_job('testjob1')

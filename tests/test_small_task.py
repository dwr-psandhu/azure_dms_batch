"""
    Run a small task by
    * Uploading files
    * Creating a pool and a job
    * Creating a task and submitting it
    * Waiting till end of task
    * Retrieving files and checking
    """
import pytest
import datetime
import dmsbatch
from tests.test_blob_client import container_name

@pytest.fixture()
def container_name():
    return 'testcontainer1'

@pytest.fixture
def blob_client(container_name):
    client =  dmsbatch.create_blob_client('../tests/data/dmsbatch.config')
    try:
        client.delete_container(container_name)
    except:
        pass
    yield client
    try:
        client.delete_container(container_name)
    except:
        pass


@pytest.fixture
def batch_client():
    yield dmsbatch.create_batch_client('../tests/data/dmsbatch.config')

@pytest.mark.integration
def test_small_task(blob_client: dmsbatch.AzureBlob, batch_client: dmsbatch.AzureBatch, container_name):
    pool_name = 'testpool1'
    job_name = 'testjob1'
    task_name = 'testtask1'
    blob_zip = f'{task_name}/dsm2v821.zip'
    app_packages = [
        ('dsm2', '8.2.c5aacef7', 'DSM2-8.2.c5aacef7-win32/bin'),
        ('unzip', '5.51-1', 'bin')]

    blob_client.zip_and_upload(
        container_name, blob_zip, '../tests/data/dsm2v821')
    batch_client.create_pool(pool_name, 1, app_packages=app_packages)
    try:
        batch_client.create_job(job_name, pool_name)
    except dmsbatch.commands.batchmodels.BatchErrorException as ex:
        assert ex.message.value.startswith('The specified job already exists')

    input_file1 = batch_client.create_input_file_spec(container_name, blob_zip, file_path='.')
    # default expiry in one day with write permissions default
    container_sas_url = blob_client.get_container_sas_url(container_name,
        dmsbatch.commands.BlobSasPermissions(write=True))
    save_std_files = batch_client.create_output_file_spec(
        '../std*.txt', container_sas_url, blob_path=task_name)
    save_output_dir = batch_client.create_output_file_spec(
        '**/output/*', container_sas_url, blob_path=task_name)
    cmdline = batch_client.wrap_cmd_with_app_path([
        f'cd {task_name}',
        'unzip dsm2v821.zip',
        'del dsm2v821.zip',
        'cd study_templates/historical',
        'hydro -e hydro.inp',
        'dir'],
        app_packages, ostype='windows')
    task1 = batch_client.create_task(
        task_name,
        cmdline,
        resource_files=[input_file1],
        output_files=[save_std_files, save_output_dir])

    batch_client.submit_tasks(job_name, [task1])
    batch_client.wait_for_pool_nodes(pool_name)
    batch_client.wait_for_tasks_to_complete(job_name, timeout=datetime.timedelta(minutes=2))

    blobs = list(blob_client.list_blobs_from_container(container_name))
    assert len(blobs) == 4 # 1 input file, 2 std* files and the output dir

    batch_client.delete_task(job_name, task_name)
    #blob_client.download_blob_from_container(container_name, task_name, '.')
    batch_client.delete_job(job_name)
    batch_client.delete_pool(pool_name)
    blob_client.delete_container(container_name)
import os
import shutil
import pytest
import filecmp
import dmsbatch

@pytest.fixture(scope='session')
def container_name():
    return 'testdmsbatch'


@pytest.fixture(scope='session')
def blob_client(container_name):
    shutil.rmtree('test.blob', ignore_errors=True)
    client = dmsbatch.create_blob_client('../tests/data/dmsbatch.config')
    try:
        client.delete_container(container_name)
    except:
        pass
    yield client
    try:
        client.delete_container(container_name)
    except:
        pass
    shutil.rmtree('test.blob', ignore_errors=True)

@pytest.mark.integration
def test_upload_and_downloads(blob_client: dmsbatch.AzureBlob, container_name):
    ''' Test upload and downloads to container and blob '''
    assert blob_client
    assert blob_client.storage_account_name
    assert len(blob_client.storage_account_key) > 20  # atleast 20 chars or more
    assert blob_client.storage_account_domain.endswith(
        '.net')  # atleast for the default storage accounts
    # check that container does not exists
    assert not blob_client.exists_container(container_name)
    #
    upload_file = 'test_cli.py'
    blob_client.upload_file_to_container(container_name, upload_file, upload_file)
    # check that container exists now because of the upload
    assert blob_client.exists_container(container_name)
    #
    blob_client.upload_file_to_container(container_name, upload_file + '.copy', upload_file)
    #
    directory_path = 'test.blob'
    shutil.rmtree(directory_path, ignore_errors=True)
    assert not os.path.isdir(directory_path)
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
    blob_client.download_blob_from_container(container_name, upload_file, directory_path)
    assert os.path.isfile(os.path.join(directory_path, upload_file))
    blob_client.download_blob_from_container(container_name, upload_file + '.copy', directory_path)
    assert os.path.isfile(os.path.join(directory_path, upload_file + '.copy'))
    assert filecmp.cmp(upload_file, os.path.join(directory_path, upload_file)
                       )  # 'Uploaded and downloaded file dont match!'
    #
    blob_names = blob_client.list_blobs_from_container(container_name)
    assert len(list(blob_names)) == 2
    blob_client.delete_blob(container_name, upload_file)
    blob_names2 = blob_client.list_blobs_from_container(container_name)
    assert len(list(blob_names2)) == 1
    blob_client.delete_blob(container_name, upload_file + '.copy')
    blob_names3 = blob_client.list_blobs_from_container(container_name)
    assert len(list(blob_names3)) == 0
    # tests uploading zip files
    blob_client.zip_and_upload(container_name, 'testdir.zip', directory_path)
    blob_client.download_blob_from_container(container_name, 'testdir.zip', '.')
    assert os.path.isfile('testdir.zip')
    os.remove('testdir.zip')
    blob_client.delete_blob(container_name, 'testdir.zip')
    # test upload zip file for None as blob name
    blob_client.zip_and_upload(container_name, None, directory_path)
    blob_client.download_blob_from_container(container_name, directory_path + '.zip', '.')
    assert os.path.isfile(directory_path + '.zip')
    os.remove(directory_path + '.zip')
    blob_client.delete_blob(container_name, directory_path + '.zip')
    #
    blob_client.delete_container(container_name)
    assert not blob_client.exists_container(container_name)

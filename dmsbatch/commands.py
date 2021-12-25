# Azure commands to work with batch and associated storage
# Heavily borrowed from azure batch samples in python
import configparser
import datetime
import io
import logging
import os
import shutil
import sys
import math
import time

import azure.batch as batch
import azure.batch.batch_auth as batchauth
import azure.batch.models as batchmodels
from azure.batch import BatchServiceClient
from azure.core.exceptions import ResourceExistsError
from azure.storage import blob
from azure.storage.blob import (BlobSasPermissions, BlobServiceClient,
                                ContainerSasPermissions, generate_account_sas,
                                generate_blob_sas, generate_container_sas)


def generate_blank_config(config_file: str):
    """
    Generate blank config. Replace the angle '<>' brackets and the text within
    them with the appropriate values

    Parameters
    ----------
    config_file : str
        config filename

    Raises
    ------
    Exception if config file already exists. Move it out of the way or generate with another name
    """
    if os.path.exists(config_file):
        raise f'Config file: {config_file} exists! Please delete or move out of the way to generate'
    with open(config_file, 'w') as fh:
        fh.write("""# Update the Batch and Storage account credential strings below with the values
# unique to your accounts. These are used when constructing connection strings
# for the Batch and Storage client objects.
# Replace the <xxxxxx> with actual values from your account

[DEFAULT]
_BATCH_ACCOUNT_NAME = <batch_account_name>
_BATCH_ACCOUNT_KEY = <batch_account_key>
_BATCH_ACCOUNT_URL = https://<batch_account_name>.<location>.batch.azure.com
_STORAGE_ACCOUNT_NAME = <storage_account_name>
_STORAGE_ACCOUNT_KEY = <storage_account_key>
_STORAGE_ACCOUNT_DOMAIN = blob.core.windows.net
        """)


def load_config(config_file: str):
    """
    Loads config file with a 'DEFAULT' section. See configparser

    Config file contains a default section. To generate an empty one use the
    generate_blank_config(config_file) method

    Parameters
    ----------
    config_file : str
        Filename

    Returns
    -------
    config: dict
        configuration name value pairs
    """
    parser = configparser.ConfigParser()
    parser.optionxform = str
    parser.read(config_file)
    config = dict(parser['DEFAULT'].items())
    return config


def create_batch_client(config_file: str):
    """
    Create a batch client

    Parameters
    ----------
    config_file : str
        filename

    Returns
    -------
    AzureBatch
        a configured instance of the class for working with the batch account
    """
    config = load_config(config_file)
    # Create a Batch service client. We'll now be interacting with the Batch
    return AzureBatch(config['_BATCH_ACCOUNT_NAME'], config['_BATCH_ACCOUNT_KEY'], config['_BATCH_ACCOUNT_URL'])


def create_blob_client(config_file: str):
    """
    Create a blob client

    Parameters
    ----------
    config_file : str
        filename

    Returns
    -------
    AzureBlob
        a configured instance for working with the storage 'blob' account
    """
    config = load_config(config_file)
    # Create the blob client, for use in obtaining references to
    # blob storage containers and uploading files to containers.
    return AzureBlob(config['_STORAGE_ACCOUNT_NAME'], config['_STORAGE_ACCOUNT_KEY'], config['_STORAGE_ACCOUNT_DOMAIN'])


class AzureBatch:
    """
    AzureBatch manages batch pools, jobs and task submissions. This class encapsulates some sensible defaults for typical
    cpu intensive tasks

    Management of batch accounts, application packages are either done manually or separately by other scripts

    The config file with the needed values are defined in a config file use :py:func:`generate_blank_config`
    """
    _STANDARD_OUT_FILE_NAME = 'stdout.txt'
    _STANDARD_ERROR_FILE_NAME = 'stderr.txt'

    def __init__(self, batch_account_name: str, batch_account_key: str, batch_account_url: str):
        """
        Initializes the batch account client

        Parameters
        ----------
        batch_account_name : str
            batch account name
        batch_account_key : str
            batch account key
        batch_account_url : str
            batch accont url
        """
        self.credentials = batchauth.SharedKeyCredentials(batch_account_name,
                                                    batch_account_key)
        self.batch_client = BatchServiceClient(
            self.credentials,
            batch_url=batch_account_url)

    def create_pool_if_not_exist(self, pool: batchmodels.PoolAddParameter) -> bool:
        """
        Creates the pool if it doesn't exist. Otherwise throws an exception that is caught and returned as a bool

        Parameters
        ----------
        pool : batchmodels.PoolAddParameter
            pool to add

        -------
        Returns

        bool
            True if created else False
        """
        try:
            logging.info("Attempting to create pool:", pool.id)
            self.batch_client.pool.add(pool)
            logging.info("Created pool:", pool.id)
            return True
        except batchmodels.BatchErrorException as e:
            if e.error.code != "PoolExists":
                raise
            else:
                logging.info("Pool {!r} already exists".format(pool.id))
                return False

    def resize_pool(self, pool_id: str, pool_size: int, node_deallocation_option='taskCompletion'):
        """
        Resize pool with pool_id to pool_size. This method starts the resizing but that could take a couple of minutes 
        to finish. See :py:func:`wait_for_pool_nodes`

        Parameters
        ----------
        pool_id : str
        pool_size : int
        node_deallocation_option : str, optional
        by default 'taskCompletion' so that pool nodes are shutdown only after current tasks on it run to completion
        """
        pool_resize_param = batchmodels.PoolResizeParameter(
            target_dedicated_nodes=pool_size,
            node_deallocation_option=node_deallocation_option)  # scale it down to zero
        self.batch_client.pool.resize(pool_id, pool_resize_param)

    def wait_for_pool_nodes(self, pool_id: str):
        """
        wait for pool nodes to get to a stable state ( could be idle or unusable )
        Typically you want to do this if you want nodes available before tasks are assigned to the pool

        Parameters
        ----------
        pool_id : str
            pool name

        Raises
        ------
        RuntimeError
            if something goes wrong
        """
        nodes = self.wait_for_all_nodes_state(
            pool_id,
            frozenset(
                (batchmodels.ComputeNodeState.start_task_failed,
                batchmodels.ComputeNodeState.unusable,
                batchmodels.ComputeNodeState.idle)
            )
        )
        # ensure all node are idle
        if any(node.state != batchmodels.ComputeNodeState.idle for node in nodes):
            raise RuntimeError('node(s) of pool {} not in idle state'.format(
                pool_id))

    def create_pool(self, pool_id: str, pool_size: int,
            vm_size='standard_f2s_v2',
            tasks_per_vm=2,
            os_image_data=('microsoftwindowsserver', 'windowsserver', '2019-datacenter-core'),
            app_packages=[],
            start_task_cmd="cmd /c set",
            start_task_admin=False,
            resource_files=None,
            elevation_level=batchmodels.ElevationLevel.admin,
            enable_inter_node_communication=False,
            wait_for_success=False):
        """
        Create or if exists then resize pool to desired pool_size
        The vm_size should be selected based on the kind of workload. For cpu intensive tasks, the F or D series work well.
        These `benchmarks<https://docs.microsoft.com/en-us/azure/virtual-machines/windows/compute-benchmark-scores>`_ are useful in determining
        the ones with the fastest CPU
        This has to be combined with the temporary space available in different VMs otherwise disk storage has to be separately mounted and paid for
        The pricing and storage is available by browsing `these pages<https://azure.microsoft.com/en-us/pricing/details/virtual-machines/windows/>`_

        Parameters
        ----------
        pool_id : str
            the name of the pool
        pool_size : int
            the size of the pool, i.e. number of nodes (machines) in the pool. These are of the on-demand pricing type
        vm_size : str, optional
            the name of the vm type, by default 'standard_f2s_v2'. See `this<https://docs.microsoft.com/en-us/azure/virtual-machines/sizes>`_
        tasks_per_vm : int, optional
            This is the number of tasks that can be run simultaneously. one can under or oversubscribe this relative to the cpus available in the vm_size, by default 2
        os_image_data : tuple, optional
            The `os image list <https://docs.microsoft.com/en-us/azure/batch/batch-pool-vm-sizes#supported-vm-images>`_, by default ('microsoftwindowsserver', 'windowsserver', '2019-datacenter-core')
        app_packages : list, optional
            list of tuples of (app name, app version), by default []
        start_task_cmd : str, optional
            command to be run on start of node, by default "cmd /c set"
        start_task_admin : bool, optional
            whether task should be run as admin, by default False
        resource_files : list of ResourceFile, optional
            input file spec list. See :py:func:create_input_file_spec, by default None
        elevation_level : admin or non_admin, optional
            admin or non_admin, by default batchmodels.ElevationLevel.admin
        enable_inter_node_communication : bool, optional
            if the task needs high bandwidth (Infiniband) connected nodes, by default False
        wait_for_success : bool, optional
            wait for pool to be created, by default False

        Returns
        -------
        bool
            True if pool is created, False otherwise
        """
        vm_count = pool_size
        # choosing windows machine here (just the core windows, it has no other apps on it including explorer)
        sku_to_use, image_ref_to_use = self.select_latest_verified_vm_image_with_node_agent_sku(
            *os_image_data)
        # applications needed here
        app_references = [batchmodels.ApplicationPackageReference(
            application_id=app[0], version=app[1]) for app in app_packages]
        if start_task_admin:
            user_identity = batchmodels.UserIdentity(
                auto_user=batchmodels.AutoUserSpecification(
                    scope=batchmodels.AutoUserScope.pool,
                    elevation_level=elevation_level))
        else:
            user_identity = batchmodels.UserIdentity()
        pool = batchmodels.PoolAddParameter(
            id=pool_id,
            virtual_machine_configuration=batchmodels.VirtualMachineConfiguration(
                image_reference=image_ref_to_use,
                node_agent_sku_id=sku_to_use),
            vm_size=vm_size,
            target_dedicated_nodes=vm_count,
            # not understood but carried from an example maybe outdated ?
            #max_tasks_per_node=1 if enable_inter_node_communication else tasks_per_vm,
            task_slots_per_node=1 if enable_inter_node_communication else tasks_per_vm,
            resize_timeout=datetime.timedelta(minutes=15),
            enable_inter_node_communication=enable_inter_node_communication,
            application_package_references=app_references,
            start_task=batchmodels.StartTask(command_line=start_task_cmd,
                                            user_identity=user_identity,
                                            wait_for_success=wait_for_success,
                                            resource_files=resource_files) if start_task_cmd else None
        )
        pool_created = self.create_pool_if_not_exist(pool)
        return pool_created

    def create_or_resize_pool(self, pool_id, pool_size,
            vm_size='standard_f4s_v2',
            tasks_per_vm=2,
            os_image_data=('microsoftwindowsserver', 'windowsserver', '2019-datacenter-core'),
            app_packages=[],
            start_task_cmd="cmd /c set",
            start_task_admin=False,
            resource_files=None,
            elevation_level=batchmodels.ElevationLevel.admin,
            enable_inter_node_communication=False,
            wait_for_success=False):
        """Create or if exists then resize pool to desired pool_size

        Args:
            pool_id (str): pool id
            pool_size (int): pool size in number of vms (cores per vm may depend on machine type here)
            vm_size: name of vm, default standard_f4s_v2
            tasks_per_vm (default 4): this is tied to the number of cores on the vm_size above. if your task needs 1 cpu per task set this to number of cores
        """
        pool_created = self.create_pool(pool_id, pool_size,
            vm_size,
            tasks_per_vm,
            os_image_data,
            app_packages,
            start_task_cmd,
            start_task_admin,
            resource_files,
            elevation_level,
            enable_inter_node_communication,
            wait_for_success)
        if not pool_created:
            self.resize_pool(pool_id, pool_size)

    def exists_pool(self, pool_id: str) -> bool:
        """
        checks if pool exists

        Parameters
        ----------
        pool_id : str
            name of pool
        Returns
        -------
        bool
            True if pool exists
        """
        return self.batch_client.pool.exists(pool_id)

    def delete_pool(self, pool_id: str):
        """
        asks pool to be deleted. The pool delete will take some time as the nodes have to be shutdown, etc.

        Parameters
        ----------
        pool_id : str
            name of pool
        Returns
        -------

        """
        return self.batch_client.pool.delete(pool_id)

    def wait_for_pool_delete(self, pool_id: str, polling_interval_secs=10):
        """
        wait for pool to be deleted. Polls with :py:func:`exists_pool` every polling_interval_secs (10 is default)

        Parameters
        ----------
        pool_id : str
            name of pool
        polling_interval_secs : int, optional
            polling interval by default 10
        """
        while self.exists_pool(pool_id):
            time.sleep(polling_interval_secs)

    def create_job(self, job_id: str, pool_id: str, prep_task: batchmodels.JobPreparationTask = None):
        """
        Creates a job with the specified ID, associated with the specified pool.

        Parameters
        ----------
        job_id : str
            name of job
        pool_id : str
            name of pool
        prep_task : batchmodels.JobPreparationTask, optional
            a preperation task to be run before any tasks, by default None
        """
        logging.info('Creating job [{}]...'.format(job_id))

        job = batch.models.JobAddParameter(
            id=job_id,
            job_preparation_task=prep_task,
            pool_info=batch.models.PoolInformation(pool_id=pool_id))

        self.batch_client.job.add(job)

    def get_job(self, job_id: str) -> batchmodels.CloudJob:
        """
        get job with matching id

        Parameters
        ----------
        job_id : str
            job id

        Returns
        -------
        batchmodels.CloudJob

        """
        return self.batch_client.job.get(job_id)

    def delete_job(self, job_id: str):
        """
        deletes the job

        Parameters
        ----------
        job_id : str
            job id
        """
        self.batch_client.job.delete(job_id)

    def wait_for_job_under_job_schedule(self, job_schedule_id: str,
            timeout: datetime.timedelta,
            polling_interval_secs: int = 10) -> batchmodels.CloudJob:
        """
        Waits for a job schedule to run

        Parameters
        ----------
        job_schedule_id : str
            job schedule id
        timeout : datetime.timedelta
            how long to wait 
        polling_interval_secs: int
            how often to poll

        Returns
        -------
        [type]
            [description]

        Raises
        ------
        TimeoutError
            [description]
        """
        time_to_timeout_at = datetime.datetime.now() + timeout

        while datetime.datetime.now() < time_to_timeout_at:
            cloud_job_schedule = self.batch_client.job_schedule.get(
                job_schedule_id=job_schedule_id)

            logging.info("Checking if job exists...")
            if (cloud_job_schedule.execution_info.recent_job) and (
                    cloud_job_schedule.execution_info.recent_job.id is not None):
                return cloud_job_schedule.execution_info.recent_job.id
            time.sleep(polling_interval_secs)

        raise TimeoutError("Timed out waiting for tasks to complete")

    def wait_for_job_schedule_to_complete(self, job_schedule_id: str, timeout: datetime.timedelta, polling_interval_secs: int = 10):
        """
        Waits for a job schedule to complete.

        Parameters
        ----------
        job_schedule_id : str
        timeout : datetime.timedelta
            how long to wait
        polling_interval_secs : int, optional
            how often to poll
        """
        while datetime.datetime.now() < timeout:
            cloud_job_schedule = self.batch_client.job_schedule.get(
                job_schedule_id=job_schedule_id)

            logging.info("Checking if job schedule is complete...")
            state = cloud_job_schedule.state
            if state == batchmodels.JobScheduleState.completed:
                return
            time.sleep(polling_interval_secs)
        return

    def create_input_file_spec(self, container_name: str, blob_prefix: str, file_path: str = '.'):
        """
        input file specs are information for the batch task to download these files
        to the task before starting the task. 

        Parameters
        ----------
        container_name : str
            name of container
        blob_prefix : str
            path to blob 
        file_path : str, optional
            where to place the contents of the blob_prefix; appends this value to the blob_prefix, by default '.'

        Returns
        -------
        ResourceFile
            :py:func:`batch.models.ResourceFile`
        """
        return batchmodels.ResourceFile(
            auto_storage_container_name=container_name,
            blob_prefix=blob_prefix,
            file_path=file_path)

    def create_output_file_spec(self, file_pattern: str, output_container_sas_url: str, blob_path: str = '.') -> batchmodels.OutputFile:
        """
        create an output file spec that is information for uploading the output of the task matching the file_pattern to be 
        uploaded to the container as defined by the output_container_sas_url and the blob_path

        Parameters
        ----------
        file_pattern : str
            Matching patterns to upload, e.g. ../std*.txt or **/output
        output_container_sas_url : str
            The container sas url to which to upload the matching file patterns. See :py:func:`AzureBlob.get_container_sas_url`
        blob_path : str, optional
            the blob_path within the output container where to place the matching files, by default '.'

        Returns
        -------
        batchmodels.OutputFile
            [description]
        """
        return batchmodels.OutputFile(
            file_pattern=file_pattern,
            destination=batchmodels.OutputFileDestination(
                container=batchmodels.OutputFileBlobContainerDestination(
                    container_url=output_container_sas_url, path=blob_path)),
            upload_options=batchmodels.OutputFileUploadOptions(
                upload_condition=batchmodels.OutputFileUploadCondition.task_success)
        )

    def create_prep_task(self, task_name: str, commands: str,
            resource_files: list = None, ostype: str = 'windows') -> batchmodels.JobPreparationTask:
        """
        Creates a task to run on a node before any tasks for a job are run. This creates the the task that is then 
        used to create a job with this prep task specified. See :py:func:`create_job`

        Parameters
        ----------
        task_name : str
            Name of task
        commands : str
            commands to run. See :py:func:`wrap_commands_in_shell`
        resource_files : list, optional
            list of :py:func:'batchmodels.ResourceFile`, by default None
        ostype : str, optional
            name of os, either 'windows' or 'linux', by default 'windows'

        Returns
        -------
        batchmodels.JobPreparationTask
        """
        cmdline = self.wrap_commands_in_shell(ostype, commands)
        prep_task = batchmodels.JobPreparationTask(
            id=task_name,
            command_line=cmdline,
            resource_files=resource_files,
            wait_for_success=True)

        return prep_task

    def create_task_copy_file_to_shared_dir(self, container: str, blob_path: str, file_path: str,
            shared_dir: str = 'AZ_BATCH_NODE_SHARED_DIR', ostype: str = 'windows') -> batchmodels.JobPreparationTask:
        """
        A special job prep task for the common use case of copying file from container blob to shared directory on node.
        This is designed to be run as preperation task for a job to have this shared file available to the tasks that will subsequently run 
        on the node

        Parameters
        ----------
        container : str
            Name of container in storage associate with the batch account
        blob_path : str
            Path to the blob within the container
        file_path : str
            Path to file on node
        shared_dir : str, optional
            share directory on node, by default 'AZ_BATCH_NODE_SHARED_DIR'
        ostype : str, optional
            'windows' or 'linux', by default 'windows'

        Returns
        -------
        batchmodels.JobPreparationTask
            the preperation task
        """
        input_file = batchmodels.ResourceFile(
            auto_storage_container_name=container,
            blob_prefix=blob_path,
            file_path=file_path)

        cmdline = ''
        if ostype == 'windows':
            cmdline = f'move {file_path}\\{blob_path} %AZ_BATCH_NODE_SHARED_DIR%'
        else:
            cmdline = f'mv {file_path}/{blob_path}' + ' ${AZ_BATCH_NODE_SHARED_DIR}'

        prep_task = self.create_prep_task('copy_file_task', [cmdline], resource_files=[
                                          input_file], ostype=ostype)
        return prep_task

    def create_task(self, task_id: str, command: str, resource_files: list = None, output_files: list = None, env_settings: dict = None,
            elevation_level: str = None, num_instances: int = 1, coordination_cmdline: str = None, coordination_files: list = None):
        """
        Create a task for the given input_file, command, output file specs and environment settings.
        You need to add :py:func:'submit_task' to send it to batch service to run.

        To build a command line, use :py:func:`wrap_commands_in_shell`

        To build resource_files or coordination_files (batchmodels.ResourceFile) use :py:func:`create_input_file_spec`
        To build output_files use :py:func:`create_output_file_spec`


        Parameters
        ----------
        task_id : str
            The ID of the task to be added.
        command : str
            command line for the application. 
        resource_files : list, optional
            list of input files to be downloaded before running task. batchmodels.ResourceFile
        output_files : list, optional
            patterns of output files and containers to upload to defined as batchmodels.OutputFileSpecs, default None
        env_settings : dict, optional
            environment variables as key (name) and values (value), by default None
        elevation_level : str, optional
            either 'admin' or 'non_admin'
        num_instances : int, optional
            The number of instances of this task (usually = 1 ), unless using MPI
        coordination_cmdline : str, optional
            coordination command line, usually for MP tasks
        coordination_files : list, optional
            list of common_files as batchmodels.ResourceFile, by default None

        Returns
        -------
        batchmodels.TaskAddParameter
            The task definition 
        """
        environment_settings = None if env_settings is None else [
            batch.models.EnvironmentSetting(name=key, value=env_settings[key]) for key in env_settings]
        multi_instance_settings = None
        if coordination_cmdline or (num_instances and num_instances > 1):
            multi_instance_settings = batchmodels.MultiInstanceSettings(
                number_of_instances=num_instances,
                coordination_command_line=coordination_cmdline,
                common_resource_files=coordination_files)
        user = batchmodels.AutoUserSpecification(
            scope=batchmodels.AutoUserScope.pool,
            elevation_level=elevation_level)
        return batchmodels.TaskAddParameter(
                id=task_id,
                command_line=command,
                user_identity=batchmodels.UserIdentity(auto_user=user),
                resource_files=resource_files,
                environment_settings=environment_settings,
                output_files=output_files,
                multi_instance_settings=multi_instance_settings
            )

    def submit_tasks(self, job_id: str, tasks: list, tasks_per_request: int = 100):
        """
        submit tasks as a list. 
        There are limitations on size of request and also timeout. For this reason this task 
        submits tasks upto task_per_request

        Parameters
        ----------
        job_id : str
            job id
        tasks : list
            list of batchmodels.TaskAddParameter. See :py:func:`create_task`
        tasks_per_request : int, optional
            tasks per request (grouped requests), by default 100
        """
        for i in range(0, math.ceil(len(tasks) / tasks_per_request)):
            try:
                self.batch_client.task.add_collection(job_id, list(
                    tasks[i * tasks_per_request:i * tasks_per_request + tasks_per_request]))
            except batchmodels.BatchErrorException as err:
                self.print_batch_exception(err)
                raise

    def submit_tasks_and_wait(self, job_id: str, tasks: list,
            tasks_per_request: int = 100,
            timeout: datetime.timedelta = datetime.timedelta(minutes=30), polling_interval_secs: int = 10):
        """
        submit tasks as a list. 
        There are limitations on size of request and also timeout. For this reason this task 
        submits tasks upto task_per_request

        Parameters
        ----------
        job_id : str
            job id
        tasks : list
            list of batchmodels.TaskAddParameter. See :py:func:`create_task`
        tasks_per_request : int, optional
            tasks per request (grouped requests), by default 100
        timeout : datetime.timedelta, 
            how long to wait in minutes, by default 30 minutes
        polling_interval_secs:
            how often to check, by default 10 seconds
        """
        try:
            self.submit_tasks(self.batch_client, job_id, tasks)
            # Pause execution until tasks reach Completed state.
            self.wait_for_tasks_to_complete(job_id,
                                            timeout, polling_interval_secs=polling_interval_secs)
            logging.info("Success! All tasks completed within the timeout period:",
                         timeout)
        except batchmodels.BatchErrorException as err:
            self.print_batch_exception(err)
            raise

    def delete_task(self, job_name: str, task_name: str):
        """
        deletes tasks

        Parameters
        ----------
        job_name : str
            job name
        task_name : str
            task name
        """
        self.batch_client.task.delete(job_name, task_name)

    def wait_for_subtasks_to_complete(
            self, job_id: str, task_id: str,
            timeout: datetime.timedelta, polling_interval_secs: int = 10):
        """
        Returns when all subtasks in the specified task reach the Completed state.

        Parameters
        ----------
        job_id : str
            job id
        task_id : str
            task id
        timeout : datetime.timedelta
            how long to wait
        polling_interval_secs : int, optional
            how often to check, by default 10 secs

        Raises
        ------
        RuntimeError
            If couldn't complete in timeout interval
        """
        timeout_expiration = datetime.datetime.now() + timeout

        logging.debug("Monitoring all tasks for 'Completed' state, timeout in {}..."
            .format(timeout), end='')

        while datetime.datetime.now() < timeout_expiration:
            subtasks = self.batch_client.task.list_subtasks(job_id, task_id)
            incomplete_subtasks = [subtask for subtask in subtasks.value if
                                subtask.state !=
                                batchmodels.TaskState.completed]
            if not incomplete_subtasks:
                return True
            else:
                time.sleep(polling_interval_secs)

        raise RuntimeError(
            "ERROR: Subtasks did not reach 'Completed' state within "
            "timeout period of " + str(timeout))

    def wait_for_tasks_to_complete(self, job_id: str,
            timeout: datetime.timedelta = datetime.timedelta(minutes=10),
            polling_interval_secs: int = 10):
        """
         Returns when all tasks in the specified job reach the Completed state.

        Parameters
        ----------
        job_id : str
            job id
        timeout : datetime.timedelta, optional
            how long to wait, by default datetime.timedelta(minutes=10)
        polling_interval_secs : int, optional
            how often to check, by default 10

        Raises
        ------
        RuntimeError
            if tasks in job not completed in the specified timeout
        """
        timeout_expiration = datetime.datetime.now() + timeout

        while datetime.datetime.now() < timeout_expiration:
            tasks = self.batch_client.task.list(job_id)

            incomplete_tasks = []
            for task in tasks:
                if task.state == batchmodels.TaskState.completed:
                    # Pause execution until subtasks reach Completed state.
                    incomplete_tasks.append(self.wait_for_subtasks_to_complete(job_id,
                                                task.id,
                                                datetime.timedelta(minutes=10), polling_interval_secs=polling_interval_secs))
                else:
                    incomplete_tasks.append(False)
            if not all(incomplete_tasks):
                time.sleep(polling_interval_secs)
            else:
                return True
        raise RuntimeError("ERROR: Tasks did not reach 'Completed' state within "
                        "timeout period of " + str(timeout))

    def wait_for_all_nodes_state(self, pool_id: str, node_state: list,
            polling_interval_secs: int = 10):
        """
        Waits for all nodes in pool to reach any specified state in set

        Parameters
        ----------
        pool_id : str
            pool id
        node_state : str
            `node state <https://docs.microsoft.com/en-us/python/api/azure-batch/azure.batch.models.computenodestate?view=azure-python>`_
        polling_interval_secs : int, optional
            how often to check, by default 10 secs

        Returns
        -------
        list
            list of compute nodes :py:func:`batchmodels.ComputeNode`

        Raises
        ------
        RuntimeError
            incase the nodes don't achieve the desired state within the timeout
        """
        logging.info('waiting for all nodes in pool {} to reach one of: {!r}'.format(
            pool_id, node_state))
        i = 0
        while True:
            # refresh pool to ensure that there is no resize error
            pool = self.batch_client.pool.get(pool_id)
            if pool.allocation_state == 'steady':
                if pool.resize_errors is not None:
                    resize_errors = "\n".join([repr(e) for e in pool.resize_errors])
                    raise RuntimeError(
                        'resize error encountered for pool {}:\n{}'.format(
                            pool.id, resize_errors))
                nodes = list(self.batch_client.compute_node.list(pool.id))
                if (len(nodes) >= pool.target_dedicated_nodes and
                        all(node.state in node_state for node in nodes)):
                    return nodes
                i += 1
                if i % 3 == 0:
                    logging.info('waiting for {} nodes to reach desired state...'.format(
                        pool.target_dedicated_nodes))
            time.sleep(polling_interval_secs)

    def print_batch_exception(self, batch_exception: Exception):
        """
        Prints the contents of the specified Batch exception.

        Parameters
        ----------
        batch_exception : Exception

        """
        logging.info('-------------------------------------------')
        logging.info('Exception encountered:')
        if (batch_exception.error and batch_exception.error.message and
                batch_exception.error.message.value):
            logging.info(batch_exception.error.message.value)
            if batch_exception.error.values:
                logging.info()
                for mesg in batch_exception.error.values:
                    logging.info('{}:\t{}'.format(mesg.key, mesg.value))
        logging.info('-------------------------------------------')

    def wrap_commands_in_shell(self, commands, ostype: str = 'windows') -> str:
        """
        Wrap commands in a shell

        Parameters
        ----------
        commands : str or list
            command as string or list of strings to wrap
        ostype : str, optional
            'windows' or 'linux', by default 'windows'

        Returns
        -------
        str
            The wrapped command in a shell

        Raises
        ------
        ValueError
            if ostype is unknown
        """
        if ostype.lower() == 'linux':
            return '/bin/bash -c \'set -e; set -o pipefail; {}; wait\''.format(
                ';'.join(commands))
        elif ostype.lower() == 'windows':
            return 'cmd.exe /c "{}"'.format('&'.join(commands))
        else:
            raise ValueError('unknown ostype: {}'.format(ostype))

    def set_path_to_apps(self, apps: list, ostype='windows') -> str:
        """
        create cmd to set path to apps binary locations

        Parameters
        ----------
        apps : list
             apps is a list of tuples (app_name, app_version, binary_location)
        ostype : str, optional
            'windows' or 'linux', by default 'windows'

        Returns
        -------
        str
            windows command line to set PATH envvar

        Example
        -------

        app_pkgs = [('dsm2', '8.2.c5aacef7', 'DSM2-8.2.c5aacef7-win32/bin'),
                    ('vista', '1.0-v2019-05-28', 'bin'),
                    ('unzip', '5.51-1', 'bin')]
        set_path_to_apps(app_pkgs)

        `
        'set "PATH=%AZ_BATCH_APP_PACKAGE_dsm2#8.2.c5aacef7%/DSM2-8.2.c5aacef7-win32/bin;%AZ_BATCH_APP_PACKAGE_vista#1.0-v2019-05-28%/bin;%AZ_BATCH_APP_PACKAGE_unzip#5.51-1%/bin;%PATH%"'
        `
        """
        if ostype == 'windows':
            app_loc_var = ['%AZ_BATCH_APP_PACKAGE_{app_name}#{app_version}%/{bin_loc}'.format(
                app_name=name, app_version=version, bin_loc=bin_loc) for name, version, bin_loc in apps]
            env_var_path = ";".join(app_loc_var)
            cmd = 'set "PATH={env_var_path};%PATH%"'.format(env_var_path=env_var_path)
        elif ostype == 'linux':
            app_loc_var = ['${' + 'AZ_BATCH_APP_PACKAGE_{app_name}_{app_version}{brace}/{bin_loc}'.format(
                app_name=name.replace('.', '_'), app_version=version.replace('.', '_'), brace='}', bin_loc=bin_loc) for name, version, bin_loc in apps]
            env_var_path = ":".join(app_loc_var)
            cmd = "export PATH={env_var_path}:$PATH".format(env_var_path=env_var_path)
        else:
            raise Exception(f'unknown ostype: {ostype}')
        return cmd

    def wrap_cmd_with_app_path(self, cmd, app_pkgs: list, ostype='windows'):
        """
        wraps cmd with a shell and set to the application packages paths before calling this cmd string

        Parameters
        ----------
        cmd : str or list
            command as string or list of strings to wrap
        apps : list
             apps is a list of tuples (app_name, app_version, binary_location)
        ostype : str, optional
            'windows' or 'linux', by default 'windows'

        Returns
        -------
        str
            commands wrapped in a shell with a set PATH to the app packages

        Raises
        ------
            exception if ostype is not supported

        Example
        -------

        wrap_cmd_with_app_path('hydro -v', app_pkgs)

        `
        'cmd /c set "PATH=%AZ_BATCH_APP_PACKAGE_dsm2#8.2.c5aacef7%/DSM2-8.2.c5aacef7-win32/bin;%AZ_BATCH_APP_PACKAGE_vista#1.0-v2019-05-28%/bin;%AZ_BATCH_APP_PACKAGE_unzip#5.51-1%/bin;%PATH%" & hydro -v'
        `
        """
        set_path_cmd = self.set_path_to_apps(app_pkgs, ostype=ostype)
        if isinstance(cmd, str):
            cmds = [set_path_cmd, cmd]
        elif isinstance(cmd, list):
            cmds = [set_path_cmd, *cmd]
        else:
            raise "Unknown type of cmd : {}".format(type(cmd))

        return self.wrap_commands_in_shell(cmds, ostype)

    def skus_available(self, filter="verificationType eq 'verified'"):
        """
        Shows the VM SKUs available matching the filter

        Parameters
        ----------
        filter : str, optional
            filter, by default "verificationType eq 'verified'"

        Returns
        -------
        list
            skus available for usage
        """
        options = batchmodels.AccountListSupportedImagesOptions(
            filter=filter)
        images = self.batch_client.account.list_supported_images(
            account_list_supported_images_options=options)
        skus_to_use = [(image.node_agent_sku_id, image.image_reference.publisher,
                        image.image_reference.offer, image.image_reference.sku) for image in images]
        return skus_to_use

    def select_latest_verified_vm_image_with_node_agent_sku(
            self, publisher: str, offer: str, sku_starts_with: str, filter="verificationType eq 'verified'"):
        """
        Select the latest verified image that Azure Batch supports given
        a publisher, offer and sku (starts with filter).

        Parameters
        ----------
        publisher : str
            vm image publisher
        offer : str
            vm image offer
        sku_starts_with : str
            sku_starts_with: vm sku starts with filter
        filter : str, optional
            filter, by default "verificationType eq 'verified'"

        Returns
        -------
        tuple
            (node agent sku id to use, vm image ref to use)
        """
        # get verified vm image list and node agent sku ids from service
        options = batchmodels.AccountListSupportedImagesOptions(filter=filter)
        images = self.batch_client.account.list_supported_images(
            account_list_supported_images_options=options)
        # pick the latest supported sku
        skus_to_use = [
            (image.node_agent_sku_id, image.image_reference) for image in images
            if image.image_reference.publisher.lower() == publisher.lower() and
            image.image_reference.offer.lower() == offer.lower() and
            image.image_reference.sku.startswith(sku_starts_with)
        ]

        # pick first
        agent_sku_id, image_ref_to_use = skus_to_use[0]
        logging.info(agent_sku_id, image_ref_to_use)
        return (agent_sku_id, image_ref_to_use)

    def print_task_output(self, job_id: str, task_ids: list, encoding: str = None):
        """
        Prints the stdout and stderr for each task specified.

        Parameters
        ----------
        job_id : str
            job id
        task_ids : list
            The collection of tasks to print the output for.
        encoding : str, optional
            The encoding to use when downloading the file, by default None
        """
        for task_id in task_ids:
            file_text = self.read_task_file_as_string(
                job_id,
                task_id,
                AzureBatch._STANDARD_OUT_FILE_NAME,
                encoding)
            logging.info("{} content for task {}: ".format(
                AzureBatch._STANDARD_OUT_FILE_NAME,
                task_id))
            logging.info(file_text)

            file_text = self.read_task_file_as_string(
                job_id,
                task_id,
                AzureBatch._STANDARD_ERROR_FILE_NAME,
                encoding)
            logging.info("{} content for task {}: ".format(
                AzureBatch._STANDARD_ERROR_FILE_NAME,
                task_id))
            logging.info(file_text)

    def print_configuration(self, config: str):
        """
        Prints the configuration being used as a dictionary

        Parameters
        ----------
        config : str
            The configuration.
        """
        configuration_dict = {s: dict(config.items(s)) for s in
                            config.sections() + ['DEFAULT']}

        logging.info("Configuration is:")
        logging.info(configuration_dict)

    def _read_stream_as_string(self, stream, encoding: str = 'utf-8'):
        """
        Read stream as string

        Parameters
        ----------
        stream : 
            input stream generator
        encoding : str, optional
            The encoding of the file., by default 'utf-8'

        Returns
        -------
        str
            The file content

        Raises
        ------
        RuntimeError
            if unsuccessful
        """

        output = io.BytesIO()
        try:
            for data in stream:
                output.write(data)
            if encoding is None:
                encoding = 'utf-8'
            return output.getvalue().decode(encoding)
        finally:
            output.close()
        raise RuntimeError('could not write data to stream or decode bytes')

    def read_task_file_as_string(
            self, job_id: str, task_id: str, file_name: str, encoding: str = None):
        """
        Reads the specified file as a string.

        Parameters
        ----------
        job_id : str
            job id
        task_id : str
            task id
        file_name : str
            file name
        encoding : str, optional
            the encoding, by default None (or utf-8)

        Returns
        -------
        str
            The file content
        """
        stream = self.batch_client.file.get_from_task(job_id, task_id, file_name)
        return self._read_stream_as_string(stream, encoding)

    def read_compute_node_file_as_string(
            self, pool_id: str, node_id: str, file_name: str, encoding: str = None):
        """
        Reads the specified file as a string.

        Parameters
        ----------
        pool_id : str
            pool id
        node_id : str
            node id
        file_name : str
            file name
        encoding : str, optional
            file encoding, by default None (utf-8)

        Returns
        -------
        str
            The file content.
        """
        stream = self.batch_client.file.get_from_compute_node(
            pool_id, node_id, file_name)
        return self._read_stream_as_string(stream, encoding)
#################################################################################


class AzureBlob:
    """
    Azure Blob service client. Gets SAS tokens and URLs for uploading and downloading files and directories to blobs
    """

    def __init__(self, storage_account_name: str, storage_account_key: str, storage_account_domain: str):
        """
        Create the blob client, for use in obtaining references to blob storage containers and uploading files to containers.

        Parameters
        ----------
        storage_account_name : str
            storage account name, e.g. mystorageaccount
        storage_account_key : str
            storage account key, e.g. dkkd3l==3ldldkdk....... 
        storage_account_domain : str
            storage account domain, e.g. blob.core.windows.net
        """
        # C
        self.blob_client = BlobServiceClient(
        account_url="https://{}.{}/".format(storage_account_name, storage_account_domain),
        credential=storage_account_key
        )
        self.storage_account_name = storage_account_name
        self.storage_account_key = storage_account_key
        self.storage_account_domain = storage_account_domain

    def get_container_sas_token(self, container_name: str, permission=BlobSasPermissions(write=True),
            expiry: datetime = None, timeout=datetime.timedelta(days=1)) -> str:
        """
        get token for container with permissions (write is default) which expires after expiry date or after timeout.
        Note: If expiry is non None, timeout is ignored

        Parameters
        ----------
        container_name : str
            container name
        permission : azure.storage.blob.BlobSasPermissions, optional
            by default BlobSasPermissions(write=True)
        expiry : datetime, optional
            a datetime (UTC) at which this token expires, by default None so the timeout is used
            if expiry is specified, timeout is ignored
        timeout : datetime.timedelta, optional
            timeout interval from currrent time, by default datetime.timedelta(days=1)

        Returns
        -------
        str
            shared access token with expiration
        """
        # Obtain the SAS token for the container.ontainer_client(container_name)
        if expiry == None:  # if expiry not defined, use timeout
            expiry = datetime.datetime.utcnow() + timeout
        return generate_container_sas(self.storage_account_name, container_name,
                               account_key=self.storage_account_key, permission=permission, expiry=expiry)

    def get_container_sas_url(self,
                            container_name: str,
                            permission=BlobSasPermissions(write=True),
                            expiry: datetime = None,
                            timeout=datetime.timedelta(days=1)) -> str:
        """
        Obtains a shared access signature URL that provides write (default permission) access to the container 
        with an expiry or timeout from current time (expiry takes precedence)

        Parameters
        ----------
        container_name : str
            container name
        permission : azure.storage.blob.BlobSasPermissions, optional
            by default BlobSasPermissions(write=True)
        expiry : datetime, optional
            a datetime (UTC) at which this token expires, by default None so the timeout is used
            if expiry is specified, timeout is ignored
        timeout : datetime.timedelta, optional
            timeout interval from currrent time, by default datetime.timedelta(days=1)

        Returns
        -------
        str
            sas url with the permissions and expiry
        """
        # Obtain the SAS token for the container.
        sas_token = self.get_container_sas_token(
            container_name, permission=permission, expiry=expiry, timeout=timeout)

        # Construct SAS URL for the container
        container_sas_url = "https://{}.blob.core.windows.net/{}?{}".format(
            self.storage_account_name, container_name, sas_token)

        return container_sas_url

    def create_container_and_create_sas(
                self, container_name: str,
            permission=BlobSasPermissions(write=True),
            expiry: datetime = None,
            timeout=datetime.timedelta(days=1)) -> str:
        """
        Creates a container if it doesn't exist and returns a SAS url with given permissions and expiry to it

        Parameters
        ----------
        container_name : str
            container name
        permission : azure.storage.blob.BlobSasPermissions, optional
            by default BlobSasPermissions(write=True)
        expiry : datetime, optional
            a datetime (UTC) at which this token expires, by default None so the timeout is used
            if expiry is specified, timeout is ignored
        timeout : datetime.timedelta, optional
            timeout interval from currrent time, by default datetime.timedelta(days=1)

        Returns
        -------
        str
            sas url with the permissions and expiry
        """
        if expiry is None:
            if timeout is None:
                timeout = 1
            expiry = datetime.datetime.utcnow() + datetime.timedelta(days=timeout)

        self.blob_client.create_container(
            container_name,
            fail_on_exist=False)

        self.get_container_sas_url(container_name, permission=permission, expiry=expiry)

    def get_blob_sas_token(self,
                            container_name: str, blob_name: str,
                            permission=BlobSasPermissions(read=True),
                            expiry: datetime = None,
                            timeout=datetime.timedelta(days=1)) -> str:
        """
        Obtains a shared access signature granting the specified permissions to the
        container.

        Parameters
        ----------
        container_name : str
        blob_name : str
        permission : azure.storage.blob.BlobSasPermissions(read=True), optional
           by default BlobSasPermissions(read=True)
        expiry : datetime, optional
            a datetime (UTC) at which this token expires, by default None so the timeout is used
            if expiry is specified, timeout is ignored
        timeout : datetime.timedelta, optional
            timeout interval from currrent time, by default datetime.timedelta(days=1)

        Returns
        -------
        str
            sas token with the permissions and expiry
        """
        blob_sas_token = generate_blob_sas(
            self.storage_account_name,
            container_name,
            blob_name,
            account_key=self.storage_account_key,
            permission=permission,
            expiry=expiry,
            timeout=timeout
        )

        return blob_sas_token

    def get_blob_sas_url(self, container_name: str, blob_name: str,
                        permission=BlobSasPermissions(read=True),
                        expiry: datetime = None,
                        timeout=datetime.timedelta(days=1)) -> str:
        """
        Obtains a shared access signature granting the specified permissions to the
        container.

        Parameters
        ----------
        container_name : str
        blob_name : str
        permission : azure.storage.blob.BlobSasPermissions(read=True), optional
            by default BlobSasPermissions(read=True)
        expiry : datetime, optional
            a datetime (UTC) at which this token expires, by default None so the timeout is used
            if expiry is specified, timeout is ignored
        timeout : datetime.timedelta, optional
            timeout interval from currrent time, by default datetime.timedelta(days=1)

        Returns
        -------
        str
            sas url with the permissions and expiry
        """
        sas_token = self.get_blob_sas_token(
            container_name, blob_name, permission=permission, expiry=expiry, timeout=timeout)

        sas_url = "https://{}.{}/{}/{}?{}".format(
            self.storage_account_name,
            self.storage_account_domain,
            container_name,
            blob_name,
            sas_token
        )

        return sas_url

    def upload_blob_and_create_sas(
            self, container_name: str, blob_name: str, file_name: str,
            expiry: datetime = None,
            timeout=datetime.timedelta(days=1),
            max_concurrency=2):
        """
        Uploads a file from local disk to Azure Storage and creates
        a SAS for it.

        Parameters
        ----------
        container_name : str
        blob_name : str
        file_name : str
         the local file to upload
        expiry : datetime, optional
            a datetime (UTC) at which this token expires, by default None so the timeout is used
            if expiry is specified, timeout is ignored
        timeout : datetime.timedelta, optional
            timeout interval for sas url from currrent time, by default datetime.timedelta(days=1)
        max_concurrency : int, optional
            the number of concurrent connections to upload, higher will be faster, by default 2

        Returns
        -------
        str
            sas url with the permissions and expiry

        """
        try:
            self.blob_client.create_container(container_name)
        except ResourceExistsError as exc:
            pass
        local_blob_client = self.blob_client.get_blob_client(container_name, blob_name)
        try:
            with open(file_name, "rb") as data:
                local_blob_client.upload_blob(
                    data, overwrite=True, timeout=1800, max_concurrency=max_concurrency)
                # timeout
        except ResourceExistsError:
            pass

        return self.get_blob_sas_url(container_name, blob_name, expiry, timeout)

    def upload_file_to_container(
            self, container_name: str, blob_name: str, file_name: str,
            expiry: datetime = None,
            timeout=datetime.timedelta(days=1),
            max_concurrency=2):
        """
        Uploads a local file to an Azure Blob storage container.

        Parameters
        ----------
        container_name : str
        blob_name : str
        file_name : str
         the local file to upload
        expiry : datetime, optional
            a datetime (UTC) at which this token expires, by default None so the timeout is used
            if expiry is specified, timeout is ignored
        timeout : datetime.timedelta, optional
            timeout interval from currrent time, by default datetime.timedelta(days=1)
        max_concurrency : int, optional
            the number of concurrent connections to upload, higher will be faster, by default 2

        Returns
        -------
        str
            sas url with the permissions and expiry
        """
        logging.info('Uploading file {} to container [{}]...'.format(
            file_name, container_name))
        sas_url = self.upload_blob_and_create_sas(
            container_name, blob_name, file_name, expiry=expiry,
            timeout=timeout, max_concurrency=max_concurrency)
        return sas_url

    def zip_and_upload(self, container_name: str, blob_name: str, dirname: str,
            expiry: datetime = None,
            timeout=datetime.timedelta(days=1),
            max_concurrency=2):
        """
        Zips and uploads to the container with the blob_name appended to the dirname(basename) +.zip

        Parameters
        ----------
        container_name : str
            name of container,  (will be created if it doesn't exist)
        blob_name : str
        dirname : str
            path to the directory
        expiry : datetime, optional
            a datetime (UTC) at which this token expires, by default None so the timeout is used
            if expiry is specified, timeout is ignored
        timeout : datetime.timedelta, optional
            timeout interval from currrent time, by default datetime.timedelta(days=1)
        max_concurrency : int, optional
            the number of concurrent connections to upload, higher will be faster, by default 2

        Returns
        -------
        str
            sas url with the permissions and expiry
        """
        try:
            zipfile = os.path.basename(os.path.normpath(dirname))
            shutil.make_archive(zipfile, 'zip', dirname)
            zipfname = zipfile + '.zip'
            if blob_name is None:
                blob_name = zipfname
            return self.upload_file_to_container(container_name, blob_name, zipfname,
                expiry=expiry, timeout=timeout, max_concurrency=max_concurrency)
        finally:
            os.remove(zipfname)

    def download_blob_from_container(
            self, container_name:str, blob_name:str, directory_path:str):
        """
        Downloads specified blob from the specified Azure Blob storage container.

        Parameters
        ----------
        container_name : str
        blob_name : str
        directory_path : str
        """            
        logging.info('Downloading result file from container [{}]...'.format(
            container_name))

        destination_file_path = os.path.join(directory_path, blob_name)
        local_blob_client = self.blob_client.get_blob_client(container_name, blob_name)

        with open(destination_file_path, "wb") as my_blob:
            blob_data = local_blob_client.download_blob()
            blob_data.readinto(my_blob)

        logging.info('  Downloaded blob [{}] from container [{}] to {}'.format(
            blob_name, container_name, destination_file_path))

        logging.info('  Download complete!')
    #

    def delete_blob(self, container_name:str, blob_name:str):
        """
        Deletes the blob

        Parameters
        ----------
        container_name : str
        blob_name : str
        """        
        local_blob_client = self.blob_client.get_blob_client(container_name, blob_name)
        local_blob_client.delete_blob()

    def list_blobs_from_container(self, container_name):
        container_client = self.blob_client.get_container_client(container_name)
        return container_client.list_blobs()

    def delete_container(self, container_name):
        container_client = self.blob_client.get_container_client(container_name)
        return container_client.delete_container()

    def exists_container(self, container_name):
        container_client = self.blob_client.get_container_client(container_name)
        return container_client.exists()


def query_yes_no(question:str, default="yes") -> str:
    """
    Prompts the user for yes/no input, displaying the specified question text.

    Parameters
    ----------
    question : str
        The text of the prompt for input.
    default : str, optional
        The default if the user hits <ENTER>. Acceptable values
        are 'yes', 'no', and None., by default "yes"

    Returns
    -------
    str
        'yes' or 'no'

    Raises
    ------
    ValueError
    """    
    valid = {'y': 'yes', 'n': 'no'}
    if default is None:
        prompt = ' [y/n] '
    elif default == 'yes':
        prompt = ' [Y/n] '
    elif default == 'no':
        prompt = ' [y/N] '
    else:
        raise ValueError("Invalid default answer: '{}'".format(default))

    while 1:
        choice = input(question + prompt).lower()
        if default and not choice:
            return default
        try:
            return valid[choice[0]]
        except (KeyError, IndexError):
            print("Please respond with 'yes' or 'no' (or 'y' or 'n').\n")

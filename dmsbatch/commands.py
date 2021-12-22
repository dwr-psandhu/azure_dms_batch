# Azure commands to work with batch and associated storage
# Heavily borrowed from azure batch samples in python
import configparser
import datetime
import shutil
import sys
import os
import io
import time
import logging
import config

import azure.storage.blob as azureblob
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions
import azure.batch as batch
from azure.batch import BatchServiceClient
import azure.batch.batch_auth as batchauth
import azure.batch.models as batchmodels
from azure.core.exceptions import ResourceExistsError

def query_yes_no(question, default="yes"):
    """
    Prompts the user for yes/no input, displaying the specified question text.

    :param str question: The text of the prompt for input.
    :param str default: The default if the user hits <ENTER>. Acceptable values
    are 'yes', 'no', and None.
    :rtype: str
    :return: 'yes' or 'no'
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



def generate_blank_config(config_file):
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

_BATCH_ACCOUNT_NAME = <batch_account_name>
_BATCH_ACCOUNT_KEY = <batch_account_key>
_BATCH_ACCOUNT_URL = https://<batch_account_name>.<location>.batch.azure.com
_STORAGE_ACCOUNT_NAME = <storage_account_name>
_STORAGE_ACCOUNT_KEY = <storage_account_key>
        """)


def create_batch_client(BATCH_ACCOUNT_NAME=config._BATCH_ACCOUNT_NAME,
        BATCH_ACCOUNT_KEY=config._BATCH_ACCOUNT_KEY,
        BATCH_ACCOUNT_URL=config._BATCH_ACCOUNT_URL):
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
    return AzureBatch(BATCH_ACCOUNT_NAME,BATCH_ACCOUNT_KEY,BATCH_ACCOUNT_URL)

#def create_blob_client(config_file):
def create_blob_client(
        STORAGE_ACCOUNT_NAME=config._STORAGE_ACCOUNT_NAME,
        STORAGE_ACCOUNT_KEY=config._STORAGE_ACCOUNT_KEY,
        STORAGE_ACCOUNT_DOMAIN=config._STORAGE_ACCOUNT_DOMAIN):
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
    return AzureBlob(STORAGE_ACCOUNT_NAME,STORAGE_ACCOUNT_KEY,STORAGE_ACCOUNT_DOMAIN)


"""
AzureBatch manages batch pools, jobs and task submissions along with
uploading input files and specifiying output files and environment variables needed to run batch jobs on Azure

Management of batch accounts, application packages are either done manually or separately by other scripts

The config file with the needed values are defined in a config file ( see configparser module )

"""


class AzureBatch:
    _STANDARD_OUT_FILE_NAME = 'stdout.txt'
    _STANDARD_ERROR_FILE_NAME = 'stderr.txt'

    def __init__(self, batch_account_name, batch_account_key, batch_account_url):
        self.credentials = batchauth.SharedKeyCredentials(batch_account_name,
                                                    batch_account_key)
        self.batch_client = BatchServiceClient(
            self.credentials,
            batch_url=batch_account_url)
        
    def submit_tasks_and_wait(self, job_id, tasks, wait_time_mins=30, poll_secs=5):
        self.submit_tasks_and_wait(job_id,
                                    tasks, wait_time_mins=wait_time_mins, poll_secs=poll_secs)

    def create_or_resize_pool(self, pool_id, pool_size,
            vm_size='standard_f4s_v2',
            tasks_per_vm=4,
            os_image_data=('microsoftwindowsserver', 'windowsserver', '2019-datacenter-core'),
            app_packages=[('dsm2', '8.2.1')],
            start_task_cmd="cmd /c set"):
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
            start_task_cmd)
        if not pool_created:
            self.resize_pool(pool_id, pool_size)

    def create_pool_and_wait_for_vms(
            self, pool_id,
            publisher, offer, sku, vm_size,
            target_dedicated_nodes,
            command_line=None, resource_files=None,
            elevation_level=batchmodels.ElevationLevel.admin):
        """
        Creates a pool of compute nodes with the specified OS settings.

        :param batch_service_client: A Batch service client.
        :type batch_service_client: `azure.batch.BatchServiceClient`
        :param str pool_id: An ID for the new pool.
        :param str publisher: Marketplace Image publisher
        :param str offer: Marketplace Image offer
        :param str sku: Marketplace Image sku
        :param str vm_size: The size of VM, eg 'Standard_A1' or 'Standard_D1' per
        https://azure.microsoft.com/en-us/documentation/articles/
        virtual-machines-windows-sizes/
        :param int target_dedicated_nodes: Number of target VMs for the pool
        :param str command_line: command line for the pool's start task.
        :param list resource_files: A collection of resource files for the pool's
        start task.
        :param str elevation_level: Elevation level the task will be run as;
            either 'admin' or 'nonadmin'.
        """
        print('Creating pool [{}]...'.format(pool_id))

        # Create a new pool of Linux compute nodes using an Azure Virtual Machines
        # Marketplace image. For more information about creating pools of Linux
        # nodes, see:
        # https://azure.microsoft.com/documentation/articles/batch-linux-nodes/

        # Get the virtual machine configuration for the desired distro and version.
        # For more information about the virtual machine configuration, see:
        # https://azure.microsoft.com/documentation/articles/batch-linux-nodes/
        sku_to_use, image_ref_to_use = \
            self.select_latest_verified_vm_image_with_node_agent_sku(
                publisher, offer, sku)
        user = batchmodels.AutoUserSpecification(
            scope=batchmodels.AutoUserScope.pool,
            elevation_level=elevation_level)
        new_pool = batch.models.PoolAddParameter(
            id=pool_id,
            virtual_machine_configuration=batchmodels.VirtualMachineConfiguration(
                image_reference=image_ref_to_use,
                node_agent_sku_id=sku_to_use),
            vm_size=vm_size,
            target_dedicated_nodes=target_dedicated_nodes,
            resize_timeout=datetime.timedelta(minutes=15),
            enable_inter_node_communication=True,
            start_task=batch.models.StartTask(
                command_line=command_line,
                user_identity=batchmodels.UserIdentity(auto_user=user),
                wait_for_success=True,
                resource_files=resource_files) if command_line else None,
        )
       
        self.create_pool_if_not_exist(new_pool)

        # because we want all nodes to be available before any tasks are assigned
        # to the pool, here we will wait for all compute nodes to reach idle
        nodes = self.wait_for_all_nodes_state(
            new_pool,
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

    def add_task(
            self, job_id, task_id, num_instances,
            application_cmdline, input_files, elevation_level,
            output_file_names, output_container_sas,
            coordination_cmdline, common_files):
        """
        Adds a task for each input file in the collection to the specified job.

        :param batch_service_client: A Batch service client.
        :type batch_service_client: `azure.batch.BatchServiceClient`
        :param str job_id: The ID of the job to which to add the task.
        :param str task_id: The ID of the task to be added.
        :param str application_cmdline: The application commandline for the task.
        :param list input_files: A collection of input files.
        :param elevation_level: Elevation level used to run the task; either
        'admin' or 'nonadmin'.
        :type elevation_level: `azure.batch.models.ElevationLevel`
        :param int num_instances: Number of instances for the task
        :param str coordination_cmdline: The application commandline for the task.
        :param list common_files: A collection of common input files.
        """

        print('Adding {} task to job [{}]...'.format(task_id, job_id))

        multi_instance_settings = None
        if coordination_cmdline or (num_instances and num_instances > 1):
            multi_instance_settings = batchmodels.MultiInstanceSettings(
                number_of_instances=num_instances,
                coordination_command_line=coordination_cmdline,
                common_resource_files=common_files)
        user = batchmodels.AutoUserSpecification(
            scope=batchmodels.AutoUserScope.pool,
            elevation_level=elevation_level)
        output_file = batchmodels.OutputFile(
            file_pattern=output_file_names,
            destination=batchmodels.OutputFileDestination(
                container=batchmodels.OutputFileBlobContainerDestination(
                    container_url=output_container_sas)),
            upload_options=batchmodels.OutputFileUploadOptions(
                upload_condition=batchmodels.
                OutputFileUploadCondition.task_completion))
        task = batchmodels.TaskAddParameter(
            id=task_id,
            command_line=application_cmdline,
            user_identity=batchmodels.UserIdentity(auto_user=user),
            resource_files=input_files,
            multi_instance_settings=multi_instance_settings,
            output_files=[output_file])
        self.batch_client.task.add(job_id, task)

    def wait_for_subtasks_to_complete(
            self, job_id, task_id, timeout):
        """
        Returns when all subtasks in the specified task reach the Completed state.

        :param batch_service_client: A Batch service client.
        :type batch_service_client: `azure.batch.BatchServiceClient`
        :param str job_id: The id of the job whose tasks should be to monitored.
        :param str task_id: The id of the task whose subtasks should be monitored.
        :param timedelta timeout: The duration to wait for task completion. If all
        tasks in the specified job do not reach Completed state within this time
        period, an exception will be raised.
        """
        timeout_expiration = datetime.datetime.now() + timeout

        print("Monitoring all tasks for 'Completed' state, timeout in {}..."
            .format(timeout), end='')

        while datetime.datetime.now() < timeout_expiration:
            print('.', end='')
            sys.stdout.flush()

            subtasks = self.batch_client.task.list_subtasks(job_id, task_id)
            incomplete_subtasks = [subtask for subtask in subtasks.value if
                                subtask.state !=
                                batchmodels.TaskState.completed]

            if not incomplete_subtasks:
                print()
                return True
            else:
                time.sleep(10)

        print()
        raise RuntimeError(
            "ERROR: Subtasks did not reach 'Completed' state within "
            "timeout period of " + str(timeout))

    def wait_for_tasks_to_complete(self, job_id, timeout):
        """
        Returns when all tasks in the specified job reach the Completed state.

        :param batch_service_client: A Batch service client.
        :type batch_service_client: `azure.batch.BatchServiceClient`
        :param str job_id: The id of the job whose tasks should be to monitored.
        :param timedelta timeout: The duration to wait for task completion. If all
        tasks in the specified job do not reach Completed state within this time
        period, an exception will be raised.
        """
        timeout_expiration = datetime.datetime.now() + timeout

        print("Monitoring all tasks for 'Completed' state, timeout in {}..."
            .format(timeout), end='')

        while datetime.datetime.now() < timeout_expiration:
            print('.', end='')
            sys.stdout.flush()
            tasks = self.batch_client.task.list(job_id)

            incomplete_tasks = [task for task in tasks if
                                task.state != batchmodels.TaskState.completed]
            if not incomplete_tasks:
                print()
                return True
            else:
                time.sleep(10)

        print()
        raise RuntimeError("ERROR: Tasks did not reach 'Completed' state within "
                        "timeout period of " + str(timeout))
                           
                       
    def wait_for_all_nodes_state(self, pool_id, node_state):
        """Waits for all nodes in pool to reach any specified state in set

        :param batch_client: The batch client to use.
        :type batch_client: `batchserviceclient.BatchServiceClient`
        :param pool: The pool containing the node.
        :type pool: `batchserviceclient.models.CloudPool`
        :param set node_state: node states to wait for
        :rtype: list
        :return: list of `batchserviceclient.models.ComputeNode`
        """
        print('waiting for all nodes in pool {} to reach one of: {!r}'.format(
            pool_id, node_state))
        i = 0
        while True:
            # refresh pool to ensure that there is no resize error
            pool = self.batch_client.pool.get(pool_id)
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
                print('waiting for {} nodes to reach desired state...'.format(
                    pool.target_dedicated_nodes))
            time.sleep(10)
    
    
    def create_pool(self, pool_id, pool_size,
            vm_size='standard_f4s_v2',
            tasks_per_vm=4,
            os_image_data=('microsoftwindowsserver', 'windowsserver', '2019-datacenter-core'),
            app_packages=[('dsm2', '8.2.1')],
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

    def resize_pool(self, pool_id, pool_size):
        pool_resize_param = batchmodels.PoolResizeParameter(
            target_dedicated_nodes=pool_size)  # scale it down to zero
        self.batch_client.pool.resize(pool_id, pool_resize_param)

    def wait_for_pool_nodes(self, pool_id):
        # because we want all nodes to be available before any tasks are assigned
        # to the pool, here we will wait for all compute nodes to reach idle
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

    def create_pool_if_not_exist(self, pool):
        """Creates the specified pool if it doesn't already exist

        :param batch_client: The batch client to use.
        :type batch_client: `batchserviceclient.BatchServiceClient`
        :param pool: The pool to create.
        :type pool: `batchserviceclient.models.PoolAddParameter`
        """
        try:
            logging.info("Attempting to create pool:", pool.id)
            print(pool.id)
            self.batch_client.pool.add(pool)
            logging.info("Created pool:", pool.id)
            return True
        except batchmodels.BatchErrorException as e:
            if e.error.code != "PoolExists":
                raise
            else:
                logging.info("Pool {!r} already exists".format(pool.id))
                return False

    def create_task_copy_file_to_shared_dir(self, container, blob_path, file_path, shared_dir='AZ_BATCH_NODE_SHARED_DIR', ostype='windows'):
        """
        Creates a task to copy file from container blob to shared directory on node.

        Args:
            container (str): Name of container in storage associate with the batch account
            blob_path (str): Path to the blob within the container
            file_path (str): Path to file on node
            shared_dir (str, optional): share directory on node. Defaults to 'AZ_BATCH_NODE_SHARED_DIR'.
            ostype (str, optional): windows or linux. Defaults to 'windows'
        Returns:
            batchmodels.JobPreparationTask: The preparation task
        """
        input_file = batchmodels.ResourceFile(
            auto_storage_container_name=container,
            blob_prefix=blob_path,
            file_path=file_path)

        if ostype == 'windows':
            self.wrap_commands_in_shell(
                'windows', f'move {file_path}\\{blob_path} %AZ_BATCH_NODE_SHARED_DIR%')
        else:
            self.wrap_commands_in_shell(
                'linux', f'mv {file_path}/{blob_path}' + ' ${AZ_BATCH_NODE_SHARED_DIR}')

        prep_task = batchmodels.JobPreparationTask(
            id="copy_file_task",
            command_line=f'cmd /c move {file_path}\\{blob_path} %AZ_BATCH_NODE_SHARED_DIR%',
            resource_files=[input_file])

        return prep_task

    def wrap_commands_in_shell(self, ostype, commands):
        """Wrap commands in a shell

        :param list commands: list of commands to wrap
        :param str ostype: OS type, linux or windows
        :rtype: str
        :return: a shell wrapping commands
        """
        if ostype.lower() == 'linux':
            return '/bin/bash -c \'set -e; set -o pipefail; {}; wait\''.format(
                ';'.join(commands))
        elif ostype.lower() == 'windows':
            return 'cmd.exe /c "{}"'.format('&'.join(commands))
        else:
            raise ValueError('unknown ostype: {}'.format(ostype))

    def create_input_file_spec(self, container_name, blob_prefix, file_path='.'):
        return batchmodels.ResourceFile(
            auto_storage_container_name=container_name,
            blob_prefix=blob_prefix) #,
        #    file_path=file_path)

    def create_output_file_spec(self, file_pattern, output_container_sas_url, blob_path='.'):
        return batchmodels.OutputFile(
            file_pattern=file_pattern,
            destination=batchmodels.OutputFileDestination(
                container=batchmodels.OutputFileBlobContainerDestination(
                    container_url=output_container_sas_url, path=blob_path)),
            upload_options=batchmodels.OutputFileUploadOptions(
                upload_condition=batchmodels.OutputFileUploadCondition.task_success)
        )

    def create_job(self, job_id, pool_id, prep_task=None):
        """
        Creates a job with the specified ID, associated with the specified pool.

        :param batch_service_client: A Batch service client.`azure.batch.BatchServiceClient`
        :param str job_id: The ID for the job.
        :param str pool_id: The ID for the pool.
        :param JobPreparationTask: preparation task before running tasks in the job
        """
        logging.info('Creating job [{}]...'.format(job_id))

        job = batch.models.JobAddParameter(
            id=job_id,
            job_preparation_task=prep_task,
            pool_info=batch.models.PoolInformation(pool_id=pool_id))

        self.batch_client.job.add(job)

    def delete_job(self, job_id):
        raise 'TBD'

    def create_task(self, task_id, command, resource_files=None, output_files=None, env_settings=None,
            elevation_level=None, num_instances=1, coordination_cmdline=None, coordination_files=None):
        """Create a task for the given input_file, command, output file specs and environment settings.

        Args:
            task_id(str) : a unique string to the task id
            command (str): command to execute task. Specific to the OS (e.g. batch file for windows)
            input_file (batchmodels.ResourceFile): Specifies an input file from the storage container blob
            output_files (batchmodels.OutputFile): The output file patterns that will be copied to the storage blob upon success
            env_settings (dict): Dictionary of environment variables
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

    def submit_tasks(self, job_id, tasks):
        try:
            self.batch_client.task.add_collection(job_id, list(tasks))
        except batchmodels.BatchErrorException as err:
            self.print_batch_exception(err)
            raise

    def submit_tasks_and_wait(self, job_id, tasks, wait_time_mins=30, poll_secs=5):
        try:
            self.submit_tasks(self.batch_client, job_id, tasks)
            # Pause execution until tasks reach Completed state.
            self.wait_for_tasks_to_complete(job_id,
                                            datetime.timedelta(minutes=wait_time_mins), poll_secs=poll_secs)
            logging.info("Success! All tasks completed within the timeout period:",
                         wait_time_mins, ' minutes')
        except batchmodels.BatchErrorException as err:
            self.print_batch_exception(err)
            raise

    def print_batch_exception(self, batch_exception):
        """
        Prints the contents of the specified Batch exception.

        :param batch_exception:
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

    def wait_for_job_under_job_schedule(self, job_schedule_id, timeout):
        """Waits for a job to be created and returns a job id.

        :param batch_client: The batch client to use.
        :type batch_client: `batchserviceclient.BatchServiceClient`
        :param str job_schedule_id: The id of the job schedule to monitor.
        :param timeout: The maximum amount of time to wait.
        :type timeout: `datetime.timedelta`
        """
        time_to_timeout_at = datetime.datetime.now() + timeout

        while datetime.datetime.now() < time_to_timeout_at:
            cloud_job_schedule = self.batch_client.job_schedule.get(
                job_schedule_id=job_schedule_id)

            logging.info("Checking if job exists...")
            if (cloud_job_schedule.execution_info.recent_job) and (
                    cloud_job_schedule.execution_info.recent_job.id is not None):
                return cloud_job_schedule.execution_info.recent_job.id
            time.sleep(1)

        raise TimeoutError("Timed out waiting for tasks to complete")

    def wait_for_job_schedule_to_complete(self, job_schedule_id, timeout):
        """Waits for a job schedule to complete.

        :param batch_client: The batch client to use.
        :type batch_client: `batchserviceclient.BatchServiceClient`
        :param str job_schedule_id: The id of the job schedule to monitor.
        :param timeout: The maximum amount of time to wait.
        :type timeout: `datetime.datetime`
        """
        while datetime.datetime.now() < timeout:
            cloud_job_schedule = self.batch_client.job_schedule.get(
                job_schedule_id=job_schedule_id)

            logging.info("Checking if job schedule is complete...")
            state = cloud_job_schedule.state
            if state == batchmodels.JobScheduleState.completed:
                return
            time.sleep(10)

        return

    def set_path_to_apps(self, apps, ostype='windows'):
        """create cmd to set path to apps binary locations

        Parameters
        ----------
        apps : list
            apps is a list of tuples (app_name, app_version, binary_location)


        Returns
        -------
        str
            windows command line to set PATH envvar.

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
                app_name=name.replace('.','_'), app_version=version.replace('.','_'), brace='}', bin_loc=bin_loc) for name, version, bin_loc in apps]
            env_var_path = ":".join(app_loc_var)
            cmd = "export PATH={env_var_path}:$PATH".format(env_var_path=env_var_path)
        else:
            raise Exception(f'unknown ostype: {ostype}')
        return cmd

    def wrap_cmd_with_app_path(self, cmd, app_pkgs, ostype='windows'):
        """wraps cmd with a shell and set to the application packages paths before calling this cmd string

        Args:
            cmd (str): The command line to be wrapped
            app_pkgs (list): list of tuple of (app_name, app_version, binary_location_relative)
            ostype (str, optional): [description]. Defaults to 'windows'.

        Raises:
            f: exception if ostype is not supported

        Returns:
            str: cmd string to use

        Example:
        wrap_cmd_with_app_path('hydro -v', app_pkgs)

        `
        'cmd /c set "PATH=%AZ_BATCH_APP_PACKAGE_dsm2#8.2.c5aacef7%/DSM2-8.2.c5aacef7-win32/bin;%AZ_BATCH_APP_PACKAGE_vista#1.0-v2019-05-28%/bin;%AZ_BATCH_APP_PACKAGE_unzip#5.51-1%/bin;%PATH%" & hydro -v'
        `
        """
        if ostype == 'windows':
            cmd_string = 'cmd /c '
            cmd_string += self.set_path_to_apps(app_pkgs, ostype=ostype)
            cmd_string += ' & '
            cmd_string += cmd
            return cmd_string
        elif ostype == 'linux':
            cmd_string = '/bin/bash -c \''
            cmd_string += self.set_path_to_apps(app_pkgs, ostype=ostype)
            cmd_string += '; '
            cmd_string += cmd + '\''
            return cmd_string
        else:
            raise f'unsupported ostype: {ostype}, only "windows" supported'

    def skus_available(self):
        options = batchmodels.AccountListSupportedImagesOptions(
            filter="verificationType eq 'verified'")
        images = self.batch_client.account.list_supported_images(
            account_list_supported_images_options=options)
        skus_to_use = [(image.node_agent_sku_id, image.image_reference.publisher,
                        image.image_reference.offer, image.image_reference.sku) for image in images]
        return skus_to_use

    def select_latest_verified_vm_image_with_node_agent_sku(
            self, publisher, offer, sku_starts_with):
        """Select the latest verified image that Azure Batch supports given
        a publisher, offer and sku (starts with filter).
    
        :param batch_client: The batch client to use.
        :type batch_client: `batchserviceclient.BatchServiceClient`
        :param str publisher: vm image publisher
        :param str offer: vm image offer
        :param str sku_starts_with: vm sku starts with filter
        :rtype: tuple
        :return: (node agent sku id to use, vm image ref to use)
        """
        # get verified vm image list and node agent sku ids from service
        options = batchmodels.AccountListSupportedImagesOptions(
            filter="verificationType eq 'verified'")
        images = self.batch_client.account.list_supported_images(
            account_list_supported_images_options=options)
        print(images)
        # pick the latest supported sku
        skus_to_use = [
            (image.node_agent_sku_id, image.image_reference) for image in images
            if image.image_reference.publisher.lower() == publisher.lower() and
            image.image_reference.offer.lower() == offer.lower() and
            image.image_reference.sku.startswith(sku_starts_with)
        ]
    
        # pick first
        agent_sku_id, image_ref_to_use = skus_to_use[0]
        print(agent_sku_id, image_ref_to_use)
        return (agent_sku_id, image_ref_to_use)

    def print_task_output(self, job_id, task_ids, encoding=None):
        """Prints the stdout and stderr for each task specified.

        :param batch_client: The batch client to use.
        :type batch_client: `batchserviceclient.BatchServiceClient`
        :param str job_id: The id of the job to monitor.
        :param task_ids: The collection of tasks to print the output for.
        :type task_ids: `list`
        :param str encoding: The encoding to use when downloading the file.
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

    def _read_stream_as_string(self, stream, encoding):
        """Read stream as string

        :param stream: input stream generator
        :param str encoding: The encoding of the file. The default is utf-8.
        :return: The file content.
        :rtype: str
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
            self, job_id, task_id, file_name, encoding=None):
        """Reads the specified file as a string.

        :param batch_client: The batch client to use.
        :type batch_client: `batchserviceclient.BatchServiceClient`
        :param str job_id: The id of the job.
        :param str task_id: The id of the task.
        :param str file_name: The name of the file to read.
        :param str encoding: The encoding of the file. The default is utf-8.
        :return: The file content.
        :rtype: str
        """
        stream = self.batch_client.file.get_from_task(job_id, task_id, file_name)
        return self._read_stream_as_string(stream, encoding)

    def read_compute_node_file_as_string(
            self, pool_id, node_id, file_name, encoding=None):
        """Reads the specified file as a string.

        :param batch_client: The batch client to use.
        :type batch_client: `batchserviceclient.BatchServiceClient`
        :param str pool_id: The id of the pool.
        :param str node_id: The id of the node.
        :param str file_name: The name of the file to read.
        :param str encoding: The encoding of the file.  The default is utf-8
        :return: The file content.
        :rtype: str
        """
        stream = self.batch_client.file.get_from_compute_node(
            pool_id, node_id, file_name)
        return self._read_stream_as_string(stream, encoding)
#################################################################################


class AzureBlob:

    def __init__(self, storage_account_name, storage_account_key,storage_account_domain):
        # Create the blob client, for use in obtaining references to
        # blob storage containers and uploading files to containers.
        self.blob_client = azureblob.BlobServiceClient(
        account_url="https://{}.{}/".format(storage_account_name,storage_account_domain), 
        credential=storage_account_key
        )
        #connection_string = "DefaultEndpointsProtocol=https;AccountName=xxxx;AccountKey=xxxx;EndpointSuffix=core.windows.net"
        #service = BlobServiceClient.from_connection_string(conn_str=connection_string)
        self.storage_account_name = storage_account_name
        self.storage_account_key = storage_account_key
        self.storage_account_domain = storage_account_domain

    def get_container_sas_token(self,
                            container_name, blob_name, blob_permissions):
        """
        Obtains a shared access signature granting the specified permissions to the
        container.

        :param block_blob_client: A blob service client.
        :type block_blob_client: `azure.storage.blob.BlobServiceClient`
        :param str container_name: The name of the Azure Blob storage container.
        :param BlobPermissions blob_permissions:
        :rtype: str
        :return: A SAS token granting the specified permissions to the container.
        """
        # Obtain the SAS token for the container, setting the expiry time and
        # permissions. In this case, no start time is specified, so the shared
        # access signature becomes valid immediately. Expiration is in 2 hours.
        container_sas_token = generate_blob_sas( 
            self.storage_account_name,
            container_name,
            blob_name,
            account_key=self.storage_account_key,
            permission=BlobSasPermissions(read=True),
            expiry=datetime.datetime.utcnow() + datetime.timedelta(hours=2)
        )

        return container_sas_token

    def get_container_sas_url(self,
                            container_name, blob_name, blob_permissions):
        """
        Obtains a shared access signature URL that provides write access to the
        ouput container to which the tasks will upload their output.

        :param block_blob_client: A blob service client.
        :type block_blob_client: `azure.storage.blob.BlobServiceClient`
        :param str container_name: The name of the Azure Blob storage container.
        :param BlobPermissions blob_permissions:
        :rtype: str
        :return: A SAS URL granting the specified permissions to the container.
        """
        if not blob_permissions:
            blob_permissions = BlobSasPermissions(write=True)
        # Obtain the SAS token for the container.
        sas_token = self.get_container_sas_token(container_name, blob_name, blob_permissions)

        # Construct SAS URL for the container
        container_sas_url = "https://{}.blob.core.windows.net/{}?{}".format(
            self.blob_client.account_name, container_name, sas_token)

        return container_sas_url

    def create_container_and_create_sas(
            self, container_name, permission, expiry=None,
            timeout=None):
        """Create a blob sas token

        :param block_blob_client: The storage block blob client to use.
        :type block_blob_client: `azure.storage.blob.BlobServiceClient`
        :param str container_name: The name of the container to upload the blob to.
        :param expiry: The SAS expiry time.
        :type expiry: `datetime.datetime`
        :param int timeout: timeout in minutes from now for expiry,
            will only be used if expiry is not specified
        :return: A SAS token
        :rtype: str
        """
        if expiry is None:
            if timeout is None:
                timeout = 30
            expiry = datetime.datetime.utcnow() + datetime.timedelta(
                minutes=timeout)

        self.blob_client.create_container(
            container_name,
            fail_on_exist=False)

        return self.blob_client.generate_container_shared_access_signature(
            container_name=container_name, permission=permission, expiry=expiry)

    def create_sas_url(self, container_name, blob_name, expiry=None, timeout=None, permission=BlobSasPermissions(read=True)):
        sas_token = generate_blob_sas(
            self.storage_account_name,
            container_name,
            blob_name,
            account_key=self.storage_account_key,
            permission=BlobSasPermissions(read=True),
            expiry=datetime.datetime.utcnow() + datetime.timedelta(hours=2)
        )

        sas_url = "https://{}.{}/{}/{}?{}".format(
            self.storage_account_name,
            self.storage_account_domain,
            container_name,
            blob_name,
            sas_token
        )
        
        return sas_url
    
    def upload_blob_from_node(self,filepath):
        try:
            with open(filepath, "rb") as data:
                self.blob_client.upload_blob(data, overwrite=True)   
        except ResourceExistsError:
            pass
            
            
    def upload_blob_and_create_sas(
            self, container_name, blob_name, file_name, expiry,
            timeout=None, max_connections=2):
        """Uploads a file from local disk to Azure Storage and creates
        a SAS for it.

        :param block_blob_client: The storage block blob client to use.
        :type block_blob_client: `azure.storage.blob.BlobServiceClient`
        :param str container_name: The name of the container to upload the blob to.
        :param str blob_name: The name of the blob to upload the local file to.
        :param str file_name: The name of the local file to upload.
        :param expiry: The SAS expiry time.
        :type expiry: `datetime.datetime`
        :param int timeout: timeout in minutes from now for expiry,
            will only be used if expiry is not specified
        :return: A SAS URL to the blob with the specified expiry time.
        :rtype: str
        """
        try:
            self.blob_client.create_container(container_name)
        except ResourceExistsError:
            pass
        self.blob_client = self.blob_client.get_blob_client(container_name, blob_name)
        try:
            with open(file_name, "rb") as data:
                self.blob_client.upload_blob(data, overwrite=True)   
        except ResourceExistsError:
            pass
        
        return self.create_sas_url(container_name, blob_name, expiry, timeout)

    def upload_file_to_container(
            self, container_name, blob_name, file_path, timeout=None, max_connections=2):
        """
        Uploads a local file to an Azure Blob storage container.

        :param block_blob_client: A blob service client.
        :type block_blob_client: `azure.storage.blob.BlobServiceClient`
        :param str container_name: The name of the Azure Blob storage container.
        :param str file_path: The local path to the file.
        :param int timeout: timeout in minutes from now for expiry,
            will only be used if expiry is not specified
        :rtype: `azure.batch.models.ResourceFile`
        :return: A ResourceFile initialized with a SAS URL appropriate for Batch
        tasks.
        """
        logging.info('Uploading file {} to container [{}]...'.format(
            file_path, container_name))
        sas_url = self.upload_blob_and_create_sas(
            container_name, blob_name, file_path, expiry=None,
            timeout=timeout, max_connections=max_connections)
        return batchmodels.ResourceFile(
            file_path=blob_name, http_url=sas_url)

    def zip_and_upload(self, container_name, blob_name, dirname, timeout=30):
        """
        Zips and uploads to the container with the blob_name appended to the dirname the directory dirname

        Parameters
        ----------
        container_name : str
            name of storage container (will be created if it doesn't exist)
        blob_name : str
            name of blob in the storage container
        dirname : str
            path to the directory
        timeout : int, optional
            timeout in minutes, by default 30

        Returns
        -------
        ResourceFile
            a resource file containing information on what was uploaded.
        """
        try:
            base, name = os.path.split(dirname)
            shutil.make_archive(dirname, 'zip', base, name)
            
            if blob_name is None:
                blob_name = ''
            if len(blob_name) > 0 and not blob_name.endswith('/'):
                blob_name = blob_name + '/'
            zippath = base+'/'+name+'.zip'
            return self.upload_file_to_container(container_name, f'{blob_name}{name}.zip', zippath, timeout=timeout)
        finally:
            os.remove(zippath)
        
        
    
        
        
    def download_blob_from_container(
            self, container_name, blob_name, directory_path):
        """
        Downloads specified blob from the specified Azure Blob storage container.

        :param block_blob_client: A blob service client.
        :type block_blob_client: `azure.storage.blob.BlobServiceClient`
        :param container_name: The Azure Blob storage container from which to
            download file.
        :param blob_name: The name of blob to be downloaded
        :param directory_path: The local directory to which to download the file.
        """
        logging.info('Downloading result file from container [{}]...'.format(
            container_name))

        destination_file_path = os.path.join(directory_path, blob_name)

        self.blob_client.get_blob_to_path(
            container_name, blob_name, destination_file_path)

        logging.info('  Downloaded blob [{}] from container [{}] to {}'.format(
            blob_name, container_name, destination_file_path))

        logging.info('  Download complete!')
    #

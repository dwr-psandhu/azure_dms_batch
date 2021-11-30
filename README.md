============================================================
Azure Batch runs for Models (input(s) --> EXE --> output(s))
=============================================================

Azure Batch runs for a model, i.e., a executable that runs independently based on a set of input files and environment
variables and produces a set of output files.

This module is currently tested with "Windows" based exes but shoud be easily adapatable to "Linux"

Setup 
---------
Setup can be done via az commands. Here we setup a batch account with associated storage

Login with your Azure credentials
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``az login ``

Create a resource group in the desired location
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

See the Azure docs for details. To use the commands below, enter your values (replacing the angle brackets and values)

``az group create --name <resource_group_name> --location <location_name>``

``az storage account create --resource-group <resource_group_name> --name <storage_account_name> --location <location_name> --sku Standard_LRS``

``az batch account create --name <batch_account_name> --storage-account <storage_account_name> --resource-group <resource_group_name> --location <location_name>``

Applications
~~~~~~~~~~~~~

Applications are binary executable packages. These are uploaded as application packages with a version number. A pool can be specified
so that these application packages are pre-downloaded to the nodes of the pool before a job/tasks are run on it.

VM sizes available
~~~~~~~~~~~~~~~~~~~~~

This is needed later when deciding what machine sizes to use
``az batch location list-skus --location <location_name> --output table``

OS Images available
~~~~~~~~~~~~~~~~~~~~
set AZ_BATCH_ACCOUNT=<batch_account_name>
set AZ_BATCH_ACCESS_KEY=<batch_account_key>
set AZ_BATCH_ENDPOINT=<batch_account_url>
az batch pool supported-images list --output table


Tools
------

`Batch Explorer<https://azure.github.io/BatchExplorer/>`_ is a desktop tool for managing batch jobs, pools and application packages

`Storage Explorer<https://azure.microsoft.com/en-us/features/storage-explorer/>`_ is a desktop tool for working with storage containers

Classes
--------

There are two classes in batch.commands

*AzureBlob* to work with uploading/downloading to blobs
*AzureBatch* to work with pools, jobs and tasks. Depends upon *AzureBlob* but not other way around

Management of batch resources such as creation of batch account, storage account, etc is a low repeat activity 
and can be managed via the az command line options

Model
-----

Model is considered to be something that :-
 - needs application packages, versions and the location of the binary directory (i.e. ApplicationPackage[])
 - needs input file(s), common ones or unique ones. These need to be uploaded to storage as blobs and then referenced
 - has output file(s), which are uploaded to the associated storage via directives to the batch service


Model run
~~~~~~~~~

 Model run is a particular execution that is submitted to the batch service as a *task* 
 Each run :-  
  - needs a unique task name 
  - will have an output unique to it
  - could have a set of unique input files
  - could have environment settings unique to each run

Parameterized runs
~~~~~~~~~~~~~~~~~~~

 Many times the model runs are closely related to each other and only a few parameters are varied. These are
 submitted as a *task* to the batch service and perhaps reuse the same *pool* 
 The batch submission script samples the paramter space and submits the *task*.

 The best way to submit these *tasks* is to vary the environment variables and have the model use those 
 environment variables to change the parameter values. A less efficient but equally effective way would be to 
 express the change in parameter input as a changed input file that can be overlaid on top of the other inputs

 In each case, the model run is expressed as a *task*

Beopest runs
~~~~~~~~~~~~

 PEST (Parameterized ESTimation) is a software package for non-linear optimization. Beopest is a master/slave model 
 to implement a parallel version of estimation runs. Each run is a separate process that needs to run on its own set of
 a model run. The difference between the "Parameterized" runs and this is that the orchestration of the parameters is 
 done by beopest.

 First a beopest master is submitted to the batch service. Then that tasks stdout.txt is polled and the first line is 
 assumed to have the hostname which is then captured. This hostname is then passed as an environment variable to start
 multiple slaves as batch runs.  beopest master should then be able to register these slave tasks as they come in and 
 submit runs to them through its own mechanism (MPI). 
 
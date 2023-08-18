# crabOverseer documentation

`crabOverseer` is a tool to manage CRAB tasks and the following post-processing steps.

`crabOverseer` (and, in general, `RunKit`) is intended as a submodule for a task-specific framework that setups the environment and provides the necessary configuration files.
Here are some examples of frameworks that use `RunKit`:

- [NanoProd](https://github.com/cms-tau-pog/NanoProd)
- [TauMLTools](https://github.com/cms-tau-pog/TauMLTools)
- [HH->bbtautau framework](https://github.com/cms-hh-bbtautau/Framework)
- [HNLProd](https://github.com/cms-hnl/HNLProd)

## Logical flow

- Preparation steps:
    1. Setup framework area and environment on the site where the crab outputs will be staged out
    1. Define crabOverseer configurations
    1. Setup VOMS certificates
    1. Make sure that there is enough space to store the crab outputs
        - with the current implementation (to be improved), you should reserve 2x the expected size to store intermediate outputs, which can be removed after the post-processing is finished
    1. Run dry-run submission to make sure that setup works as expected
- Submission and the main `crabOverseer` loop:
    1. Update the status of all active tasks:
        - if a task is new, submit it
        - if a task is submitted on CRAB, update the status
        - if a task is failed on crab, create and submit a crab recovery task
            - if the maximal number of recovery attempts is reached, create local recovery jobs
        - if a task is finished on CRAB, check that all outputs are present
            - if not, create and submit a crab (or local) task
            - if yes, create a local post-processing job
    1. Submit and monitor the list of local recovery and post-processing jobs defined in the previous step
        - [law](https://github.com/riga/law) is used to submit and monitor the jobs on a local grid
    1. If there are non-finished tasks, wait for `updateInterval` (starting from the last beginning of step 1) and repeat the loop
    1. If all tasks are finished, finish `crabOverseer`.

## Usage

```bash
python RunKit/crabOverseer.py [-h] [--work-area WORK_AREA] [--cfg CFG] [--no-status-update] [--update-cfg] [--no-loop] [--select SELECT] [--action ACTION] [--verbose VERBOSE] [task_file ...]
```

Command line arguments:

| Argument | Description |
| :--- | :--- |
| **-h**, **--help** | Show the help message and exit |
| **--work-area** WORK_AREA| The working area to store crabOverseer state (default: *$PWD/.crabOverseer*) |
| **--cfg** CFG| The main crabOverseer configuration file (see description [below](#main-config-file-format)) |
| **task_file** | The list of files with a description of tasks to be managed by crabOverseer (see description [below](#task-configuration-file-format)) |
| **--no-status-update** | If specified, do not call crab to update tasks statuses and proceed with the next steps |
| **--update-cfg** | Update the main and all task configurations from the config files provided in **--cfg** and **task_file** arguments |
| **--no-loop** | Run one iteration of task update and submission and exit |
| **--action** ACTION | Apply an action on the selected tasks and exit (see description [below](#actions)) |
| **--select** SELECT | Select tasks to which the action should be applied (default: select all) |
| **--verbose** VERBOSE | Verbosity level (default: 1) |

After the first call, the crabOverseer state is stored in the working area, and the subsequent calls will use it.
Therefore, it is not necessary to provide arguments for the subsequent calls, meaning that the following command will be enough:

```bash
python RunKit/crabOverseer.py
```
Alternatively, if a non-default working area is used:
```bash
python RunKit/crabOverseer.py --work-area <working_area>
```

## Main configuration file format

The main `crabOversser` configuration file uses the [YAML](https://en.wikipedia.org/wiki/YAML) format.
It contains definitions that are common for all tasks. A task-specific definition can be defined (or overwritten) in the [task configuration file](#task-configuration-file-format).

### Supported parameters

| Parameter | Description |
| :--- | :--- |
| **cmsswPython** | path to the CMSSW python configuration file. For a nanoAOD production, use `RunKit/nanoProdWrapper.py`. |
| **params** | list of parameters that will be passed during execution of `cmsRun` on the `cmsswPython` file |
| **splitting** | Crab job splitting. Currently, only `FileBased` splitting is supported |
| **unitsPerJob** | number of units per job (i.e. files per job for `FileBased` splitting) for the initial task submission. This parameter is decreased by a factor of 2 for each consecutive recovery submission. Suggested value: 16 |
| **scriptExe** | Executable script that will be run on the remote nodes. Suggested value: `RunKit/crabJob.sh` |
| **outputFiles** | List of output files produced by the crab job. The suggested value for a nanoAOD production: `- nano.root` |
| **filesToTransfer** | List of files that CRAB will transfer to the remote nodes |
| **site** | Site where CRAB will transfer jobs outputs. Example: `T2_CH_CERN` |
| **crabOutput** | The path where jobs outputs will be stored using `/store/...` notation |
| **localCrabOutput** | path where `crabOutput` is mounted in the local file system |
| **finalOutput** | path in the local file system where the final post-processed outputs will be stored |
| **maxMemory** | Memory requirements per job in MB. Suggested value: 2500 |
| **numCores** | number of cores per job. Suggested value: 1 |
| **inputDBS** | Input DBS. Suggested value: global |
| **allowNonValid** | Allow processing datasets listed as not VALID on DAS. Suggested value: False |
| **dryrun** | Run CRAB in a dry-run mode (for testing). Suggested value: False |
| **maxRecoveryCount** | Maximal number of recovery attempts. Suggested value:  3 |
| **updateInterval** | Interval in minutes between the task update & post-processing iterations | Suggested value: 60 |
| **localProcessing** | Parameters for the local recovery and post-processing step. |
| localProcessing / **lawTask** | Name of the law task. Suggested value: `ProdTask` |
| localProcessing / **workflow** | Workflow type. Currently, only the `htcondor` workflow is supported |
| localProcessing / **bootstrap** | Bootstrap file to load environment on a remote node. Suggested value: `bootstrap.sh` |
| localProcessing / **requirements** | (optional) additional requirement for a remote node |
| **targetOutputFileSize** | Desired size of the output files in MiB. Suggested value: 2048 |
| **renewKerberosTicket** | Periodically renew the validity of a Kerberos ticket. Suggested value: `True` if run on AFS; otherwise `False` |
| **whitelistFinalRecovery** | list of "most reliable" sites where the final recovery will be performed |

Example configuration file:
```yaml
cmsswPython: RunKit/nanoProdWrapper.py
params:
  customise: NanoProd/NanoProd/customize.customize
  skimCfg: skim_htt.yaml
  skimSetup: skim
  skimSetupFailed: skim_failed
  maxEvents: -1
splitting: FileBased
unitsPerJob: 16
scriptExe: RunKit/crabJob.sh
outputFiles:
  - nano.root
filesToTransfer:
  - RunKit/crabJob.sh
  - RunKit/crabJob.py
  - RunKit/crabJob_nanoProd.py
  - RunKit/skim_tree.py
  - RunKit/sh_tools.py
  - NanoProd/config/skim_htt.yaml
  - NanoProd/python/customize.py
site: T2_CH_CERN
crabOutput: /store/group/phys_tau/kandroso/prod
localCrabOutput: /eos/cms/store/group/phys_tau/kandroso/prod
finalOutput: /eos/cms/store/group/phys_tau/kandroso/final
maxMemory: 2500
numCores: 1
inputDBS: global
allowNonValid: False
dryrun: False
maxRecoveryCount: 3
updateInterval: 60
localProcessing:
  lawTask: ProdTask
  workflow: htcondor
  bootstrap: bootstrap.sh
targetOutputFileSize: 2048
renewKerberosTicket: True
whitelistFinalRecovery:
  - T1_DE_KIT
  - T2_CH_CERN
  - T2_DE_DESY
  - T2_IT_Legnaro
  - T3_CH_PSI
```

## Task configuration file format

The task configuration file uses the [YAML](https://en.wikipedia.org/wiki/YAML) format.
Each file contains a list of tasks with the same set of parameters.
The parameters defined in the task configuration file overwrite the parameters defined in the [main configuration file](#main-config-file-format).

### Format
- **config**: Section with the task-specific parameters. Same as for the [main configuration](#supported-parameters) |
- all items with names different from "config" are considered task descriptions. Two formats are possible:
    - short format:
        ```yaml
        dataset_name: dataset_path
        ```
        Example:
        ```yaml
        TTTo2L2Nu: /TTTo2L2Nu_TuneCP5_13TeV-powheg-pythia8/RunIISummer20UL18MiniAODv2-106X_upgrade2018_realistic_v16_L1v1-v1/MINIAODSIM
        ```
    - long format, where some parameters are overwritten:
        ```yaml
        dataset_name:
            path: dataset_path
            param1: value1
        ...
        ```

        Example:
        ```yaml
        QCD_HT200to300:
            inputDataset: /QCD_HT200to300_TuneCP5_13TeV-madgraphMLM-pythia8/RunIISummer20UL18MiniAODv2-106X_upgrade2018_realistic_v16_L1v1-v2/MINIAODSIM
            ignoreFiles:
                - /store/mc/RunIISummer20UL18MiniAODv2/QCD_HT200to300_TuneCP5_13TeV-madgraphMLM-pythia8/MINIAODSIM/106X_upgrade2018_realistic_v16_L1v1-v2/70000/C38FF40C-E9F0-CB48-B9C7-1E874A4AF010.root
        ```

Example configuration file:
```yaml
config:
  params:
    sampleType: mc
    era: Run2_2018
    storeFailed: True

TTTo2L2Nu: /TTTo2L2Nu_TuneCP5_13TeV-powheg-pythia8/RunIISummer20UL18MiniAODv2-106X_upgrade2018_realistic_v16_L1v1-v1/MINIAODSIM
TTToHadronic: /TTToHadronic_TuneCP5_13TeV-powheg-pythia8/RunIISummer20UL18MiniAODv2-106X_upgrade2018_realistic_v16_L1v1-v1/MINIAODSIM
TTToSemiLeptonic: /TTToSemiLeptonic_TuneCP5_13TeV-powheg-pythia8/RunIISummer20UL18MiniAODv2-106X_upgrade2018_realistic_v16_L1v1-v2/MINIAODSIM
```

## Actions
When something goes wrong and automatic recovery fails, it could be necessary to apply some manual manipulations to the tasks.
Such intervention can be done by using `crabOverseer` actions. Alternatively, manual editing of task configuration and status files could be required in complicated cases.

Action is applied from a command line on the selected tasks.
Tasks can be selected with the `--select` argument using Python syntax.
Selection is applied to each individual [crabTask](https://github.com/kandrosov/RunKit/blob/main/crabTask.py) object.

Examples:

- `--select 'name == "TTTo2L2Nu"'` : select task with name `TTTo2L2Nu`
- `--select 'task.params["sampleType"] == "mc"'`: select all tasks that process MC datasets

The following actions are supported:

- **print**: print names of the selected tasks

    Example:
    ```bash
    python RunKit/crabOverseer.py --action print --select 'task.params["sampleType"] == "mc"'
    ```

- **list_files_to_process**: print a list of files that are yet to be processed

    Example:
    ```bash
    python RunKit/crabOverseer.py --action list_files_to_process --select 'name == "TTTo2L2Nu"'
    ```

- **kill**: kill selected tasks

    Example:
    ```bash
    python RunKit/crabOverseer.py --action kill --select 'name == "TTTo2L2Nu"'
    ```

- **remove**: remove selected tasks (**cannot be undone!**)

    Example:
    ```bash
    python RunKit/crabOverseer.py --action remove --select 'name == "TTTo2L2Nu"'
    ```

- **remove_final_output**: remove final outputs of selected tasks

    Example:
    ```bash
    python RunKit/crabOverseer.py --action remove_final_output --select 'name == "TTTo2L2Nu"'
    ```

- **run_cmd**: execute the specified Python code on each selected task

    Examples:
    ```bash
    python RunKit/crabOverseer.py --action 'run_cmd task.kill()' --select 'name == "TTTo2L2Nu"'
    python RunKit/crabOverseer.py --action 'run_cmd task.taskStatus.status = status.WaitingForRecovery' --select 'name == "TTTo2L2Nu"'
    ```

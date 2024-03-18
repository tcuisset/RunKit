import copy
import datetime
import json
import os
import shutil
import sys
import tempfile
import traceback

if __name__ == "__main__":
  file_dir = os.path.dirname(os.path.abspath(__file__))
  sys.path.append(os.path.dirname(file_dir))
  __package__ = 'RunKit'

from .crabTaskStatus import CrabTaskStatus, Status, JobStatus, LogEntryParser, StatusOnScheduler, StatusOnServer
from .run_tools import PsCallError, ps_call, natural_sort, timestamp_str, adler32sum
from .grid_tools import get_voms_proxy_info, lfn_to_pfn, gfal_copy_safe, gfal_ls_safe, das_file_pfns, \
                        gfal_copy, GfalError, COPY_TMP_SUFFIX, gfal_exists, gfal_rm
from .envToJson import get_cmsenv
from .getFileRunLumi import getFileRunLumi

class Task:
  _taskCfgProperties = [
    'cmsswPython', 'params', 'unitsPerJob', 'scriptExe', 'filesToTransfer',
    'lumiMask', 'maxMemory', 'numCores', 'inputDBS', 'allowNonValid',
    'vomsGroup', 'vomsRole', 'blacklist', 'whitelist', 'whitelistFinalRecovery', 'dryrun',
    'maxRecoveryCount', 'targetOutputFileSize', 'ignoreFiles', 'ignoreLocality', 'crabType'
  ]

  _taskCfgPrivateProperties = [
    'name', 'inputDataset', 'recoveryIndex', 'taskIds', 'lastJobStatusUpdate', 'outputs', 'startDate', 'endDate'
  ]

  inputLumiMaskJsonName = 'inputLumis'
  crabOperationTimeout = 10 * 60
  dasOperationTimeout = 10 * 60

  def __init__(self):
    self.taskStatus = CrabTaskStatus()
    self.workArea = ''
    self.cfgPath = ''
    self.statusPath = ''
    self.name = ''
    self.inputDataset = ''
    self.cmsswPython = ''
    self.params = {}
    self.unitsPerJob = -1
    self.scriptExe = ''
    self.filesToTransfer = []
    self.outputs = []
    self.lumiMask = ''
    self.maxMemory = -1
    self.numCores = -1
    self.inputDBS = ''
    self.allowNonValid = False
    self.vomsGroup = ''
    self.vomsRole = ''
    self.blacklist = []
    self.whitelist = []
    self.whitelistFinalRecovery = []
    self.ignoreLocality = False
    self.dryrun = False
    self.recoveryIndex = 0
    self.maxRecoveryCount = 0
    self.targetOutputFileSize = 0
    self.ignoreFiles = []
    self.jobInputFiles = None
    self.datasetFiles = None
    self.fileRunLumi = None
    self.fileRepresentativeRunLumi = None
    self.taskIds = {}
    self.lastJobStatusUpdate = -1.
    self.cmsswEnv = None
    self.gridJobs = None
    self.crabType = ''
    self.processedFilesCache = None
    self.vomsToken = None
    self.startDate = ''
    self.endDate = ''
    self.singularity_cmd = os.environ.get('CMSSW_SINGULARITY', None)
    if self.singularity_cmd is not None and len(self.singularity_cmd) == 0:
      self.singularity_cmd = None

  def checkConfigurationValidity(self):
    def check(cond, prop):
      if not cond:
        raise RuntimeError(f'{self.name}: Configuration error: {prop} is not correctly set.')
    def check_len(prop):
      check(len(getattr(self, prop)) > 0, prop)

    for prop in [ 'cmsswPython', 'inputDBS', 'name', 'inputDataset' ]:
      check_len(prop)
    check(self.unitsPerJob > 0, 'unitsPerJob')
    check(self.maxMemory > 0, 'maxMemory')
    check(self.numCores > 0, 'numCores')

  def _setFromCfg(self, pName, cfg, add=False):
    if pName in cfg:
      pType = type(getattr(self, pName))
      pValue = copy.deepcopy(cfg[pName])
      if pType == float and type(pValue) == int:
        pValue = float(pValue)
      if pType != type(pValue):
        raise RuntimeError(f'{self.name}: inconsistent config type for "{pName}". cfg value = "{pValue}"')
      if add:
        if pType == list:
          x = list(set(getattr(self, pName) + pValue))
          setattr(self, pName, x)
        elif pType == dict:
          getattr(self, pName).update(pValue)
        else:
          setattr(self, pName, pValue)
      else:
        setattr(self, pName, pValue)

  def saveCfg(self):
    cfg = { }
    for pName in Task._taskCfgPrivateProperties:
      cfg[pName] = getattr(self, pName)
    for pName in Task._taskCfgProperties:
      cfg[pName] = getattr(self, pName)
    with open(self.cfgPath, 'w') as f:
      json.dump(cfg, f, indent=2)

  def saveStatus(self):
    with open(self.statusPath, 'w') as f:
      f.write(self.taskStatus.to_json())

  def requestName(self, recoveryIndex=None):
    name = self.name
    if recoveryIndex is None:
      recoveryIndex = self.recoveryIndex
    if recoveryIndex > 0:
      name += f'_recovery_{recoveryIndex}'
    return name

  def getOutputs(self, forceUpdate=False):
    if len(self.outputs) == 0 or forceUpdate:
      self.outputs = []
      if 'outputs' not in self.params or len(self.params['outputs']) == 0:
        raise RuntimeError(f'{self.name}: no outputs are defined.')
      for output in self.params['outputs']:
        desc = { 'file': output['file'] }
        desc['name'], desc['ext'] = os.path.splitext(output['file'])
        for destName in [ 'crabOutput', 'finalOutput' ]:
          dest = output[destName]
          if dest.startswith('T'):
            server, lfn = dest.split(':')
            task_lfn = os.path.join(lfn, self.name)
            dest = lfn_to_pfn(server, task_lfn)
          desc[destName] = dest
        if 'skimCfg' in output:
          desc['skimCfg'] = output['skimCfg']
          desc['skimSetup'] = output['skimSetup']
          if 'skimSetupFailed' in output:
            desc['skimSetupFailed'] = output['skimSetupFailed']
        self.outputs.append(desc)
    return self.outputs

  def getParams(self, appendDatasetFiles=True):
    params = [ f'{key}={value}' for key,value in self.params.items() if key != 'outputs' ]
    for output in self.getOutputs():
      output_list = [ output['file'], output['crabOutput'] ]
      if 'skimCfg' in output:
        output_list.append(output['skimCfg'])
        output_list.append(output['skimSetup'])
        if 'skimSetupFailed' in output:
          output_list.append(output['skimSetupFailed'])
      output_str = 'output=' + ';'.join(output_list)
      params.append(output_str)
    params.append(f'recoveryIndex={self.recoveryIndex}')
    if appendDatasetFiles:
      datasetFileDir, datasetFileName = os.path.split(self.getDatasetFilesPath())
      params.append(f'datasetFiles={datasetFileName}')
    return params

  def isInputDatasetLocal(self):
    return self.inputDataset.startswith('local:')

  def isInLocalRunMode(self, recoveryIndex=None):
    if recoveryIndex is None:
      recoveryIndex = self.recoveryIndex
    return self.isInputDatasetLocal() or recoveryIndex >= self.maxRecoveryCount

  def getUnitsPerJob(self):
    if self.recoveryIndex >= self.maxRecoveryCount - 1:
      return 1
    return max(self.unitsPerJob // (2 ** self.recoveryIndex), 1)

  def getSplitting(self):
    return 'FileBased'

  def getLumiMask(self):
    if self.recoveryIndex > 0:
      return os.path.join(self.workArea, f'{Task.inputLumiMaskJsonName}_{self.recoveryIndex}.json')
    return self.lumiMask

  def getMaxMemory(self):
    if self.recoveryIndex >= self.maxRecoveryCount - 1:
      return max(self.maxMemory, 4000)
    return self.maxMemory

  def getMaxJobRuntime(self):
    if self.recoveryIndex >= self.maxRecoveryCount - 1:
      return 2 * 24
    return 24

  def getWhiteList(self):
    if self.recoveryIndex >= self.maxRecoveryCount - 1:
      return self.whitelistFinalRecovery
    return self.whitelist

  def getBlackList(self):
    return self.blacklist

  def getIgnoreLocality(self):
    if self.recoveryIndex == self.maxRecoveryCount - 1:
      return True
    return self.ignoreLocality

  def getFilesToTransfer(self, appendDatasetFiles=True):
    if appendDatasetFiles:
      return self.filesToTransfer + [ self.getDatasetFilesPath() ]
    return self.filesToTransfer

  def getCmsswEnv(self):
    if self.cmsswEnv is None:
      cmssw_path = os.environ['DEFAULT_CMSSW_BASE']
      self.cmsswEnv = get_cmsenv(cmssw_path, crab_env=True, crab_type=self.crabType,
                                 singularity_cmd=self.singularity_cmd)
      self.cmsswEnv['X509_USER_PROXY'] = os.environ['X509_USER_PROXY']
      self.cmsswEnv['HOME'] = os.environ['HOME'] if 'HOME' in os.environ else self.workArea
      if 'KRB5CCNAME' in os.environ:
        self.cmsswEnv['KRB5CCNAME'] = os.environ['KRB5CCNAME']
      for var in [ 'BASH_FUNC_cmsrel%%', 'BASH_FUNC_cmsenv%%' ]:
        if var in self.cmsswEnv:
          del self.cmsswEnv[var]
    return self.cmsswEnv

  def getVomsToken(self):
    if self.vomsToken is None:
      self.vomsToken = get_voms_proxy_info()['path']
    return self.vomsToken

  def getDatasetFilesPath(self):
    return os.path.join(self.workArea, 'dataset_files.json')

  def getDatasetFiles(self):
    if self.datasetFiles is None:
      datasetFilesPath = self.getDatasetFilesPath()
      if os.path.exists(datasetFilesPath):
        with open(datasetFilesPath, 'r') as f:
          self.datasetFiles = json.load(f)
      else:
        if self.isInputDatasetLocal():
          print(f'{self.name}: Gathering dataset files ...')
          ds_path = self.inputDataset[len('local:'):]
          # if not os.path.exists(ds_path):
          #   raise RuntimeError(f'{self.name}: unable to find local dataset path "{ds_path}"')
          self.datasetFiles = {}
          # all_files = []
          # for subdir, dirs, files in os.walk(ds_path):
          #   for file in files:
          #     if file.endswith('.root') and not file.startswith('.'):
          #       all_files.append('file:' + os.path.join(subdir, file))
          # from XRootD import client
          # from XRootD.client.flags import DirListFlags, OpenFlags, MkDirFlags, QueryCode
          # myclient = client.FileSystem('root://eos.grif.fr')
          import subprocess
          res = subprocess.run("gfal-ls '" + ds_path + "'", shell=True, env={}, stdout=subprocess.PIPE, check=True, text=True)
          for file_id, filename in enumerate(natural_sort(res.stdout.splitlines())):
            self.datasetFiles[os.path.join(ds_path, filename)] = file_id

          # for file_id, file_path in enumerate(natural_sort(all_files)):
          #   self.datasetFiles[file_path] = file_id
        else:
          self.datasetFiles = {}
          for file_id, file in enumerate(natural_sort(self.getFileRunLumi().keys())):
            self.datasetFiles[file] = file_id
        with open(datasetFilesPath, 'w') as f:
          json.dump(self.datasetFiles, f, indent=2)
      if len(self.datasetFiles) == 0:
        raise RuntimeError(f'{self.name}: empty dataset {self.inputDataset}')
    return self.datasetFiles

  def getDatasetFileById(self, file_id):
    if type(file_id) is str:
      file_id = int(file_id)
    for file, fileId in self.getDatasetFiles().items():
      if fileId == file_id:
        return file
    raise RuntimeError(f'{self.name}: unable to find file with id {file_id}')

  def getFileRunLumi(self):
    if self.fileRunLumi is None:
      fileRunLumiPath = os.path.join(self.workArea, 'file_run_lumi.json')
      if not os.path.exists(fileRunLumiPath):
        print(f'{self.name}: Gathering file->(run,lumi) correspondance ...')
        self.fileRunLumi = getFileRunLumi(self.inputDataset, inputDBS=self.inputDBS,
                                          dasOperationTimeout=Task.dasOperationTimeout)
        with open(fileRunLumiPath, 'w') as f:
          json.dump(self.fileRunLumi, f, indent=2)
      else:
        with open(fileRunLumiPath, 'r') as f:
          self.fileRunLumi = json.load(f)
    return self.fileRunLumi

  def getFileRepresentativeRunLumi(self):
    if self.fileRepresentativeRunLumi is None:
      fileRunLumi = self.getFileRunLumi()
      self.fileRepresentativeRunLumi = {}
      for file, fileRuns in fileRunLumi.items():
        def hasOverlaps(run, lumi):
          for otherFile, otherFileRuns in fileRunLumi.items():
            if otherFile != file and run in otherFileRuns and lumi in otherFileRuns[run]:
              return True
          return False
        def findFirstRepresentative():
          for fileRun, runLumis in fileRuns.items():
            for runLumi in runLumis:
              if not hasOverlaps(fileRun, runLumi):
                return (fileRun, runLumi)
          print(f"{self.name}: Unable to find representative (run, lumi) for {file}. Using the first one.")
          fileRun = next(iter(fileRuns))
          runLumi = fileRuns[fileRun][0]
          return (fileRun, runLumi)
        self.fileRepresentativeRunLumi[file] = findFirstRepresentative()
    return self.fileRepresentativeRunLumi

  def getRepresentativeLumiMask(self, files):
    lumiMask = {}
    repRunLumi = self.getFileRepresentativeRunLumi()
    for file in files:
      if file not in repRunLumi:
        raise RuntimeError(f'{self.name}: cannot find representative run-lumi for "{file}"')
      run, lumi = repRunLumi[file]
      run = str(run)
      if run not in lumiMask:
        lumiMask[run] = []
      lumiMask[run].append([lumi, lumi])
    return lumiMask

  def selectJobIds(self, jobStatuses, invert=False, recoveryIndex=None):
    jobIds = []
    for jobId, status in self.getTaskStatus(recoveryIndex).get_job_status().items():
      if (status in jobStatuses and not invert) or (status not in jobStatuses and invert):
        jobIds.append(jobId)
    return jobIds

  def getTimeSinceLastJobStatusUpdate(self):
    if self.lastJobStatusUpdate <= 0:
      return -1
    now = datetime.datetime.now()
    t = datetime.datetime.fromtimestamp(self.lastJobStatusUpdate)
    return (now - t).total_seconds() / (60 * 60)

  def getTaskStatus(self, recoveryIndex=None):
    if recoveryIndex is None:
      recoveryIndex = self.recoveryIndex
    if recoveryIndex == self.recoveryIndex:
      return self.taskStatus
    statusPath = os.path.join(self.workArea, f'status_{recoveryIndex}.json')
    with open(statusPath, 'r') as f:
      return CrabTaskStatus.from_json(f.read())

  def getTaskId(self, recoveryIndex=None):
    if recoveryIndex is None:
      recoveryIndex = self.recoveryIndex
    if recoveryIndex not in self.taskIds:
      self.taskIds[recoveryIndex] = self.getTaskStatus(recoveryIndex=recoveryIndex).task_id()
      self.saveCfg()
    return self.taskIds[recoveryIndex]

  def postProcessOutputs(self, job_home):
    missingFiles = self.getFilesToProcess()
    if len(missingFiles) > 0:
      raise RuntimeError(f'{self.name}: missing outputs for following input files: ' + ' '.join(missingFiles))
    haddnanoEx_path = os.path.join(os.path.dirname(__file__), 'haddnanoEx.py')
    datasetFiles = self.getDatasetFiles()
    processedFiles = self.getProcessedFiles()
    notProcessedFiles = sorted(list(datasetFiles.keys() - processedFiles.keys()))
    for output in self.getOutputs():
      outputName = output['file']
      outputNameBase, outputExt = os.path.splitext(outputName)
      print(f'{self.name}: merging outputs for {outputName}...')

      report = {}
      report['inputDataset'] = self.inputDataset
      report['processingStart'] = self.startDate
      report['notProcessedFiles'] = notProcessedFiles
      haddInputs = {}
      file_list_path = os.path.join(job_home, 'file_list.txt')
      with open(file_list_path, 'w') as f:
        for fileName, fileDesc in processedFiles.items():
          x = fileDesc['outputs'][outputName]
          haddInputs[x] = fileName
          f.write(x + '\n')

      hadd_report_path = os.path.join(job_home, 'merge_report.json')
      cmd = [ 'python3', '-u', haddnanoEx_path, '--output-dir', output['finalOutput'], '--output-name', outputName,
             '--target-size', str(self.targetOutputFileSize), '--file-list', file_list_path, '--remote-io',
             '--work-dir', job_home, '--merge-report', hadd_report_path]
      ps_call(cmd, verbose=1)
      with open(hadd_report_path, 'r') as f:
        hadd_report = json.load(f)
      report['outputs'] = {}
      for haddOutput, inputList in hadd_report.items():
        report['outputs'][haddOutput] = {}
        for haddInput in inputList:
          origInput = haddInputs[haddInput]
          report['outputs'][haddOutput][origInput] = self.getFileRunLumi()[origInput]
      report['processingEnd'] = timestamp_str()

      report_file =f'prodReport_{outputNameBase}.json'
      report_tmp_path = os.path.join(job_home, report_file)
      report_final_path = os.path.join(output['finalOutput'], report_file)
      with open(report_tmp_path, 'w') as f:
        json.dump(report, f, indent=2)
      gfal_copy_safe(report_tmp_path, report_final_path, self.getVomsToken(), verbose=1)

  def getPostProcessingDoneFlagFile(self):
    return os.path.join(self.workArea, 'post_processing_done.txt')

  def getGridJobDoneFlagDir(self):
    return os.path.join(self.workArea, 'grid_jobs_results')

  def getGridJobDoneFlagFile(self, job_id):
    return os.path.join(self.getGridJobDoneFlagDir(), f'job_{job_id}.done')

  def hasFailedJobs(self):
    return JobStatus.failed in self.taskStatus.job_stat

  def crabArea(self, recoveryIndex=None):
    return os.path.join(self.workArea, 'crab_' + self.requestName(recoveryIndex))

  def lastCrabStatusLog(self):
    return os.path.join(self.workArea, 'lastCrabStatus.txt')

  def submit(self, lawTaskManager=None):
    self.getDatasetFiles()
    if self.isInLocalRunMode():
      self.taskStatus = CrabTaskStatus()
      self.taskStatus.status = Status.SubmittedToLocal
      self.taskStatus.status_on_server = StatusOnServer.SUBMITTED
      self.taskStatus.status_on_scheduler = StatusOnScheduler.SUBMITTED
      for job_id in self.getGridJobs(lawTaskManager=lawTaskManager):
        self.taskStatus.details[str(job_id)] = { "State": "idle" }
      self.saveStatus()
      return True
    else:
      crabSubmitPath = os.path.join(os.path.dirname(__file__), 'crabSubmit.py')
      if self.recoveryIndex == 0:
        print(f'{self.name}: submitting ...')
      try:
        timeout = None if self.dryrun else Task.crabOperationTimeout
        ps_call(f'python3 {crabSubmitPath} {self.workArea}', shell=True, timeout=timeout, env=self.getCmsswEnv(),
                singularity_cmd=self.singularity_cmd)
        self.taskStatus.status = Status.Submitted
        self.saveStatus()
      except PsCallError as e:
        crabArea = self.crabArea()
        if os.path.exists(crabArea):
          shutil.rmtree(crabArea)
        raise e
      return False

  def updateStatus(self, lawTaskManager=None):
    neen_local_run = False
    oldTaskStatus = self.taskStatus
    if self.isInLocalRunMode():
      self.taskStatus = CrabTaskStatus()
      self.taskStatus.status = Status.SubmittedToLocal
      self.taskStatus.status_on_server = StatusOnServer.SUBMITTED
      self.taskStatus.status_on_scheduler = StatusOnScheduler.SUBMITTED
      for job_id in self.getGridJobs(lawTaskManager=lawTaskManager):
        job_flag_file = self.getGridJobDoneFlagFile(job_id)
        if os.path.exists(job_flag_file):
          with open(job_flag_file, 'r') as f:
            job_status = f.read().strip()
        else:
          job_status = "idle"
        self.taskStatus.details[str(job_id)] = { "State": job_status }
      jobIds = self.selectJobIds([JobStatus.finished], invert=True)
      if len(jobIds) == 0:
        filesToProcess = self.getFilesToProcess()
        if len(filesToProcess) == 0:
          self.taskStatus.status = Status.CrabFinished
        else:
          for file in filesToProcess:
            file_found = False
            iter_idx = 0
            while (iter_idx < 2) and (not file_found):
              for job_id, job_files in self.getGridJobs(lawTaskManager=lawTaskManager, forceUpdate=iter_idx>0).items():
                if file in job_files:
                  file_found = True
                  done_flag = self.getGridJobDoneFlagFile(job_id)
                  if os.path.exists(done_flag):
                    print(f'{self.name}: job {job_id} is marked as finished, but no output file is found.'
                          ' Removing the done flag and resubmitting the job.')
                    os.remove(done_flag)
                    break
              iter_idx += 1
            if not file_found:
              raise RuntimeError(f'{self.name}: cannot find job for file "{file}"')
      failedJobIds = self.selectJobIds([JobStatus.failed])
      if len(jobIds) == len(failedJobIds) and len(failedJobIds) > 0:
        self.taskStatus.status = Status.Failed
      self.saveStatus()
      neen_local_run = self.taskStatus.status not in [ Status.CrabFinished, Status.Failed ]
    else:
      returncode, output, err = ps_call(f'crab status --json -d {self.crabArea()}', shell=True, verbose=0,
                                        catch_stdout=True, split='\n', timeout=Task.crabOperationTimeout,
                                        env=self.getCmsswEnv(), singularity_cmd=self.singularity_cmd)
      self.taskStatus = LogEntryParser.Parse(output)
      if self.taskStatus.status == Status.CrabFinished:
        filesToProcess = self.getFilesToProcess()
        if len(filesToProcess) != 0:
          self.taskStatus.status = Status.WaitingForRecovery
      self.saveStatus()
      with open(self.lastCrabStatusLog(), 'w') as f:
        f.write('\n'.join(output))
      if self.taskStatus.status == Status.Unknown:
        print(f'{self.name}: {self.taskStatus.status}. Parse error: {self.taskStatus.parse_error}')
      self.getTaskId()
    now = datetime.datetime.now()
    hasUpdates = self.lastJobStatusUpdate <= 0
    if not hasUpdates:
      jobStatus = self.taskStatus.get_job_status()
      oldJobStatus = oldTaskStatus.get_job_status()
      if len(jobStatus) != len(oldJobStatus):
        hasUpdates = True
      else:
        for jobId, status in self.taskStatus.get_job_status().items():
          if jobId not in oldJobStatus or status != oldJobStatus[jobId]:
            hasUpdates = True
            break
    if hasUpdates:
      self.lastJobStatusUpdate = now.timestamp()
      self.saveCfg()
    return neen_local_run

  def recover(self, lawTaskManager=None):
    filesToProcess = self.getFilesToProcess()
    if len(filesToProcess) == 0:
      print(f'{self.name}: no recovery is needed. All files have been processed.')
      self.taskStatus.status = Status.CrabFinished
      self.saveStatus()
      return False

    if self.isInLocalRunMode(recoveryIndex=self.recoveryIndex+1):
      print(f'{self.name}: creating a local recovery task\nFiles to process: ' + ', '.join(filesToProcess))
      if self.recoveryIndex == self.maxRecoveryCount - 1:
        shutil.copy(self.statusPath, os.path.join(self.workArea, f'status_{self.recoveryIndex}.json'))
        self.recoveryIndex += 1
        self.jobInputFiles = None
        self.lastJobStatusUpdate = -1.
        self.saveCfg()
        self.submit(lawTaskManager=lawTaskManager)
      return self.updateStatus()

    jobIds = self.selectJobIds([JobStatus.finished], invert=True)
    lumiMask = self.getRepresentativeLumiMask(filesToProcess)
    msg = f'{self.name}: creating a recovery task. Attempt {self.recoveryIndex + 1}/{self.maxRecoveryCount}.'
    msg += '\nUnfinished job ids: ' + ', '.join(jobIds)
    msg += '\nFiles to process: ' + ', '.join(filesToProcess)
    msg += '\nRepresentative lumi mask: ' + json.dumps(lumiMask)
    print(msg)
    n_lumi = sum([ len(x) for _, x in lumiMask.items()])
    if n_lumi != len(filesToProcess):
      raise RuntimeError(f"{self.name}: number of representative lumi sections != number of files to process.")
    shutil.copy(self.statusPath, os.path.join(self.workArea, f'status_{self.recoveryIndex}.json'))
    self.recoveryIndex += 1
    self.jobInputFiles = None
    self.lastJobStatusUpdate = -1.
    with open(self.getLumiMask(), 'w') as f:
      json.dump(lumiMask, f)
    self.saveCfg()
    try:
      self.submit()
    except PsCallError as e:
      self.recoveryIndex -= 1
      self.saveCfg()
      raise e
    return False

  def gridJobsFile(self):
    return os.path.join(self.workArea, 'grid_jobs.json')

  def getGridJobs(self, lawTaskManager=None, forceUpdate=False):
    if not self.isInLocalRunMode():
      return {}
    if self.gridJobs is None:
      if os.path.exists(self.gridJobsFile()):
        with open(self.gridJobsFile(), 'r') as f:
          self.gridJobs = { int(key) : value for key,value in json.load(f).items() }
    if forceUpdate or self.gridJobs is None:
      if lawTaskManager is None:
        raise RuntimeError(f'{self.name}: lawTaskManager needs to be set to create grid jobs.')
      prev_files = set()
      if self.gridJobs is None:
        self.gridJobs = {}
        job_id = 0
      else:
        job_id = max(self.gridJobs.keys()) + 1
        prev_files.update(*self.gridJobs.values())
      units_per_job = self.getUnitsPerJob()
      for file in self.getFilesToProcess():
        if file in prev_files: continue
        while True:
          if job_id not in self.gridJobs:
            self.gridJobs[job_id] = []
          if len(self.gridJobs[job_id]) < units_per_job:
            self.gridJobs[job_id].append(file)
            break
          else:
            job_id += 1
      for grid_job_id in self.gridJobs:
        lawTaskManager.add(self.workArea, grid_job_id, self.getGridJobDoneFlagFile(grid_job_id))
      with open(self.gridJobsFile(), 'w') as f:
        json.dump(self.gridJobs, f, indent=2)
    return self.gridJobs

  def runJobLocally(self, job_id, job_home):
    print(f'{self.name}: running job {job_id} locally in {job_home}.')
    try:
      if not os.path.exists(job_home):
        os.makedirs(job_home)

      ana_path = os.environ['ANALYSIS_PATH']
      for file in self.getFilesToTransfer(appendDatasetFiles=False):
        shutil.copy(os.path.join(ana_path, file), job_home)
      cmd = [ 'python3', os.path.join(ana_path, self.cmsswPython), f'datasetFiles={self.getDatasetFilesPath()}',
              'writePSet=True', 'mustProcessAllInputs=True' ]
      for param in self.getParams(appendDatasetFiles=False):
        cmd.append(f'"{param}"')
      file_list = [ file for file in self.getGridJobs()[job_id] if file not in self.ignoreFiles ]
      if len(file_list) > 0:
        file_list = ','.join(file_list)
        cmd.append(f'inputFiles={file_list}')
        cmd = ' '.join(cmd)
        ps_call(cmd, cwd=job_home, shell=True, env=self.getCmsswEnv(), singularity_cmd=self.singularity_cmd, verbose=1)
        _, scriptName = os.path.split(self.scriptExe)
        ps_call(os.path.join(job_home, scriptName), shell=True, cwd=job_home, env=self.getCmsswEnv(),
                singularity_cmd=self.singularity_cmd, verbose=1)
      return True
    except:
      print(traceback.format_exc())
      print(f'{self.name}: failed to run job {job_id}.')
    return False


  def kill(self):
    if self.isInLocalRunMode():
      print(f'{self.name}: cannot kill a task with local jobs.')
    else:
      try:
        ps_call(f'crab kill -d {self.crabArea()}', shell=True, timeout=Task.crabOperationTimeout,
                env=self.getCmsswEnv(), singularity_cmd=self.singularity_cmd)
      except PsCallError as e:
        print(f'{self.name}: error sending kill request. {e}')
        print(f'{self.name}: setting status to WaitingForRecovery.')
        self.taskStatus.status = Status.WaitingForRecovery
        self.saveStatus()

  def getProcessedFiles(self, useCacheOnly=False, resetCache=False):
    cache_file = os.path.join(self.workArea, 'processed_files.json')
    has_changes = False
    if resetCache:
      self.processedFilesCache = None
      if os.path.exists(cache_file):
        os.remove(cache_file)

    if self.processedFilesCache is None:
      if os.path.exists(cache_file):
        with open(cache_file, 'r') as f:
          self.processedFilesCache = json.load(f)
      else:
        self.processedFilesCache = {}
        has_changes = True

    ls_result = {}

    def collectOutputs(fileId):
      filePaths = {}
      for output in self.getOutputs():
        if output["file"] not in ls_result:
          ls_result[output["file"]] = {}
          ls_files = gfal_ls_safe(output['crabOutput'], catch_stderr=True, voms_token=self.getVomsToken(), verbose=0)
          if ls_files is not None:
            for file_info in ls_files:
              ls_result[output["file"]][file_info.name] = file_info
        found = False
        for recoveryIndex in range(-1, self.recoveryIndex + 1):
          if recoveryIndex < 0:
            fileName = f'{output["name"]}_{fileId}{output["ext"]}'
          else:
            fileName = f'{output["name"]}_{fileId}_{recoveryIndex}{output["ext"]}'
          filePath = os.path.join(output['crabOutput'], fileName)
          fileNameTmp = fileName + COPY_TMP_SUFFIX
          if fileName in ls_result[output["file"]] and fileNameTmp not in ls_result[output["file"]]:
            filePaths[output['file']] = filePath
            found = True
            break
        if not found:
          return None
      return filePaths

    if not useCacheOnly:
      for file, fileId in self.getDatasetFiles().items():
        if fileId in self.processedFilesCache: continue
        outputPaths = collectOutputs(fileId)
        if outputPaths is not None:
          self.processedFilesCache[file] = {
            'id': fileId,
            'outputs': outputPaths
          }
          has_changes = True

    if has_changes:
      with open(cache_file, 'w') as f:
        json.dump(self.processedFilesCache, f, indent=2)
    return self.processedFilesCache

  def getFilesStats(self, useCacheOnly=True):
    allFiles = set(self.getDatasetFiles().keys())
    processedFiles = set(self.getProcessedFiles(useCacheOnly=useCacheOnly).keys())
    toProcess = allFiles - processedFiles - set(self.ignoreFiles)
    return len(allFiles), len(processedFiles), len(toProcess), len(self.ignoreFiles)

  def getFilesToProcess(self):
    allFiles = set(self.getDatasetFiles().keys())
    processedFiles = set(self.getProcessedFiles().keys())
    return list(allFiles - processedFiles - set(self.ignoreFiles))

  def checkCompleteness(self):
    filesToProcess = self.getFilesToProcess()
    if len(filesToProcess) > 0:
      print(f'{self.name}: task is not complete. The following files still needs to be processed: {filesToProcess}')
      return False
    return True

  def resetGridJobs(self, file=None, lawTaskManager=None, expect_at_least_one_job=True):
    file_found = False
    if file is not None:
      for job_id, job_files in self.getGridJobs(lawTaskManager=lawTaskManager).items():
        if file in job_files:
          file_found = True
          done_flag = self.getGridJobDoneFlagFile(job_id)
          if os.path.exists(done_flag):
            os.remove(done_flag)
          break
    if not file_found:
      self.gridJobs = None
      if os.path.exists(self.gridJobsFile()):
        os.remove(self.gridJobsFile())
      if os.path.exists(self.getGridJobDoneFlagDir()):
        shutil.rmtree(self.getGridJobDoneFlagDir())
      if expect_at_least_one_job and len(self.getGridJobs(lawTaskManager=lawTaskManager)) == 0:
        raise RuntimeError(f'{self.name}: unable to reset grid jobs')

  def checkFilesToProcess(self, lawTaskManager=None, resetStatus=False):
    filesToProcess = self.getFilesToProcess()
    print(f'dataset={self.inputDataset}')
    tmp_dir = tempfile.mkdtemp(dir=os.environ.get('TMPDIR', '/tmp'))
    pfnsPrefix = self.params.get('inputPFNSprefix', None)
    has_status_changes = False
    for file in filesToProcess:
      file_out = os.path.join(tmp_dir, os.path.basename(file))
      print(f'  {file}')
      sources = []
      if pfnsPrefix is not None:
        sources = [ pfnsPrefix + file ]
        expected_adler32sum = None
      else:
        sources, expected_adler32sum = das_file_pfns(file, disk_only=False, return_adler32=True,
                                                     inputDBS=self.inputDBS, verbose=0)
      has_at_least_one_valid_source = False
      for pfn_type, pfn_list in sources.items():
        for pfn in pfn_list:
          ok = True
          try:
            if pfn_type == 'TAPE':
              msg = 'no user access'
              ok = False
            else:
              gfal_copy(pfn, file_out, voms_token=self.getVomsToken(), verbose=0)
              if expected_adler32sum is not None:
                asum = adler32sum(file_out)
                if asum != expected_adler32sum:
                  msg = f'adler32sum mismatch. Expected = {expected_adler32sum:x}, got = {asum:x}.'
                  ok = False
          except GfalError as e:
            msg = 'gfal-copy failed'
            ok = False
          if os.path.exists(file_out):
            os.remove(file_out)
          if ok:
            msg = "OK"
            has_at_least_one_valid_source = True
          print(f'    {pfn}: type={pfn_type} {msg}')
      if has_at_least_one_valid_source and resetStatus:
        has_status_changes = True
        self.resetGridJobs(file=file, lawTaskManager=lawTaskManager)
        self.recoveryIndex = self.maxRecoveryCount
        self.taskStatus.status = Status.SubmittedToLocal
    if has_status_changes:
      self.saveStatus()
      self.saveCfg()

  def ignoreMissingFiles(self, lawTaskManager=None):
    filesToProcess = self.getFilesToProcess()
    if len(filesToProcess) == 0:
      return
    self.ignoreFiles = list(set(self.ignoreFiles) | set(filesToProcess))
    print(f'{self.name}: ignoring missing files: ' + ', '.join(filesToProcess))
    self.resetGridJobs(lawTaskManager=lawTaskManager, expect_at_least_one_job=False)
    self.recoveryIndex = self.maxRecoveryCount
    self.taskStatus.status = Status.SubmittedToLocal
    self.saveStatus()
    self.saveCfg()

  def checkProcessedFiles(self, lawTaskManager=None, resetStatus=False):
    processedFiles = self.getProcessedFiles()
    print(f'{self.name}: checking processed files...')
    tmp_dir = tempfile.mkdtemp(dir=os.environ.get('TMPDIR', '/tmp'))

    def check_file(file):
      file_out = os.path.join(tmp_dir, 'tmp.root')
      ok = True
      try:
        gfal_copy_safe(file, file_out, self.getVomsToken(), verbose=0)
      except GfalError as e:
        ok = False
      if os.path.exists(file_out):
        os.remove(file_out)
      return ok

    invalid_files = []
    all_ok = True
    for input_file, entry in sorted(processedFiles.items(), key=lambda x: x[1]['id']):
      for output_file_name, output_file_path in entry['outputs'].items():
        if not check_file(output_file_path):
          invalid_files.append(output_file_path)
          print(f'  {output_file_path}: invalid')
          if resetStatus and gfal_exists(output_file_path, voms_token=self.getVomsToken()):
            gfal_rm(output_file_path, voms_token=self.getVomsToken())
        else:
          print(f'  {output_file_path}: OK')

    if len(invalid_files) > 0:
      print(f'{self.name}: the following files are invalid: {", ".join(invalid_files)}')
      if resetStatus:
        self.getProcessedFiles(resetCache=True)
        filesToProcess = self.getFilesToProcess()
        if len(filesToProcess) == 0:
          raise RuntimeError(f'{self.name}: unable to reset status.')
        for file in filesToProcess:
          self.resetGridJobs(file=file, lawTaskManager=lawTaskManager)
        self.recoveryIndex = self.maxRecoveryCount
        self.taskStatus.status = Status.SubmittedToLocal
        self.saveStatus()
        self.saveCfg()

  def updateConfig(self, mainCfg, taskCfg):
    taskName = self.name
    customTask = type(taskCfg[taskName]) == dict
    for pName in Task._taskCfgProperties:
      self._setFromCfg(pName, mainCfg, add=False)
      if 'config' in taskCfg:
        self._setFromCfg(pName, taskCfg['config'], add=True)
      if customTask:
        self._setFromCfg(pName, taskCfg[taskName], add=True)
    if customTask:
      inputDataset = taskCfg[taskName]['inputDataset']
    else:
      inputDataset = taskCfg[taskName]
    if inputDataset != self.inputDataset:
      raise RuntimeError(f'{self.name}: change of input dataset is not possible')
    for job_id in self.getGridJobs():
      job_id = str(job_id)
      if job_id in self.taskStatus.details and self.taskStatus.details[job_id]["State"] == "failed":
        job_flag_file = self.getGridJobDoneFlagFile(job_id)
        if os.path.exists(job_flag_file):
          os.remove(job_flag_file)
    self.getOutputs(forceUpdate=True)
    self.saveCfg()
    if self.taskStatus.status == Status.Failed:
      self.taskStatus.status = Status.WaitingForRecovery
      self.saveStatus()

  def updateStatusFromFile(self, statusPath=None, not_exists_ok=True):
    if statusPath is None:
      statusPath = self.statusPath
    if os.path.exists(statusPath):
      with open(statusPath, 'r') as f:
        self.taskStatus = CrabTaskStatus.from_json(f.read())
      return True
    if not not_exists_ok:
      raise RuntimeError(f'{self.name}: Unable to update config from "{statusPath}".')

  @staticmethod
  def Load(workArea=None, mainWorkArea=None, taskName=None):
    task = Task()
    if (workArea is not None and (mainWorkArea is not None or taskName is not None)) \
        or (mainWorkArea is not None and taskName is None) or (workArea is None and mainWorkArea is None):
      raise RuntimeError("ambiguous Task.Load params")
    if workArea is not None:
      task.workArea = workArea
    else:
      task.name = taskName
      task.workArea = os.path.join(mainWorkArea, taskName)
    task.cfgPath = os.path.join(task.workArea, 'cfg.json')
    task.statusPath = os.path.join(task.workArea, 'status.json')
    with open(task.cfgPath, 'r') as f:
      cfg = json.load(f)
    for pName in Task._taskCfgPrivateProperties + Task._taskCfgProperties:
      task._setFromCfg(pName, cfg, add=False)
    task.updateStatusFromFile()
    return task

  @staticmethod
  def Create(mainWorkArea, mainCfg, taskCfg, taskName):
    task = Task()
    task.startDate = timestamp_str()
    task.workArea = os.path.join(mainWorkArea, taskName)
    task.cfgPath = os.path.join(task.workArea, 'cfg.json')
    task.statusPath = os.path.join(task.workArea, 'status.json')
    if os.path.exists(task.workArea):
      raise RuntimeError(f'Task with name "{taskName}" already exists.')
    os.mkdir(task.workArea)
    customTask = type(taskCfg[taskName]) == dict
    for pName in Task._taskCfgProperties:
      task._setFromCfg(pName, mainCfg, add=False)
      if 'config' in taskCfg:
        task._setFromCfg(pName, taskCfg['config'], add=True)
      if customTask:
        task._setFromCfg(pName, taskCfg[taskName], add=True)
    task.taskStatus.status = Status.Defined
    if customTask:
      task.inputDataset = taskCfg[taskName]['inputDataset']
    else:
      task.inputDataset = taskCfg[taskName]
    task.name = taskName
    task.getOutputs(forceUpdate=True)
    task.saveCfg()
    task.saveStatus()
    return task

if __name__ == "__main__":
  import sys

  workArea = sys.argv[1]
  task = Task.Load(workArea=workArea)

  # ok = "OK" if task.checkCompleteness(includeNotFinishedFromLastIteration=False) else "INCOMPLETE"
  # print(f'{task.name}: {ok}')
  # print(task.getAllOutputPaths())
  filesToProcess = task.getFilesToProcess()
  print(f'{task.name}: {len(filesToProcess)} {filesToProcess}')
  lumiMask = task.getRepresentativeLumiMask(filesToProcess)
  n_lumi = sum([ len(x) for _, x in lumiMask.items()])
  print(f'{task.name}: {n_lumi} {lumiMask}')

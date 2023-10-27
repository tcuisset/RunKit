import copy
import datetime
import json
import os
import re
import shutil
import sys
import tarfile
import time

if __name__ == "__main__":
  file_dir = os.path.dirname(os.path.abspath(__file__))
  sys.path.append(os.path.dirname(file_dir))
  __package__ = 'RunKit'

from .crabTaskStatus import CrabTaskStatus, Status, JobStatus, LogEntryParser, StatusOnScheduler, StatusOnServer
from .sh_tools import ShCallError, sh_call, natural_sort, get_voms_proxy_info, gfal_copy, gfal_ls_recursive
from .envToJson import get_cmsenv
from .getFileRunLumi import getFileRunLumi

class Task:
  _taskCfgProperties = [
    'cmsswPython', 'params', 'splitting', 'unitsPerJob', 'scriptExe', 'outputFiles', 'filesToTransfer', 'site',
    'crabOutput', 'localCrabOutput', 'lumiMask', 'maxMemory', 'numCores', 'inputDBS', 'allowNonValid',
    'vomsGroup', 'vomsRole', 'blacklist', 'whitelist', 'whitelistFinalRecovery', 'dryrun', 'finalOutput',
    'maxRecoveryCount', 'targetOutputFileSize', 'ignoreFiles', 'ignoreLocality', 'crabType', 'tmp_area'
  ]

  _taskCfgPrivateProperties = [
    'name', 'inputDataset', 'recoveryIndex', 'taskIds', 'lastJobStatusUpdate',
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
    self.splitting = ''
    self.unitsPerJob = -1
    self.scriptExe = ''
    self.outputFiles = []
    self.filesToTransfer = []
    self.site = ''
    self.crabOutput = ''
    self.localCrabOutput = ''
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
    self.finalOutput = ''
    self.targetOutputFileSize = 0
    self.ignoreFiles = []
    self.jobInputFiles = None
    self.datasetFiles = None
    self.fileRunLumi = None
    self.fileRepresentativeRunLumi = None
    self.taskIds = {}
    self.lastJobStatusUpdate = -1.
    self.cmsswEnv = None
    self.singularity_cmd = os.environ.get('CMSSW_SINGULARITY', None)
    self.gridJobs = None
    self.crabType = ''
    self.processedFilesCache = None
    self.tmp_area = ''

  def checkConfigurationValidity(self):
    def check(cond, prop):
      if not cond:
        raise RuntimeError(f'{self.name}: Configuration error: {prop} is not correctly set.')
    def check_len(prop):
      check(len(getattr(self, prop)) > 0, prop)

    for prop in [ 'cmsswPython', 'splitting', 'outputFiles', 'site', 'crabOutput', 'localCrabOutput', 'inputDBS',
                  'finalOutput', 'name', 'inputDataset' ]:
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

  def getParams(self, appendDatasetFiles=True):
    params = [ f'{key}={value}' for key,value in self.params.items() ]
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
    if self.recoveryIndex > 0:
      return 'FileBased'
    return self.splitting

  def getLumiMask(self):
    if self.recoveryIndex > 0:
      return os.path.join(self.workArea, f'{Task.inputLumiMaskJsonName}_{self.recoveryIndex}.json')
    return self.lumiMask

  def getMaxMemory(self):
    if self.recoveryIndex == self.maxRecoveryCount:
      return max(self.maxMemory, 4000)
    return self.maxMemory

  def getWhiteList(self):
    if self.recoveryIndex == self.maxRecoveryCount:
      return self.whitelistFinalRecovery
    return self.whitelist

  def getBlackList(self):
    return self.blacklist

  def getIgnoreLocality(self):
    if self.recoveryIndex == self.maxRecoveryCount:
      return True
    return self.ignoreLocality

  def getFilesToTransfer(self, appendDatasetFiles=True):
    if appendDatasetFiles:
      return self.filesToTransfer + [ self.getDatasetFilesPath() ]
    return self.filesToTransfer

  def getCrabJobOutput(self):
    return 'output.tar'

  def getCmsswEnv(self):
    if self.cmsswEnv is None:
      cmssw_path = os.environ['DEFAULT_CMSSW_BASE']
      self.cmsswEnv = get_cmsenv(cmssw_path, crab_env=True, crab_type=self.crabType)
      self.cmsswEnv['X509_USER_PROXY'] = os.environ['X509_USER_PROXY']
      self.cmsswEnv['HOME'] = os.environ['HOME'] if 'HOME' in os.environ else self.workArea
      if 'KRB5CCNAME' in os.environ:
        self.cmsswEnv['KRB5CCNAME'] = os.environ['KRB5CCNAME']
    return self.cmsswEnv

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
          if not os.path.exists(ds_path):
            raise RuntimeError(f'{self.name}: unable to find local dataset path "{ds_path}"')
          self.datasetFiles = {}
          all_files = []
          for subdir, dirs, files in os.walk(ds_path):
            for file in files:
              if file.endswith('.root') and not file.startswith('.'):
                all_files.append('file:' + os.path.join(subdir, file))
          for file_id, file_path in enumerate(natural_sort(all_files)):
            self.datasetFiles[file_path] = file_id
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

  def getTaskOutputPath(self, recoveryIndex=None):
    if recoveryIndex is None:
      recoveryIndex = self.recoveryIndex
    if self.isInLocalRunMode(recoveryIndex=recoveryIndex):
      return os.path.join(self.localCrabOutput, 'local_' + self.requestName())
    else:
      datasetParts = [ s for s in self.inputDataset.split('/') if len(s) > 0 ]
      datasetName = datasetParts[0]
      outputBase = os.path.join(self.localCrabOutput, datasetName)
      return os.path.join(outputBase, 'crab_' + self.requestName(recoveryIndex=recoveryIndex),
                          self.getTaskId(recoveryIndex=recoveryIndex))

  def findOutputFile(self, taskOutput, jobId):
    outputName, outputExt = os.path.splitext(self.getCrabJobOutput())
    fileName = f'{outputName}_{jobId}{outputExt}'
    outputFiles = []
    if taskOutput.startswith("/"):
      for root, dirs, files in os.walk(taskOutput):
        for file in files:
          if file == fileName:
            filePath = os.path.join(root, file)
            outputFiles.append(filePath)
    else:
      outputFiles = gfal_ls_recursive(taskOutput, fileName)
    if len(outputFiles) == 0:
      raise RuntimeError(f'{self.name}: unable to find output for jobId={jobId} outputName={outputName}' + \
                         f' in {taskOutput}')
    if len(outputFiles) > 1:
      raise RuntimeError(f'{self.name}: duplicated outputs for jobId={jobId} outputName={outputName}' + \
                         f' in {taskOutput}: ' + ' '.join(outputFiles))
    return outputFiles[0]

  def getTarFileFromGfal(self, outputFile):
    tempdir = os.environ['TMPDIR'] if self.tmp_area == 'TMPDIR' else self.tmp_area
    temp_root = os.path.join(tempdir, outputFile.split("/")[-1])
    gfal_copy(outputFile, temp_root, get_voms_proxy_info()['path'], verbose=0)
    return temp_root

  def getProcessedFilesFromTar(self, outputFile):
    outputName, outputExt = os.path.splitext(self.outputFiles[0])
    files = {}
    nonLocal = False
    if not outputFile.startswith("/"):
      outputFile = self.getTarFileFromGfal(outputFile)
      nonLocal = True
    with tarfile.open(outputFile, 'r') as tar:
      files_in_tar = tar.getnames()
      for file_in_tar in files_in_tar:
        if file_in_tar.startswith(outputName):
          file_id = file_in_tar[len(outputName) + 1:-len(outputExt)]
          file = self.getDatasetFileById(file_id)
          if file in files:
            raise RuntimeError(f'{self.name}: duplicated file {file} in {outputFile}')
          files[file] = file_id
    if nonLocal:
      sh_call(['rm', outputFile,], shell=False, verbose=0)
    return files

  def getPostProcessList(self):
    return os.path.join(self.workArea, 'postProcessList.json')

  def preparePostProcessList(self):
    listFile = self.getPostProcessList()
    if not os.path.exists(listFile):
      allFiles = set(self.getDatasetFiles().keys())
      processedFiles, outputFiles = self.getProcessedFiles()
      missingFiles = list(allFiles - processedFiles - set(self.ignoreFiles))
      if len(missingFiles) > 0:
        raise RuntimeError(f'{self.name}: missing outputs for following input files: ' + ' '.join(missingFiles))
      with open(listFile, 'w') as f:
        json.dump(outputFiles, f, indent=2)

  def getFinalOutput(self):
    return os.path.join(self.finalOutput, self.name)

  def extractTarOutputs(self, outputIndex, max_tries=4, try_delay=10):
    outputName, outputExt = os.path.splitext(self.outputFiles[outputIndex])
    outputDir = os.path.join(self.finalOutput, f'.{self.name}.untar')
    if os.path.exists(outputDir):
      shutil.rmtree(outputDir)
    os.makedirs(outputDir, exist_ok=True)
    unpackedFiles = []
    with open(self.getPostProcessList(), 'r') as f:
      tarFiles = json.load(f)
    for tarFile, packedFiles in tarFiles.items():
      print(tarFile)
      nonLocal = False
      if not tarFile.startswith("/"):
        tarFile = self.getTarFileFromGfal(tarFile)
        nonLocal = True
      with tarfile.open(tarFile, 'r') as tar:
        for _, packedFileId in packedFiles:
          packedFile = f'{outputName}_{packedFileId}{outputExt}'
          packedSize = tar.getmember(packedFile).size
          for try_idx in range(max_tries):
            try:
              print(f'  {packedFile}', end=' ', flush=True)
              unpackedFile = os.path.join(outputDir, packedFile)
              if os.path.exists(unpackedFile):
                os.remove(unpackedFile)
              tar.extract(packedFile, outputDir)
              unpackedSize = os.path.getsize(unpackedFile)
              if unpackedSize != packedSize:
                raise RuntimeError(f"Unpacked file size = {unpackedSize} doesn't match the packed size = {packedSize}.")
              print('ok')
              break
            except (OSError, RuntimeError) as e:
              if try_idx == max_tries - 1:
                print('failed')
                raise RuntimeError(f'{self.name}: unable to extract {packedFile} from {tarFile}: {e}')
              else:
                print(f'failed (attempt {try_idx+1}/{max_tries})')
                time.sleep(try_delay)
          unpackedFiles.append(os.path.join(outputDir, packedFile))
      if nonLocal:
        sh_call(['rm', tarFile,], shell=False, verbose=0)
    unpackedList = self.getPostProcessList() + f'.{outputName}.unpacked'
    with open(unpackedList, 'w') as f:
      for file in unpackedFiles:
        f.write(file + '\n')
    return unpackedList, outputDir

  def postProcessOutputs(self):
    haddnanoEx_path = os.path.join(os.path.dirname(__file__), 'haddnanoEx.py')
    for outputIndex in range(len(self.outputFiles)):
      outputName = self.outputFiles[outputIndex]
      outputNameBase, outputExt = os.path.splitext(outputName)
      print(f'{self.name}: extracting outputs for {outputName} from tars...')
      unpackedList, unpackedDir = self.extractTarOutputs(outputIndex)
      cmd = [ 'python3', '-u', haddnanoEx_path, '--output-dir', self.getFinalOutput(),
              '--output-name', outputName, '--target-size', str(self.targetOutputFileSize),
              '--file-list', unpackedList ]
      _, output, _ = sh_call(cmd, catch_stdout=True, catch_stderr=True, print_output=True, verbose=1)
      with open(os.path.join(self.workArea, f'postProcessing_{outputNameBase}.log'), 'w') as f:
        f.write(output)
      if os.path.exists(unpackedDir):
        shutil.rmtree(unpackedDir)
      if os.path.exists(unpackedList):
        os.remove(unpackedList)
    self.taskStatus.status = Status.PostProcessingFinished
    self.saveStatus()

  def getPostProcessingDoneFlagFile(self):
    return os.path.join(self.workArea, 'post_processing_done.txt')

  def getGridJobDoneFlagFile(self, job_id):
    return os.path.join(self.workArea, 'grid_jobs_results', f'job_{job_id}.done')

  def hasFailedJobs(self):
    return JobStatus.failed in self.taskStatus.job_stat

  def crabArea(self, recoveryIndex=None):
    return os.path.join(self.workArea, 'crab_' + self.requestName(recoveryIndex))

  def lastCrabStatusLog(self):
    return os.path.join(self.workArea, 'lastCrabStatus.txt')

  def submit(self):
    self.getDatasetFiles()
    if self.isInLocalRunMode():
      self.taskStatus = CrabTaskStatus()
      self.taskStatus.status = Status.Submitted
      self.taskStatus.status_on_server = StatusOnServer.SUBMITTED
      self.taskStatus.status_on_scheduler = StatusOnScheduler.SUBMITTED
      for job_id in self.getGridJobs():
        self.taskStatus.details[str(job_id)] = { "State": "idle" }
      self.saveStatus()
      return True
    else:
      crabSubmitPath = os.path.join(os.path.dirname(__file__), 'crabSubmit.py')
      if self.recoveryIndex == 0:
        print(f'{self.name}: submitting ...')
      try:
        timeout = None if self.dryrun else Task.crabOperationTimeout
        sh_call(['python3', crabSubmitPath, self.workArea], timeout=timeout, env=self.getCmsswEnv(),
                singularity_cmd=self.singularity_cmd)
        self.taskStatus.status = Status.Submitted
        self.saveStatus()
      except ShCallError as e:
        crabArea = self.crabArea()
        if os.path.exists(crabArea):
          shutil.rmtree(crabArea)
        raise e
      return False

  def updateStatus(self):
    neen_local_run = False
    oldTaskStatus = self.taskStatus
    if self. isInLocalRunMode():
      self.taskStatus = CrabTaskStatus()
      self.taskStatus.status = Status.Submitted
      self.taskStatus.status_on_server = StatusOnServer.SUBMITTED
      self.taskStatus.status_on_scheduler = StatusOnScheduler.SUBMITTED
      for job_id in self.getGridJobs():
        job_flag_file = self.getGridJobDoneFlagFile(job_id)
        if os.path.exists(job_flag_file):
          with open(job_flag_file, 'r') as f:
            job_status = f.read().strip()
        else:
          job_status = "idle"
        self.taskStatus.details[str(job_id)] = { "State": job_status }
      jobIds = self.selectJobIds([JobStatus.finished], invert=True)
      if len(jobIds) == 0:
        self.taskStatus.status = Status.CrabFinished
      self.saveStatus()
      neen_local_run = self.taskStatus.status != Status.CrabFinished
    else:
      returncode, output, err = sh_call(['crab', 'status', '--json', '-d', self.crabArea()],
                                        catch_stdout=True, split='\n', timeout=Task.crabOperationTimeout,
                                        env=self.getCmsswEnv(), singularity_cmd=self.singularity_cmd)
      self.taskStatus = LogEntryParser.Parse(output)
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

  def recover(self):
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
        self.submit()
        return True
      else:
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
    except ShCallError as e:
      self.recoveryIndex -= 1
      self.saveCfg()
      raise e
    return False

  def gridJobsFile(self):
    return os.path.join(self.workArea, 'grid_jobs.json')

  def getGridJobs(self):
    if not self.isInLocalRunMode():
      return {}
    if self.gridJobs is None:
      if os.path.exists(self.gridJobsFile()):
        with open(self.gridJobsFile(), 'r') as f:
          self.gridJobs = { int(key) : value for key,value in json.load(f).items() }
      else:
        self.gridJobs = {}
        job_id = 0
        units_per_job = self.getUnitsPerJob()
        for file in self.getFilesToProcess():
          while True:
            if job_id not in self.gridJobs:
              self.gridJobs[job_id] = []
            if len(self.gridJobs[job_id]) < units_per_job:
              self.gridJobs[job_id].append(file)
              break
            else:
              job_id += 1
        with open(self.gridJobsFile(), 'w') as f:
          json.dump(self.gridJobs, f, indent=2)
    return self.gridJobs

  def runJobLocally(self, job_id, job_home):
    print(f'{self.name}: running job {job_id} locally in {job_home}.')
    if not os.path.exists(job_home):
      os.makedirs(job_home)

    ana_path = os.environ['ANALYSIS_PATH']
    for file in self.getFilesToTransfer(appendDatasetFiles=False):
      shutil.copy(os.path.join(ana_path, file), job_home)
    cmd = [ 'python3', os.path.join(ana_path, self.cmsswPython), f'datasetFiles={self.getDatasetFilesPath()}',
            'writePSet=True', 'mustProcessAllInputs=True' ]
    cmd.extend(self.getParams(appendDatasetFiles=False))
    file_list = [ file for file in self.getGridJobs()[job_id] if file not in self.ignoreFiles ]
    job_output = os.path.join(job_home, self.getCrabJobOutput())
    if len(file_list) > 0:
      file_list = ','.join(file_list)
      cmd.append(f'inputFiles={file_list}')
      sh_call(cmd, cwd=job_home, env=self.getCmsswEnv(), singularity_cmd=self.singularity_cmd)
      _, scriptName = os.path.split(self.scriptExe)
      sh_call([ os.path.join(job_home, scriptName) ], shell=True, cwd=job_home, env=self.getCmsswEnv(),
              singularity_cmd=self.singularity_cmd)
    else:
      tar = tarfile.open(job_output, 'w')
      tar.close()
    if not os.path.exists(job_output):
      raise RuntimeError(f'{self.name}: job output file {job_output} was not produced.')
    output_path = self.getTaskOutputPath()
    if not os.path.exists(output_path):
      os.makedirs(output_path)
    out_name, out_ext = os.path.splitext(self.getCrabJobOutput())
    final_output = os.path.join(output_path, f'{out_name}_{job_id}{out_ext}')
    shutil.move(job_output, final_output)
    return True

  def kill(self):
    if self.isInLocalRunMode():
      print(f'{self.name}: cannot kill a task with local jobs.')
    else:
      sh_call(['crab', 'kill', '-d', self.crabArea()], timeout=Task.crabOperationTimeout, env=self.getCmsswEnv(),
              singularity_cmd=self.singularity_cmd)

  def getProcessedFiles(self, lastRecoveryIndex=None):
    if lastRecoveryIndex is None:
      lastRecoveryIndex = self.recoveryIndex
    cache_file = os.path.join(self.workArea, 'processed_files.json')
    has_changes = False
    if self.processedFilesCache is None:
      if os.path.exists(cache_file):
        with open(cache_file, 'r') as f:
          self.processedFilesCache = json.load(f)
      else:
        self.processedFilesCache = {}
        has_changes = True

    def getFiles(recoveryIndex, taskOutput, jobId):
      nonlocal has_changes
      if recoveryIndex not in self.processedFilesCache:
        self.processedFilesCache[recoveryIndex] = {}
      if jobId not in self.processedFilesCache[recoveryIndex]:
        outputFile = self.findOutputFile(taskOutput, jobId)
        files = self.getProcessedFilesFromTar(outputFile)
        self.processedFilesCache[recoveryIndex][jobId] = {
          'outputFile' : outputFile,
          'files' : files
        }
        has_changes = True
      entry = self.processedFilesCache[recoveryIndex][jobId]
      return entry['outputFile'], entry['files']

    processedFiles = set()
    outputFiles = {}
    for recoveryIndex in range(lastRecoveryIndex + 1):
      taskOutput = self.getTaskOutputPath(recoveryIndex=recoveryIndex)
      jobIds = self.selectJobIds([JobStatus.finished], recoveryIndex=recoveryIndex)
      for jobId in jobIds:
        outputFile, files = getFiles(str(recoveryIndex), taskOutput, jobId)
        for file, file_id in files.items():
          if file not in processedFiles:
            if outputFile not in outputFiles:
              outputFiles[outputFile] = []
            outputFiles[outputFile].append([file, file_id])
            processedFiles.add(file)
    if has_changes:
      with open(cache_file, 'w') as f:
        json.dump(self.processedFilesCache, f, indent=2)
    return processedFiles, outputFiles

  def getFilesToProcess(self, lastRecoveryIndex=None):
    allFiles = set(self.getDatasetFiles().keys())
    processedFiles, _ = self.getProcessedFiles(lastRecoveryIndex=lastRecoveryIndex)
    return list(allFiles - processedFiles - set(self.ignoreFiles))

  def checkCompleteness(self):
    filesToProcess = self.getFilesToProcess()
    if len(filesToProcess):
      print(f'{self.name}: task is not complete. The following files still needs to be processed: {filesToProcess}')
      return False
    return True

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

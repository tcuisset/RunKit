#from http.client import HTTPException
import traceback
import sys
import os

if __name__ == "__main__":
  file_dir = os.path.dirname(os.path.abspath(__file__))
  sys.path.append(os.path.dirname(file_dir))
  __package__ = 'RunKit'

from .crabTask import Task

from CRABClient.UserUtilities import ClientException
from CRABClient.UserUtilities import config as Config
from CRABAPI.RawCommand import crabCommand

def submit(task: Task):

  config = Config()

  config.General.workArea = task.workArea
  config.General.transferOutputs = False
  config.General.transferLogs = False
  config.General.requestName = task.requestName()

  config.JobType.pluginName = 'Analysis'
  config.JobType.psetName = task.cmsswPython
  config.JobType.maxMemoryMB = task.getMaxMemory()
  config.JobType.numCores = task.numCores
  if len(task.scriptExe) > 0:
    config.JobType.scriptExe = task.scriptExe
  config.JobType.inputFiles = task.getFilesToTransfer()
  config.JobType.disableAutomaticOutputCollection = True
  config.JobType.pyCfgParams = task.getParams()

  config.Data.inputDBS = task.inputDBS
  config.Data.allowNonValidInputDataset = task.allowNonValid
  config.Data.publication = False
  config.Data.unitsPerJob = task.getUnitsPerJob()
  config.Data.splitting = task.getSplitting()
  config.Data.lumiMask = task.getLumiMask()
  config.Data.inputDataset = task.inputDataset
  config.Data.ignoreLocality = task.getIgnoreLocality()

  config.Site.storageSite = 'T2_CH_CERN' # needed for CRABClient to work
  blacklist = task.getBlackList()
  if len(blacklist) != 0:
    config.Site.blacklist = blacklist

  whitelist = task.getWhiteList()
  if len(whitelist) != 0:
    config.Site.whitelist = whitelist

  if len(task.vomsGroup) != 0:
    config.User.voGroup = task.vomsGroup
  if len(task.vomsRole) != 0:
    config.User.voRole = task.vomsRole

  crabCommand('submit', config=config, dryrun=task.dryrun)

if __name__ == "__main__":
  try:
    workArea = sys.argv[1]
    task = Task.Load(workArea=workArea)
    submit(task)
  except:
    print(traceback.format_exc())
    sys.exit(1)

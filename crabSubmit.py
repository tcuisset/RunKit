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
  # config.section_("Debug")
  # config.Debug.extraJDL = ["+CMS_ALLOW_OVERFLOW=False"]

  config.JobType.pluginName = 'Analysis'
  config.JobType.psetName = task.cmsswPython
  config.JobType.maxMemoryMB = task.getMaxMemory()
  config.JobType.numCores = task.numCores

  if len(task.scriptExe) > 0:
    config.JobType.scriptExe = task.scriptExe
  config.JobType.inputFiles = task.getFilesToTransfer()
  config.JobType.outputFiles = [ ]

  config.Data.inputDBS = task.inputDBS
  config.Data.allowNonValidInputDataset = task.allowNonValid
  config.General.transferOutputs = True
  config.General.transferLogs = False
  config.Data.publication = False

  #config.Site.storageSite = task.site

  if len(task.vomsGroup) != 0:
    config.User.voGroup = task.vomsGroup
  if len(task.vomsRole) != 0:
    config.User.voRole = task.vomsRole

  config.Data.outLFNDirBase = task.crabOutput

  blacklist = task.getBlackList()
  if len(blacklist) != 0:
    config.Site.blacklist = blacklist

  whitelist = task.getWhiteList()
  if len(whitelist) != 0:
    config.Site.whitelist = whitelist

  config.JobType.pyCfgParams = task.getParams()
  config.Data.unitsPerJob = task.getUnitsPerJob()
  config.Data.splitting = task.getSplitting()
  config.Data.lumiMask = task.getLumiMask()
  config.General.requestName = task.requestName()
  config.Data.inputDataset = task.inputDataset

  config.Data.ignoreLocality = task.getIgnoreLocality()

  crabCommand('submit', config=config, dryrun=task.dryrun)

if __name__ == "__main__":
  try:
    workArea = sys.argv[1]
    task = Task.Load(workArea=workArea)
    submit(task)
  except:
    print(traceback.format_exc())
    sys.exit(1)

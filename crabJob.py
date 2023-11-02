import datetime
import importlib.util
import json
import os
import shutil
import sys
import tarfile
import traceback

from RunKit.run_tools import PsCallError, copy_remote_file

_error_msg_fmt = '''
<FrameworkError ExitStatus="{}" Type="Fatal error" >
<![CDATA[
{}
]]>
</FrameworkError>'''

_job_report_fmt = '''
<FrameworkJobReport>
<ReadBranches>
</ReadBranches>
<PerformanceReport>
  <PerformanceSummary Metric="StorageStatistics">
    <Metric Name="Parameter-untracked-bool-enabled" Value="true"/>
    <Metric Name="Parameter-untracked-bool-stats" Value="true"/>
    <Metric Name="Parameter-untracked-string-cacheHint" Value="application-only"/>
    <Metric Name="Parameter-untracked-string-readHint" Value="auto-detect"/>
    <Metric Name="ROOT-tfile-read-totalMegabytes" Value="0"/>
    <Metric Name="ROOT-tfile-write-totalMegabytes" Value="0"/>
  </PerformanceSummary>
</PerformanceReport>

<GeneratorInfo>
</GeneratorInfo>
{}
</FrameworkJobReport>
'''

_cmssw_report_name = 'cmssw_report'
_cmssw_report_ext = '.xml'
_cmssw_report = _cmssw_report_name + _cmssw_report_ext
_final_report = 'FrameworkJobReport.xml'
_tmp_report = _final_report + '.tmp'

def make_job_report(exit_code, exit_message=''):
  if exit_code == 0:
    error_msg = ''
  else:
    error_msg = _error_msg_fmt.format(exit_code, exit_message)
  report_str = _job_report_fmt.format(error_msg)
  with open(_tmp_report, 'w') as f:
    f.write(report_str)
  shutil.move(_tmp_report, _final_report)

def exit(exit_code, exit_message=''):
  if exit_code == 0 and os.path.exists(_cmssw_report):
    shutil.move(_cmssw_report, _final_report)
  else:
    make_job_report(exit_code, exit_message)
  if exit_code != 0:
    sys_exit = exit_code if exit_code >= 0 and exit_code <= 255 else 1
    sys.exit(sys_exit)

def getFilePath(file):
  if os.path.exists(file):
    return file
  file_name = os.path.basename(file)
  if os.path.exists(file_name):
    return file_name
  raise RuntimeError(f"Unable to find {file}")

def load(module_file):
  module_path = module_file
  if not os.path.exists(module_path):
    module_path = os.path.join(os.path.dirname(__file__), module_file)
    if not os.path.exists(module_path):
      module_path = os.path.join(os.getenv("CMSSW_BASE"), 'src', module_file)
      if not os.path.exists(module_path):
        raise RuntimeError(f"Cannot find path to {module_file}.")

  module_name, module_ext = os.path.splitext(module_file)
  spec = importlib.util.spec_from_file_location(module_name, module_path)
  module = importlib.util.module_from_spec(spec)
  sys.modules[module_name] = module
  spec.loader.exec_module(module)
  return module

def convertParams(cfg_params):
  class Params: pass
  params = Params()
  for param_name, param_value in cfg_params.parameters_().items():
    setattr(params, param_name, param_value.value())
  return params

def projectTime(timestamps, verbose=1):
  if len(timestamps) < 2:
    return 0
  delta_t = (timestamps[-1] - timestamps[0]).total_seconds() / 60 / 60
  n_processed = len(timestamps) - 1
  projected_time = delta_t * (n_processed + 1) / n_processed
  if verbose > 0:
    print(f"Processed {n_processed} files in {delta_t:.2f} hours." + \
          f" Projected {projected_time:.2f} hours to process {n_processed + 1} files.")
  return projected_time

def processFile(jobModule, file_id, input_file, output_file, cmd_line_args, params):
  cmssw_report = f'{_cmssw_report_name}_{file_id}{_cmssw_report_ext}'
  result = False
  tmp_files = []
  exception = None
  try:
    if input_file.startswith('file:') or not params.copyInputsToLocal:
      module_input_file = input_file
      local_file = None
    else:
      local_file = f'input_{file_id}.root'
      tmp_files.append(local_file)
      copy_remote_file(input_file, local_file, inputDBS=params.inputDBS, custom_pfns_prefix=params.PFNSprefix, verbose=1)
      module_input_file = f'file:{local_file}'
    jobModule.processFile(module_input_file, output_file, tmp_files, cmssw_report, cmd_line_args, params)
    result = True
  except PsCallError as e:
    print(traceback.format_exc())
    exception = e
  except Exception as e:
    print(traceback.format_exc())
    exception = e
  if os.path.exists(cmssw_report) and (not os.path.exists(_cmssw_report) or result):
    shutil.move(cmssw_report, _cmssw_report)
  for file in tmp_files:
    if os.path.exists(file):
      os.remove(file)
  if not result and os.path.exists(output_file):
    os.remove(output_file)
  return result, exception

def runJob(cmd_line_args):
  timestamps = [ datetime.datetime.now() ]
  pset_path = 'PSet.py'
  if not os.path.exists(pset_path):
    pset_path = os.path.join(os.getenv("CMSSW_BASE"), 'src', 'PSet.py')
  if not os.path.exists(pset_path):
    raise RuntimeError("Cannot find path to PSet.py.")

  PSet = load(pset_path)
  cfg_params = convertParams(PSet.process.exParams)
  cfg_params.maxEvents = PSet.process.maxEvents.input.value()
  jobModule = load(cfg_params.jobModule)
  outputFileBase, outputExt = os.path.splitext(cfg_params.output)
  if len(cfg_params.datasetFiles) > 0:
    with open(cfg_params.datasetFiles, 'r') as f:
      datasetFiles = json.load(f)
  else:
    datasetFiles = None

  output_files = []
  for file_index, file in enumerate(list(PSet.process.source.fileNames)):
    if not cfg_params.mustProcessAllInputs and projectTime(timestamps) > cfg_params.maxRuntime:
      break
    if cfg_params.maxFiles > 0 and file_index >= cfg_params.maxFiles:
      break
    file_id = datasetFiles[file] if datasetFiles else file_index
    output_file = f'{outputFileBase}_{file_id}{outputExt}'
    result, exception = processFile(jobModule, file_id, file, output_file, cmd_line_args, cfg_params)
    timestamps.append(datetime.datetime.now())
    if result:
      output_files.append(output_file)
    else:
      print(f"Failed to process {file}")
      if cfg_params.mustProcessAllInputs:
        raise exception

  if len(output_files) == 0:
    raise RuntimeError("No output files were produced.")

  if cfg_params.createTar:
    tar_name = 'output.tar'
    tmp_tar_name = '.' + tar_name
    with tarfile.open(tmp_tar_name, 'w') as tar:
      for file in output_files:
        tar.add(file)
    shutil.move(tmp_tar_name, tar_name)

if __name__ == "__main__":
  try:
    runJob(sys.argv[1:])
    exit(0)
  except PsCallError as e:
    print(traceback.format_exc())
    exit(e.return_code, str(e))
  except Exception as e:
    print(traceback.format_exc())
    exit(666, str(e))
  except:
    print(traceback.format_exc())
    exit(666, 'Unexpected error')

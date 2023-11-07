import os
import shutil
import sys

if len(__package__) == 0:
  file_dir = os.path.dirname(os.path.abspath(__file__))
  base_dir = os.path.dirname(file_dir)
  if base_dir not in sys.path:
    sys.path.append(base_dir)
  __package__ = os.path.split(file_dir)[-1]

from .run_tools import ps_call

def processFile(input_file, output_file, tmp_files, cmssw_report, cmd_line_args, cfg_params):
  run_cmsRun = True
  store_failed = cfg_params.storeFailed
  skim_cfg = cfg_params.skimCfg
  run_skim = len(skim_cfg) > 0
  debug = len(cmd_line_args) > 0 and cmd_line_args[0] == 'DEBUG'

  if debug:
    if len(cmd_line_args) > 1:
      run_cmsRun = cmd_line_args[1] == 'True'
    if len(cmd_line_args) > 2:
      run_skim = cmd_line_args[2] == 'True'
    if len(cmd_line_args) > 3:
      store_failed = cmd_line_args[3] == 'True'

  if run_cmsRun:
    cmsRunCfg = cfg_params.cmsRunCfg
    cmsRunOptions = cfg_params.cmsRunOptions.split(',') if len(cfg_params.cmsRunOptions) > 0 else []

    customise_commands = cfg_params.customisationCommands
    cfg_name = cmsRunCfg
    if len(customise_commands) > 0:
      cfg_name = f'{cmsRunCfg}_{output_file}.py'
      shutil.copy(cmsRunCfg, cfg_name)
      tmp_files.append(cfg_name)
      with open(cfg_name, 'a') as f:
        f.write('\n' + customise_commands)

    cmssw_cmd = [ 'cmsRun',  '-j', cmssw_report, cfg_name, f'inputFiles={input_file}', f'output={output_file}',
                  f'maxEvents={cfg_params.maxEvents}' ]
    cmssw_cmd.extend(cmsRunOptions)
    ps_call(cmssw_cmd, verbose=1)

  if run_skim:
    output_name, output_ext = os.path.splitext(output_file)
    cmsRun_out = output_name + '_cmsRun' + output_ext
    shutil.move(output_file, cmsRun_out)
    skim_tree_path = os.path.join(os.path.dirname(__file__), 'skim_tree.py')
    cmd_line = ['python3', '-u', skim_tree_path, '--input', cmsRun_out, '--output', output_file,
                '--config', cfg_params.skimCfg, '--setup', cfg_params.skimSetup, '--skip-empty', '--verbose', '1']
    ps_call(cmd_line, verbose=1)

    if store_failed:
      cmd_line = ['python3', '-u', skim_tree_path, '--input', cmsRun_out, '--output', output_file,
                  '--config', cfg_params.skimCfg, '--setup', cfg_params.skimSetupFailed,
                   '--skip-empty', '--update-output', '--verbose', '1']
      ps_call(cmd_line, verbose=1)

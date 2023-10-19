import os
import shutil
from sh_tools import sh_call

def processFile(input_file, output_file, tmp_files, cmssw_report, cmd_line_args, cfg_params):
  run_cmsDriver = True
  store_failed = cfg_params.storeFailed
  skim_cfg = cfg_params.skimCfg
  run_skim = len(skim_cfg) > 0
  debug = len(cmd_line_args) > 0 and cmd_line_args[0] == 'DEBUG'

  if debug:
    if len(cmd_line_args) > 1:
      run_cmsDriver = cmd_line_args[1] == 'True'
    if len(cmd_line_args) > 2:
      run_skim = cmd_line_args[2] == 'True'
    if len(cmd_line_args) > 3:
      store_failed = cmd_line_args[3] == 'True'

  output_name, output_ext = os.path.splitext(output_file)
  cmsRun_out = output_name + '_cmsDriver' + output_ext
  cmsDriver_py = 'nano_NANO.py'
  tmp_files.extend([ cmsRun_out, cmsDriver_py ])

  if run_cmsDriver:
    n_threads = 1
    cmsDrive_cmd = [
      'cmsDriver.py', 'nano', '--filein', input_file, '--fileout', f'file:{output_file}',
      '--eventcontent', 'NANOAODSIM', '--datatier', 'NANOAODSIM', '--step', 'NANO', '--nThreads', f'{n_threads}',
      f'--{cfg_params.sampleType}', '--conditions', cfg_params.cond,
      '--era', f"{cfg_params.era}", '-n', f'{cfg_params.maxEvents}', '--no_exec',
    ]

    customise = cfg_params.customisationFunction
    if len(customise) > 0:
      print(f'Using customisation function "{customise}"')
      customise_path, customise_fn = customise.split('.')
      customise_path = customise_path.split('/')
      if len(customise_path) == 3:
        customise_dir = os.path.join(os.environ['CMSSW_BASE'], 'src', customise_path[0], customise_path[1], 'python')
        customise_file = customise_path[2] + '.py'
        customise_file_path = os.path.join(customise_dir, customise_file)
        if not os.path.exists(customise_file_path):
          sandbox_file = os.path.join(os.path.dirname(__file__), customise_path[2] + '.py')
          if os.path.exists(sandbox_file):
            os.makedirs(customise_dir, exist_ok=True)
            shutil.copy(sandbox_file, customise_file_path)
      cmsDrive_cmd.extend(['--customise', customise])

    customise_commands = cfg_params.customisationCommands
    if len(customise_commands) > 0:
      cmsDrive_cmd.extend(['--customise_commands', customise_commands])

    cmssw_cmd = [ 'cmsRun',  '-j', cmssw_report, cmsDriver_py ]

    sh_call(cmsDrive_cmd, verbose=1)
    sh_call(cmssw_cmd, verbose=1)

  if run_skim:
    shutil.move(output_file, cmsRun_out)
    skim_tree_path = os.path.join(os.path.dirname(__file__), 'skim_tree.py')
    cmd_line = ['python3', '-u', skim_tree_path, '--input', cmsRun_out, '--output', output_file,
                '--config', cfg_params.skimCfg, '--setup', cfg_params.skimSetup, '--skip-empty', '--verbose', '1']
    sh_call(cmd_line, verbose=1)

    if store_failed:
      cmd_line = ['python3', '-u', skim_tree_path, '--input', cmsRun_out, '--output', output_file,
                  '--config', cfg_params.skimCfg, '--setup', cfg_params.skimSetupFailed,
                   '--skip-empty', '--update-output', '--verbose', '1']
      sh_call(cmd_line, verbose=1)


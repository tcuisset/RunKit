import os
import shutil
from RunKit.run_tools import ps_call

def processFile(input_file, outputs, tmp_files, cmssw_report, cmd_line_args, cfg_params):
  run_cmsDriver = True
  debug = len(cmd_line_args) > 0 and cmd_line_args[0] == 'DEBUG'
  if debug:
    if len(cmd_line_args) > 1:
      run_cmsDriver = cmd_line_args[1] == 'True'

  cmsRun_out = 'cmsRun_out.root'
  cmsDriver_py = 'nano_NANO.py'
  tmp_files.extend([ cmsDriver_py ])
  if os.path.exists(cmsRun_out):
    os.remove(cmsRun_out)

  if run_cmsDriver:
    n_threads = 1
    cmsDrive_cmd = [
      'cmsDriver.py', 'nano', '--filein', input_file, '--fileout', f'file:{cmsRun_out}',
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

    ps_call(cmsDrive_cmd, verbose=1)
    ps_call(cmssw_cmd, verbose=1)

  skim_tree_path = os.path.join(os.path.dirname(__file__), 'skim_tree.py')
  for output in outputs:
    if len(output['skim_cfg']) > 0:
      cmd_line = ['python3', '-u', skim_tree_path, '--input', cmsRun_out, '--output', output['file_name'],
                '--config', output['skim_cfg'], '--setup', output['skim_setup'], '--skip-empty', '--verbose', '1']
      ps_call(cmd_line, verbose=1)

      if len(output['skim_setup_failed']) > 0:
        cmd_line = ['python3', '-u', skim_tree_path, '--input', cmsRun_out, '--output', output['file_name'],
                    '--config', output['skim_cfg'], '--setup', output['skim_setup_failed'],
                    '--skip-empty', '--update-output', '--verbose', '1']
        ps_call(cmd_line, verbose=1)
    else:
      shutil.copy(cmsRun_out, output['file_name'])

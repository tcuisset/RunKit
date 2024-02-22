import os
import yaml

class CheckResult:
  def __init__(self, all_ok, tasks_by_name, tasks_by_dataset):
    self.all_ok = all_ok
    self.tasks_by_name = tasks_by_name
    self.tasks_by_dataset = tasks_by_dataset

def check_consistency_era(task_cfg_files):
  tasks_by_name = {}
  tasks_by_dataset = {}
  all_ok = True
  for task_cfg_file in task_cfg_files:
    if not os.path.isfile(task_cfg_file):
      print(f'ERROR: "{task_cfg_file}" does not exist.')
      all_ok = False
      continue
    try:
      with open(task_cfg_file) as f:
        cfg = yaml.safe_load(f)
    except:
      print(f'ERROR: "{task_cfg_file}" unable to parse yaml.')
      all_ok = False
      continue
    if type(cfg) != dict:
      print(f'ERROR: "{task_cfg_file}" contains {type(cfg)}, while dict is expected.')
      all_ok = False
      continue
    is_data = cfg.get('config', {}).get('params', {}).get('sampleType', '') == 'data'
    for task_name, task_desc in cfg.items():
      if task_name == 'config': continue
      customTask = type(task_desc) == dict
      if customTask:
        if 'inputDataset' not in task_desc:
          print(f'ERROR: "{task_cfg_file}" task "{task_name}" does not have "inputDataset" field.')
          all_ok = False
          continue
        if 'ignoreFiles' in task_desc:
          print(f'WARNING: "{task_cfg_file}" task "{task_name}" has "ignoreFiles" field.')
        inputDataset = task_desc['inputDataset']
      else:
        inputDataset = task_desc
      task_entry = {
        'name': task_name,
        'inputDataset': inputDataset,
        'file': task_cfg_file,
        'isData': is_data,
      }
      if task_name not in tasks_by_name:
        tasks_by_name[task_name] = []
      tasks_by_name[task_name].append(task_entry)
      if inputDataset not in tasks_by_dataset:
        tasks_by_dataset[inputDataset] = []
      tasks_by_dataset[inputDataset].append(task_entry)
  for task_name, task_list in tasks_by_name.items():
    if len(task_list) > 1:
      print(f'ERROR: task "{task_name}" is defined in multiple files:')
      for task_entry in task_list:
        print(f'  file={task_entry["file"]} dataset={task_entry["inputDataset"]}')
      all_ok = False
  for inputDataset, task_list in tasks_by_dataset.items():
    if len(task_list) > 1:
      print(f'ERROR: input dataset "{inputDataset}" is defined in multiple tasks:')
      for task_entry in task_list:
        print(f'  file={task_entry["file"]} task={task_entry["name"]}')
      all_ok = False

  return CheckResult(all_ok, tasks_by_name, tasks_by_dataset)

def check_consistency(era_files_dict, exceptions):
  era_results = {}
  tasks_by_name = {}
  all_ok = True
  for era, files in era_files_dict.items():
    era_results[era] = check_consistency_era(files)
    all_ok = all_ok and era_results[era].all_ok
    for task_name in era_results[era].tasks_by_name.keys():
      if task_name not in tasks_by_name:
        tasks_by_name[task_name] = []
      tasks_by_name[task_name].append(era)

  n_eras = len(era_files_dict)
  all_eras = set(era_files_dict.keys())
  for task_name, eras in tasks_by_name.items():
    is_data = era_results[eras[0]].tasks_by_name[task_name][0]['isData']
    if len(eras) != n_eras and not is_data:
      known_exceptions = exceptions.get(task_name, [])
      missing_eras = all_eras - set(eras) - set(known_exceptions)
      if len(missing_eras) > 0:
        missing_eras_str = ', '.join(missing_eras)
        print(f'{task_name} is not available in: {missing_eras_str}')
        for era in eras:
          for task in era_results[era].tasks_by_name[task_name]:
            print(f'  era={era} file={task["file"]} dataset={task["inputDataset"]}')
        all_ok = False
  return all_ok

if __name__ == "__main__":
  import argparse
  parser = argparse.ArgumentParser(description='Check consistency of tasks configurations for crabOverseer.')
  parser.add_argument('--cross-eras', action='store_true', help='Check consistency of tasks across different eras.')
  parser.add_argument('--exceptions', type=str, required=False, default=None,
                      help='File with exceptions for the checks.')
  parser.add_argument('task_file', type=str, nargs='+', help="file(s) with task descriptions")
  args = parser.parse_args()

  era_files_dict = {}
  if args.cross_eras:
    for era_dir in args.task_file:
      era_name = os.path.basename(era_dir)
      era_files_dict[era_name] = []
      for root, dirs, files in os.walk(era_dir):
        task_files = [os.path.join(root, f) for f in files if f.endswith('.yaml')]
        era_files_dict[era_name].extend(task_files)
  else:
    era_files_dict[''] = args.task_file

  exceptions = {}
  if args.exceptions:
    with open(args.exceptions) as f:
      exceptions = yaml.safe_load(f)


  all_ok = check_consistency(era_files_dict, exceptions)
  if all_ok:
    print("All checks are successfully passed.")
  else:
    print("Some checks are failed.")

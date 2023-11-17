import os
import yaml

def check_consistency(task_cfg_files):
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

  return all_ok

if __name__ == "__main__":
  import argparse
  parser = argparse.ArgumentParser(description='Check consistency of tasks configurations for crabOverseer.')
  parser.add_argument('task_file', type=str, nargs='+', help="file(s) with task descriptions")
  args = parser.parse_args()

  all_ok = check_consistency(args.task_file)
  if all_ok:
    print("All checks are successfully passed.")
  else:
    print("Some checks are failed.")

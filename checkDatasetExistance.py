import json
import sys
import yaml
from run_tools import ps_call

if __name__ == '__main__':
  datasets = []
  all_ok = True
  for entry in sys.argv[1:]:
    if entry.endswith('.yaml'):
      with open(entry, 'r') as f:
        cfg = yaml.safe_load(f)
      for task_name, task_desc in cfg.items():
        if task_name == 'config': continue
        customTask = type(task_desc) == dict
        if customTask:
          if 'inputDataset' not in task_desc:
            print(f'ERROR: "{entry}" task "{task_name}" does not have "inputDataset" field.')
            inputDataset = None
            all_ok = False
          else:
            inputDataset = task_desc['inputDataset']
        else:
          inputDataset = task_desc
    else:
      inputDataset = entry
    if inputDataset is not None:
      datasets.append(inputDataset)

  for dataset in datasets:
    try:
      if dataset.endswith("USER"): # Embedded samples
        _, output, _ = ps_call(['dasgoclient', '--json', '--query', f'dataset dataset={dataset} instance=prod/phys03'],
                               catch_stdout=True)
      else:
        _, output, _ = ps_call(['dasgoclient', '--json', '--query', f'dataset dataset={dataset}'],
                               catch_stdout=True)
      entries = json.loads(output)
      #print(json.dumps(info, indent=2))
      ds_infos = []
      for entry in entries:
        if "dbs3:dataset_info" in entry['das']['services']:
          ds_infos.append(entry['dataset'])
      if len(ds_infos) == 0:
        print(f'missing: {dataset}')
      # elif len(ds_infos) > 1:
      #   print(f'duplicates: {dataset}')
      else:
        status = None
        inconsistent_status = False
        for ds_info in ds_infos:
          if len(ds_info) != 1:
            print(f'multiple info: {dataset}')
            all_ok = False
          if status is not None and status != ds_info[0]['status']:
            print(f'inconsistent status: {dataset}')
            inconsistent_status = True
            all_ok = False
            break
          status = ds_info[0]['status']
        if not inconsistent_status and status != "VALID":
          print(f'not valid: {dataset}')
          all_ok = False
    except:
      print(f'query_failed: {dataset}')
      all_ok = False
  if all_ok:
    print("All datasets exist.")


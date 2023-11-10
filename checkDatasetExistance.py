import json
import sys
from run_tools import ps_call

if __name__ == '__main__':
  for dataset in sys.argv[1:]:
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
          if status is not None and status != ds_info[0]['status']:
            print(f'inconsistent status: {dataset}')
            inconsistent_status = True
            break
          status = ds_info[0]['status']
        if not inconsistent_status and status != "VALID":
          print(f'not valid: {dataset}')
    except:
      print(f'query_failed: {dataset}')

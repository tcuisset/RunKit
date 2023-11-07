import datetime
import json
import os
import select
import shutil
import sys
import yaml

if __name__ == "__main__":
  file_dir = os.path.dirname(os.path.abspath(__file__))
  sys.path.append(os.path.dirname(file_dir))
  __package__ = 'RunKit'

from .crabTaskStatus import JobStatus, Status
from .crabTask import Task
from .run_tools import PsCallError, ps_call, print_ts
from .grid_tools import get_voms_proxy_info

class TaskStat:
  summary_only_thr = 10

  def __init__(self):
    self.all_tasks = []
    self.tasks_by_status = {}
    self.n_jobs = 0
    self.total_job_stat = {}
    self.max_job_stat = {}
    self.unknown = []
    self.waiting_for_recovery = []
    self.failed = []
    self.tape_recall = []
    self.max_inactivity = None

  def add(self, task):
    self.all_tasks.append(task)
    if task.taskStatus.status not in self.tasks_by_status:
      self.tasks_by_status[task.taskStatus.status] = []
    self.tasks_by_status[task.taskStatus.status].append(task)
    if task.taskStatus.status == Status.InProgress:
      for job_status, count in task.taskStatus.job_stat.items():
        if job_status not in self.total_job_stat:
          self.total_job_stat[job_status] = 0
        self.total_job_stat[job_status] += count
        self.n_jobs += count
        if job_status not in self.max_job_stat or self.max_job_stat[job_status][0] < count:
          self.max_job_stat[job_status] = (count, task)
      delta_t = int(task.getTimeSinceLastJobStatusUpdate())
      if delta_t > 0 and (self.max_inactivity is None or delta_t > self.max_inactivity[1]):
        self.max_inactivity = (task, delta_t)
    if task.taskStatus.status == Status.Unknown:
      self.unknown.append(task)
    if task.taskStatus.status == Status.WaitingForRecovery:
      self.waiting_for_recovery.append(task)
    if task.taskStatus.status == Status.Failed:
      self.failed.append(task)
    if task.taskStatus.status == Status.TapeRecall:
      self.tape_recall.append(task)


  def report(self):
    status_list = sorted(self.tasks_by_status.keys(), key=lambda x: x.value)
    n_tasks = len(self.all_tasks)
    status_list = [ f"{n_tasks} Total" ] + [ f"{len(self.tasks_by_status[x])} {x.name}" for x in status_list ]
    print('Tasks: ' + ', '.join(status_list))
    job_stat = [ f"{self.n_jobs} total" ] + \
               [ f'{cnt} {x.name}' for x, cnt in sorted(self.total_job_stat.items(), key=lambda a: a[0].value) ]
    if self.n_jobs > 0:
      print('Jobs in active tasks: ' + ', '.join(job_stat))

    if Status.InProgress in self.tasks_by_status:
      if len(self.tasks_by_status[Status.InProgress]) > TaskStat.summary_only_thr:
        if(len(self.max_job_stat.items())):
          print('Task with ...')
          for job_status, (cnt, task) in sorted(self.max_job_stat.items(), key=lambda a: a[0].value):
            print(f'\tmax {job_status.name} jobs = {cnt}: {task.name} {task.taskStatus.dashboard_url}')
          if self.max_inactivity is not None:
            task, delta_t = self.max_inactivity
            print(f'\tmax since_last_job_status_change = {delta_t}h: {task.name} {task.taskStatus.dashboard_url}')
      else:
        for task in self.tasks_by_status[Status.InProgress]:
          text = f'{task.name}: status={task.taskStatus.status.name}. '
          delta_t = int(task.getTimeSinceLastJobStatusUpdate())
          if delta_t > 0:
            text += f' since_last_job_status_change={delta_t}h. '

          job_info = []
          for job_status, count in sorted(task.taskStatus.job_stat.items(), key=lambda x: x[0].value):
            job_info.append(f'{count} {job_status.name}')
          if len(job_info) > 0:
            text += ', '.join(job_info) + '. '
          if task.taskStatus.dashboard_url is not None:
            text += task.taskStatus.dashboard_url
          print(text)
    if len(self.unknown) > 0:
      print('Tasks with unknown status:')
      for task in self.unknown:
        print(f'{task.name}: {task.taskStatus.parse_error}. {task.lastCrabStatusLog()}')
    if len(self.waiting_for_recovery) > 0:
      names = [ task.name for task in self.waiting_for_recovery ]
      print(f"Tasks waiting for recovery: {', '.join(names)}")
    if len(self.tape_recall) > 0:
      names = [ task.name for task in self.tape_recall ]
      print(f"Tasks waiting for a tape recall to complete: {', '.join(names)}")
    if len(self.failed) > 0:
      names = [ task.name for task in self.failed ]
      print(f"Failed tasks that require manual intervention: {', '.join(names)}")


def sanity_checks(task):
  abnormal_inactivity_thr = 24

  if task.taskStatus.status == Status.InProgress:
    delta_t = task.getTimeSinceLastJobStatusUpdate()
    if delta_t > abnormal_inactivity_thr:
      text = f'{task.name}: status of all jobs is not changed for at least {delta_t:.1f} hours.' \
              + ' It is very likely that this task is stacked. The task will be killed following by recovery attempts.'
      print(text)
      task.kill()
      return False

    job_states = sorted(task.taskStatus.job_stat.keys(), key=lambda x: x.value)
    ref_states = [ JobStatus.running, JobStatus.finished, JobStatus.failed ]
    if len(job_states) <= len(ref_states) and job_states == ref_states[:len(job_states)]:
      now = datetime.datetime.now()
      start_times = task.taskStatus.get_detailed_job_stat('StartTimes', JobStatus.running)

      job_runs = []
      for job_id, start_time in start_times.items():
        t = datetime.datetime.fromtimestamp(start_time[-1])
        delta_t = (now - t).total_seconds() / (60 * 60)
        job_runs.append([job_id, delta_t])
      job_runs = sorted(job_runs, key=lambda x: x[1])
      max_run = job_runs[0][1]
      if max_run > abnormal_inactivity_thr:
        text = f'{task.name}: all running jobs are running for at least {max_run:.1f} hours.' \
              + ' It is very likely that these jobs are stacked. Task will be killed following by recovery attempts.'
        print(text)
        task.kill()
        return False

  return True

def update(tasks, no_status_update=False):
  print_ts("Updating...")
  stat = TaskStat()
  to_post_process = []
  to_run_locally = []
  for task_name, task in tasks.items():
    if task.taskStatus.status == Status.Defined:
      if task.submit():
        to_run_locally.append(task)
    elif task.taskStatus.status.value < Status.CrabFinished.value:
      if task.taskStatus.status.value < Status.WaitingForRecovery.value and not no_status_update:
        if task.updateStatus():
          to_run_locally.append(task)
      if task.taskStatus.status == Status.WaitingForRecovery:
        if task.recover():
          to_run_locally.append(task)
    sanity_checks(task)
    if task.taskStatus.status == Status.CrabFinished:
      if task.checkCompleteness():
        task.prepareForPostProcess()
        done_flag = task.getPostProcessingDoneFlagFile()
        if os.path.exists(done_flag):
          os.remove(done_flag)
        to_post_process.append(task)
      else:
        if task.recover():
          to_run_locally.append(task)
    stat.add(task)
  stat.report()
  return to_post_process, to_run_locally

def apply_action(action, tasks, task_selection, task_list_path):
  selected_tasks = []
  for task_name, task in tasks.items():
    if task_selection is None or eval(task_selection):
      selected_tasks.append(task)

  if action == 'print':
    for task in selected_tasks:
      print(task.name)
  elif action.startswith('run_cmd'):
    cmd = action[len('run_cmd') + 1:]
    for task in selected_tasks:
      exec(cmd)
      task.saveCfg()
      task.saveStatus()
  elif action == 'list_files_to_process':
    for task in selected_tasks:
      print(f'{task.name}: files to process')
      for file in task.getFilesToProcess():
        print(f'  {file}')
  elif action == 'kill':
    for task in selected_tasks:
      print(f'{task.name}: sending kill request...')
      try:
        task.kill()
      except PsCallError as e:
        print(f'{task.name}: error sending kill request. {e}')
  elif action == 'remove':
    for task in selected_tasks:
      print(f'{task.name}: removing...')
      shutil.rmtree(task.workArea)
      del tasks[task.name]
    with open(task_list_path, 'w') as f:
      json.dump([task_name for task_name in tasks], f, indent=2)
  elif action == 'remove_final_output':
    for task in selected_tasks:
      for output in task.getOutputs():
        task_output = output['finalOutput']
        print(f'{task.name}: removing final output "{task_output}"...')
        if os.path.exists(task_output):
          shutil.rmtree(task_output)
  else:
    raise RuntimeError(f'Unknown action = "{action}"')

def check_prerequisites(main_cfg):
  # if 'CRABCLIENT_TYPE' not in os.environ or len(os.environ['CRABCLIENT_TYPE'].strip()) == 0:
  #   raise RuntimeError("Crab environment is not set. Please source /cvmfs/cms.cern.ch/common/crab-setup.sh")
  voms_info = get_voms_proxy_info()
  if 'timeleft' not in voms_info or voms_info['timeleft'] < 1:
    raise RuntimeError('Voms proxy is not initalised or is going to expire soon.' + \
                       ' Please run "voms-proxy-init -voms cms -rfc -valid 192:00".')
  if 'localProcessing' not in main_cfg or 'LAW_HOME' not in os.environ:
    raise RuntimeError("Law environment is not setup. It is needed to run the local processing step.")

def overseer_main(work_area, cfg_file, new_task_list_files, verbose=1, no_status_update=False,
                  update_cfg=False, no_loop=False, task_selection=None, action=None):
  if not os.path.exists(work_area):
    os.makedirs(work_area)
  abs_work_area = os.path.abspath(work_area)
  cfg_path = os.path.join(work_area, 'cfg.yaml')
  if cfg_file is not None:
    shutil.copyfile(cfg_file, cfg_path)
  if not os.path.isfile(cfg_path):
    raise RuntimeError("The overseer configuration is not found")
  with open(cfg_path, 'r') as f:
    main_cfg = yaml.safe_load(f)

  check_prerequisites(main_cfg)
  task_list_path = os.path.join(work_area, 'tasks.json')
  tasks = {}
  if os.path.isfile(task_list_path):
    with open(task_list_path, 'r') as f:
      task_names = json.load(f)
      for task_name in task_names:
        tasks[task_name] = Task.Load(mainWorkArea=work_area, taskName=task_name)
  if len(new_task_list_files) > 0:
    for task_list_file in new_task_list_files:
      with open(task_list_file, 'r') as f:
        new_tasks = yaml.safe_load(f)
      for task_name in new_tasks:
        if task_name == 'config': continue
        if task_name in tasks:
          if update_cfg:
            tasks[task_name].updateConfig(main_cfg, new_tasks)
        else:
          tasks[task_name] = Task.Create(work_area, main_cfg, new_tasks, task_name)
    with open(task_list_path, 'w') as f:
      json.dump(list(tasks.keys()), f, indent=2)

  if action is not None:
    apply_action(action, tasks, task_selection, task_list_path)
    return

  for name, task in tasks.items():
    task.checkConfigurationValidity()

  update_interval = main_cfg.get('updateInterval', 60)

  while True:
    last_update = datetime.datetime.now()
    to_post_process, to_run_locally = update(tasks, no_status_update=no_status_update)
    if len(to_run_locally) > 0 or len(to_post_process) > 0:
      if len(to_run_locally) > 0:
        print_ts("To run on local grid: " + ', '.join([ task.name for task in to_run_locally ]))
      if len(to_post_process) > 0:
        print_ts("Post-processing: " + ', '.join([ task.name for task in to_post_process ]))
      local_proc_params = main_cfg['localProcessing']
      law_sub_dir = os.path.join(abs_work_area, 'law', 'jobs')
      law_task_dir = os.path.join(law_sub_dir, local_proc_params['lawTask'])

      if os.path.exists(law_task_dir):
        shutil.rmtree(law_task_dir)

      n_cpus = local_proc_params.get('nCPU', 1)
      max_runime = local_proc_params.get('maxRuntime', 24.0)
      cmd = [ 'law', 'run', local_proc_params['lawTask'],
              '--workflow', local_proc_params['workflow'],
              '--bootstrap-path', local_proc_params['bootstrap'],
              '--work-area', abs_work_area,
              '--log-path', os.path.join(abs_work_area, 'law', 'logs'),
              '--sub-dir', law_sub_dir,
              '--n-cpus', str(n_cpus),
              '--max-runtime', str(max_runime),
              '--transfer-logs',
      ]
      if 'requirements' in local_proc_params:
        cmd.extend(['--requirements', local_proc_params['requirements']])
      ps_call(cmd)
      for task in to_post_process + to_run_locally:
        task.updateStatusFromFile()
      print_ts("Local grid processing iteration finished.")
    has_unfinished = False
    for task_name, task in tasks.items():
      if task.taskStatus.status not in [ Status.PostProcessingFinished, Status.Failed ]:
        has_unfinished = True
        break

    if no_loop or not has_unfinished: break
    delta_t = (datetime.datetime.now() - last_update).total_seconds() / 60
    to_sleep = int(update_interval - delta_t)
    if to_sleep >= 1:
      print_ts(f"Waiting for {to_sleep} minutes until the next update. Press return to exit.", prefix='\n')
      rlist, wlist, xlist = select.select([sys.stdin], [], [], to_sleep * 60)
      if rlist:
        print_ts("Exiting...")
        break
    if main_cfg.get('renewKerberosTicket', False):
      ps_call(['kinit', '-R'])
  if not has_unfinished:
    print("All tasks are done.")

if __name__ == "__main__":
  import argparse
  parser = argparse.ArgumentParser(description='CRAB overseer.')
  parser.add_argument('--work-area', required=False, type=str, default='.crabOverseer',
                      help="Working area to store overseer and crab tasks states")
  parser.add_argument('--cfg', required=False, type=str, default=None, help="configuration file")
  parser.add_argument('--no-status-update', action="store_true", help="Do not update tasks statuses.")
  parser.add_argument('--update-cfg', action="store_true", help="Update task configs.")
  parser.add_argument('--no-loop', action="store_true", help="Run task update once and exit.")
  parser.add_argument('--select', required=False, type=str, default=None,
                      help="select tasks to which apply an action. Default: select all.")
  parser.add_argument('--action', required=False, type=str, default=None,
                      help="apply action on selected tasks and exit")
  parser.add_argument('--verbose', required=False, type=int, default=1, help="verbosity level")
  parser.add_argument('task_file', type=str, nargs='*', help="file(s) with task descriptions")
  args = parser.parse_args()

  overseer_main(args.work_area, args.cfg, args.task_file, verbose=args.verbose, no_status_update=args.no_status_update,
                update_cfg=args.update_cfg, no_loop=args.no_loop, task_selection=args.select, action=args.action)

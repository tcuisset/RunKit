import json
import law
import luigi
import os
import shutil
import tempfile
import threading

from .law_customizations import HTCondorWorkflow
from .crabTask import Task as CrabTask
from .crabTaskStatus import Status
from .run_tools import ps_call

cond = threading.Condition()

def update_kinit(verbose=0):
  if shutil.which('kinit'):
    ps_call(['kinit', '-R'], expected_return_codes=None, verbose=verbose)

def update_kinit_thread():
  timeout = 60.0 * 60 # 1 hour
  cond.acquire()
  while not cond.wait(timeout):
    update_kinit(verbose=1)
  cond.release()

class ProdTask(HTCondorWorkflow, law.LocalWorkflow):
  work_area = luigi.Parameter()

  def local_path(self, *path):
    return os.path.join(self.htcondor_output_directory().path, *path)

  def workflow_requires(self):
    return {}

  def requires(self):
    return {}

  def law_job_home(self):
    if 'LAW_JOB_HOME' in os.environ:
      return os.environ['LAW_JOB_HOME'], False
    os.makedirs(self.local_path(), exist_ok=True)
    return tempfile.mkdtemp(dir=self.local_path()), True

  def create_branch_map(self):
    task_list_path = os.path.join(self.work_area, 'tasks.json')
    with open(task_list_path, 'r') as f:
      task_names = json.load(f)
    branches = {}
    job_id = 0
    for task_name in task_names:
      task = CrabTask.Load(mainWorkArea=self.work_area, taskName=task_name)
      if task.taskStatus.status in [ Status.CrabFinished, Status.PostProcessingFinished ]:
        branches[job_id] = (task.workArea, -1, task.getPostProcessingDoneFlagFile())
        job_id += 1
      else:
        for grid_job_id in task.getGridJobs():
          branches[job_id] = (task.workArea, grid_job_id, task.getGridJobDoneFlagFile(grid_job_id))
          job_id += 1
    return branches

  def output(self):
    work_area, grid_job_id, done_flag = self.branch_data
    return law.LocalFileTarget(done_flag)

  def run(self):
    thread = threading.Thread(target=update_kinit_thread)
    thread.start()
    job_home, remove_job_home = self.law_job_home()
    try:
      work_area, grid_job_id, done_flag = self.branch_data
      task = CrabTask.Load(workArea=work_area)
      if grid_job_id == -1:
        if task.taskStatus.status in [ Status.CrabFinished, Status.PostProcessingFinished ]:
          if task.taskStatus.status == Status.CrabFinished:
            print(f'Post-processing {task.name}')
            task.postProcessOutputs(job_home)
          self.output().touch()
        else:
          raise RuntimeError(f"task {task.name} is not ready for post-processing")
      else:
        print(f'Running {task.name} job_id = {grid_job_id}')
        result = task.runJobLocally(grid_job_id, job_home)
        state_str = 'finished' if result else 'failed'
        with self.output().open('w') as output:
          output.write(state_str)
    finally:
      if remove_job_home:
        shutil.rmtree(job_home)
      cond.acquire()
      cond.notify_all()
      cond.release()
      thread.join()


  def poll_callback(self, poll_data):
    update_kinit(verbose=0)

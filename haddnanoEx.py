import json
import os
import re
import shutil
import sys
import time
import tempfile

if __name__ == "__main__":
  file_dir = os.path.dirname(os.path.abspath(__file__))
  base_dir = os.path.dirname(file_dir)
  if base_dir not in sys.path:
    sys.path.append(base_dir)
  __package__ = os.path.split(file_dir)[-1]

from .run_tools import PsCallError, ps_call

class LocalIO:
  def ls(self, path, recursive=False, not_exists_ok=False):
    if not os.path.exists(path):
      if not_exists_ok:
        return []
      raise RuntimeError(f'Path "{path}" does not exists.')
    all_files = []
    if recursive:
      for root, dirs, files in os.walk(path):
        for file in files:
            file_path = os.path.join(root, file)
            file_size = os.path.getsize(file_path)
            all_files.append((file_path, file_size))
    else:
      for file in os.lsdir(path):
        file_path = os.path.join(path, file)
        file_size = os.path.getsize(file_path)
        all_files.append((file_path, file_size))
    return all_files

  def rm(self, path):
    shutil.rmtree(path)

  def copy(self, src, dst):
    dst_dir = os.path.dirname(dst)
    os.makedirs(dst_dir, exist_ok=True)
    shutil.copy(src, dst)

  def move(self, src, dst):
    dst_dir = os.path.dirname(dst)
    os.makedirs(dst_dir, exist_ok=True)
    shutil.move(src, dst)

  def copy_local(self, files, out_dir):
    return None

class RemoteIO:
  def __init__(self):
    from .grid_tools import get_voms_proxy_info, gfal_ls, gfal_ls_recursive, gfal_copy_safe, gfal_rm, \
                            gfal_rename, gfal_exists
    self.gfal_ls = gfal_ls
    self.gfal_ls_recursive = gfal_ls_recursive
    self.gfal_copy_safe = gfal_copy_safe
    self.gfal_rm = gfal_rm
    self.gfal_rename = gfal_rename
    self.gfal_exists = gfal_exists
    self.voms_token = get_voms_proxy_info()['path']

  def ls(self, path, recursive=False, not_exists_ok=False):
    if not self.gfal_exists(path, voms_token=self.voms_token):
      if not_exists_ok:
        return []
      raise RuntimeError(f'Path "{path}" does not exists.')
    files = []
    if recursive:
      ls_result = self.gfal_ls_recursive(path, voms_token=self.voms_token)
    else:
      ls_result = self.gfal_ls(path, voms_token=self.voms_token)
    for file in ls_result:
      files.append((file.full_name, file.size))
    return files

  def rm(self, path):
    self.gfal_rm(path, recursive=True, voms_token=self.voms_token)

  def copy(self, src, dst):
    self.gfal_copy_safe(src, dst, voms_token=self.voms_token)

  def move(self, src, dst):
    self.gfal_rename(src, dst, voms_token=self.voms_token)

  def copy_local(self, files, out_dir):
    local_files = []
    for file in files:
      file_base = os.path.basename(file)
      file_name, file_ext = os.path.splitext(file_base)
      idx = 0
      while True:
        local_name = file_base if idx == 0 else f'{file_name}_{idx}{file_ext}'
        local_file = os.path.join(out_dir, local_name)
        if local_file not in local_files:
          break
        idx += 1
      self.copy(file, local_file)
      local_files.append(local_file)
    return local_files

def toMiB(size):
  return float(size) / (1024 * 1024)

class InputFile:
  def __init__(self, name, size):
    self.name = name
    self.size = size

class OutputFile:
  def __init__(self):
    self.name = None
    self.expected_size = 0.
    self.input_files = []
    self.input_files_local = None

  def try_add(self, file, max_size):
    if self.expected_size + file.size > max_size:
      return False
    self.expected_size += file.size
    self.input_files.append(file)
    return True

  def try_merge(self, input_names):
    try:
      if os.path.exists(self.out_path):
        os.remove(self.out_path)
      haddnano_path = os.path.join(os.path.dirname(__file__), 'haddnano.py')
      cmd = ['python3', '-u', haddnano_path, self.out_path ] + input_names
      ps_call(cmd, verbose=1)
      self.size = toMiB(os.path.getsize(self.out_path))
      return True, None
    except (PsCallError, OSError, FileNotFoundError) as e:
      return False, e

  def merge(self, out_dir, max_n_retries, retry_interval):
    n_retries = 0
    self.out_path = os.path.join(out_dir, self.name)
    input_names = [ f.name for f in self.input_files ] if self.input_files_local is None else self.input_files_local
    while True:
      merged, error = self.try_merge(input_names)
      if merged: return
      n_retries += 1
      if n_retries >= max_n_retries:
        raise error
      print(f"Merge failed. {error}\nWaiting {retry_interval} seconds before the next attempt...")
      time.sleep(retry_interval)

def mergeFiles(output_files, output_dir, output_name_base, work_dir, io_provider, max_n_retries, retry_interval):
  merge_dir = os.path.join(work_dir, 'merged')
  os.makedirs(merge_dir, exist_ok=True)
  input_dir = os.path.join(work_dir, 'input')
  os.makedirs(merge_dir, exist_ok=True)
  output_tmp = os.path.join(output_dir, output_name_base + '.tmp')

  for file in output_files:
    print(f'Merging {len(file.input_files)} input files into {file.name}...')
    file.input_files_local = io_provider.copy_local([ f.name for f in file.input_files ], input_dir)
    file.merge(merge_dir, max_n_retries, retry_interval)
    file.remote_tmp_path = os.path.join(output_tmp, file.name)
    io_provider.copy(file.out_path, file.remote_tmp_path)
    os.remove(file.out_path)
    if file.input_files_local is not None:
      for file_name in file.input_files_local:
        os.remove(file_name)
    print(f'Done. Expected size = {file.expected_size:.1f} MiB, actual size = {file.size:.1f} MiB.')
  print("Moving merged files into the final location and removing temporary files.")
  for file in output_files:
    io_provider.move(file.remote_tmp_path, os.path.join(output_dir, file.name))
  io_provider.rm(output_tmp)
  print('All inputs have been merged.')

def getWorkDir(work_dir):
  if work_dir is not None:
    return work_dir
  tmp_base= os.environ.get('TMPDIR', '.')
  os.makedirs(tmp_base, exist_ok=True)
  return tempfile.mkdtemp(dir=tmp_base)

def getInputFiles(input_dirs, file_list, io_provider):
  input_files = []
  for input_dir in input_dirs:
    for file_name, file_size in io_provider.ls(input_dir, recursive=True, not_exists_ok=False):
      if file_name.endswith('.root'):
        input_files.append(InputFile(file_name, toMiB(file_size)))
  if file_list is not None:
    with open(file_list, 'r') as f:
      lines = [ l for l in f.read().splitlines() if len(l) > 0 ]
    for line in lines:
      files = io_provider.ls(line, recursive=False, not_exists_ok=False)
      if len(files) != 1:
        raise RuntimeError(f'File "{line}" not found.')
      file_name, file_size = files[0]
      input_files.append(InputFile(file_name, toMiB(file_size)))
  return input_files

def createOutputPlan(input_files, target_size, output_name_base):
  input_files = sorted(input_files, key=lambda f: -f.size)
  processed_files = set()
  output_files = []
  while len(processed_files) < len(input_files):
    output_file = OutputFile()
    for file in input_files:
      if file not in processed_files and output_file.try_add(file, target_size):
        processed_files.add(file)
    output_files.append(output_file)
  for idx, file in enumerate(output_files):
    file.name = output_name_base + f'_{idx}.root'
  return output_files

def cleanOutput(output_dir, output_name_base, io_provider):
  name_pattern = re.compile(f'^{output_name_base}(|_[0-9]+)\.(root|tmp)$')
  files = io_provider.ls(output_dir, recursive=False, not_exists_ok=True)
  for file, file_size in files:
    if name_pattern.match(file):
      io_provider.rm(file)

def haddnanoEx(input_dirs, file_list, output_dir, output_name, work_dir, target_size, use_remote_io, max_n_retries,
               retry_interval, merge_report_path):
  work_dir = getWorkDir(work_dir)
  io_provider = RemoteIO() if use_remote_io else LocalIO()
  input_files = getInputFiles(input_dirs, file_list, io_provider)
  if len(input_files) == 0:
    raise RuntimeError("No input files were found.")
  output_name_base, output_name_ext = os.path.splitext(output_name)
  if output_name_ext != '.root':
    raise RuntimeError(f'Unsupported output file format "{output_name_ext}"')

  output_files = createOutputPlan(input_files, target_size, output_name_base)
  cleanOutput(output_dir, output_name_base, io_provider)
  mergeFiles(output_files, output_dir, output_name_base, work_dir, io_provider, max_n_retries, retry_interval)

  if merge_report_path is not None:
    merge_report = {}
    for output in output_files:
      merge_report[output.name] = [ f.name for f in output.input_files ]
    with open(merge_report_path, 'w') as f:
      json.dump(merge_report, f, indent=2)

if __name__ == "__main__":
  import argparse
  parser = argparse.ArgumentParser(description='hadd nano files.')
  parser.add_argument('--output-dir', required=True, type=str, help="Output where merged files will be stored.")
  parser.add_argument('--output-name', required=False, type=str, default='nano.root',
                      help="Name of the output files. _1, _2, etc. suffices will be added.")
  parser.add_argument('--work-dir', required=False, default=None, type=str, help="Work directory for temporary files.")
  parser.add_argument('--target-size', required=False, type=float, default=2*1024.,
                      help="target output file size in MiB")
  parser.add_argument('--remote-io', action='store_true', help='use remote I/O')
  parser.add_argument('--n-retries', required=False, type=int, default=1,
                      help="maximal number of retries in case if hadd fails. " + \
                           "The retry counter is reset to 0 after each successful hadd.")
  parser.add_argument('--retry-interval', required=False, type=int, default=60,
                      help="interval in seconds between retry attempts.")
  parser.add_argument('--merge-report', required=False, default=None, type=str,
                      help="File where to store merge report.")
  parser.add_argument('--file-list', required=False, type=str, default=None,
                      help="txt file with the list of input files to merge")
  parser.add_argument('input_dir', type=str, nargs='*', help="input directories")
  args = parser.parse_args()

  haddnanoEx(args.input_dir, args.file_list, args.output_dir, args.output_name, args.work_dir, args.target_size,
             args.remote_io, args.n_retries, args.retry_interval, args.merge_report)

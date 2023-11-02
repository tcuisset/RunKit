import os
import re
import shutil
import time

from RunKit.run_tools import PsCallError, ps_call

class InputFile:
  def __init__(self, name):
    self.name = name
    self.size = float(os.path.getsize(name)) / (1024 * 1024)

class OutputFile:
  def __init__(self):
    self.name = None
    self.expected_size = 0.
    self.input_files = []

  def try_add(self, file, max_size):
    if self.expected_size + file.size > max_size:
      return False
    self.expected_size += file.size
    self.input_files.append(file)
    return True

  def try_merge(self):
    try:
      if os.path.exists(self.out_path):
        os.remove(self.out_path)
      haddnano_path = os.path.join(os.path.dirname(__file__), 'haddnano.py')
      cmd = ['python3', '-u', haddnano_path, self.out_path ] + [ f.name for f in self.input_files ]
      ps_call(cmd, verbose=1)
      self.size = float(os.path.getsize(self.out_path)) / (1024 * 1024)
      return True, None
    except (PsCallError, OSError, FileNotFoundError) as e:
      return False, e

  def merge(self, out_dir, max_n_retries, retry_interval):
    n_retries = 0
    self.out_path = os.path.join(out_dir, self.name)
    while True:
      merged, error = self.try_merge()
      if merged: return
      n_retries += 1
      if n_retries == max_n_retries:
        raise error
      print(f"Merge failed. {error}\nWaiting {retry_interval} seconds before the next attempt...")
      time.sleep(retry_interval)

def merge_files(output_dir, output_name, target_size, file_list, input_dirs, max_n_retries, retry_interval):
  input_files = []
  for input_dir in input_dirs:
    for root, dirs, files in os.walk(input_dir):
      for file in files:
        if file.endswith('.root'):
          file_path = os.path.join(root, file)
          input_files.append(InputFile(file_path))
  if file_list is not None:
    with open(file_list, 'r') as f:
      lines = [ l for l in f.read().splitlines() if len(l) > 0 ]
    for line in lines:
      if not os.path.exists(line):
        raise RuntimeError(f'File {line} does not exists.')
      input_files.append(InputFile(line))
  if len(input_files) == 0:
    raise RuntimeError("No input files were found.")
  input_files = sorted(input_files, key=lambda f: -f.size)
  processed_files = set()
  output_files = []
  while len(processed_files) < len(input_files):
    output_file = OutputFile()
    for file in input_files:
      if file not in processed_files and output_file.try_add(file, target_size):
        processed_files.add(file)
    output_files.append(output_file)

  if not os.path.exists(output_dir):
    os.makedirs(output_dir)

  output_name_base, output_name_ext = os.path.splitext(output_name)
  if output_name_ext != '.root':
    raise RuntimeError(f'Unsupported output file format "{output_name_ext}"')
  name_pattern = re.compile(f'^{output_name_base}(|_[0-9]+)\.(root|tmp)$')

  for file in os.listdir(output_dir):
    if name_pattern.match(file):
      file_path = os.path.join(output_dir, file)
      if os.path.isdir(file_path):
        shutil.rmtree(file_path)
      else:
        os.remove(file_path)

  output_tmp = os.path.join(output_dir, output_name_base + '.tmp')
  os.makedirs(output_tmp)

  if len(output_files) == 1:
    output_files[0].name = output_name
  else:
    for id, file in enumerate(output_files):
      file.name = output_name_base + f'_{id}.root'

  for file in output_files:
    print(f'Merging {len(file.input_files)} input files into {file.name}...')
    file.merge(output_tmp, max_n_retries, retry_interval)
    print(f'Done. Expected size = {file.expected_size:.1f} MiB, actual size = {file.size:.1f} MiB.')
  print("Moving merged files into the final location and removing temporary files.")
  for file in output_files:
    shutil.move(file.out_path, os.path.join(output_dir, file.name))
  shutil.rmtree(output_tmp)
  print('All inputs have been merged.')


if __name__ == "__main__":
  import argparse
  parser = argparse.ArgumentParser(description='hadd nano files.')
  parser.add_argument('--output-dir', required=True, type=str, help="Output where merged files will be stored.")
  parser.add_argument('--output-name', required=False, type=str, default='nano.root',
                      help="Name of the output files." + \
                           " If the number of output files is 1, the name will be as specified in the argument." + \
                           " If the number of output files is greater than one, _1, _2, etc. suffices will be added.")
  parser.add_argument('--file-list', required=False, type=str, default=None,
                      help="txt file with the list of input files to merge")
  parser.add_argument('--target-size', required=False, type=float, default=2*1024.,
                      help="target output file size in MiB")
  parser.add_argument('--n-retries', required=False, type=int, default=4,
                      help="maximal number of retries in case if hadd fails. " + \
                           "The retry counter is reset to 0 after each successful hadd.")
  parser.add_argument('--retry-interval', required=False, type=int, default=60,
                      help="interval in seconds between retry attempts.")
  parser.add_argument('input_dir', type=str, nargs='*', help="input directories")
  args = parser.parse_args()

  merge_files(args.output_dir, args.output_name, args.target_size, args.file_list, args.input_dir, args.n_retries,
              args.retry_interval)

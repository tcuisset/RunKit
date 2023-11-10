import datetime
import json
import os
import re
import sys

if __name__ == "__main__":
  file_dir = os.path.dirname(os.path.abspath(__file__))
  sys.path.append(os.path.dirname(file_dir))
  __package__ = 'RunKit'

from .run_tools import ps_call, repeat_until_success, adler32sum, PsCallError

class FileInfo:
  def __init__(self, name=None, path=None, size=None, date=None, is_dir=None):
    self.name = name
    self.path = path
    self.size = size
    self.date = date
    self.is_dir = is_dir

  @property
  def full_name(self):
    return os.path.join(self.path, self.name)

  def __str__(self):
    date_str = self.date.strftime('%Y-%m-%dT%H:%M') if self.date is not None else None
    return f'name="{self.name}", path="{self.path}", size={self.size}, date={date_str}, is_dir={self.is_dir}'

  def __repr__(self):
    return self.__str__()

class GfalError(RuntimeError):
  def __init__(self, msg):
    super(GfalError, self).__init__(msg)

def get_voms_proxy_info():
  _, output, _ = ps_call(['voms-proxy-info'], catch_stdout=True, split='\n')
  info = {}
  for line in output:
    if len(line) == 0: continue
    match = re.match(r'^(.+) : (.+)', line)
    key = match.group(1).strip()
    info[key] = match.group(2)
  if 'timeleft' in info:
    h,m,s = info['timeleft'].split(':')
    info['timeleft'] = float(h) + ( float(m) + float(s) / 60. ) / 60.
  return info

def check_download(local_file, expected_adler32sum=None, raise_error=False, remote_file=None,
                   remove_bad_file=False):
  if expected_adler32sum is not None:
    asum = adler32sum(local_file)
    if asum != expected_adler32sum:
      if remove_bad_file:
        os.remove(local_file)
      if raise_error:
        remote_name = remote_file if remote_file is not None else 'file'
        raise RuntimeError(f'Unable to copy {remote_name} from remote. Failed adler32sum check.' + \
                           f' {asum:x} != {expected_adler32sum:x}.')
      return False
  return True

def xrd_copy(input_remote_file, output_local_file, n_retries=4, n_retries_xrdcp=4, n_streams=1, retry_sleep_interval=10,
             expected_adler32sum=None, verbose=1,
             prefixes = [ 'root://cms-xrd-global.cern.ch/', 'root://xrootd-cms.infn.it/',
                          'root://cmsxrootd.fnal.gov/' ]):
  def download(prefix):
    xrdcp_args = ['xrdcp', '--retry', str(n_retries_xrdcp), '--streams', str(n_streams) ]
    if os.path.exists(output_local_file):
      xrdcp_args.append('--continue')
    if verbose == 0:
      xrdcp_args.append('--silent')
    xrdcp_args.extend([f'{prefix}{input_remote_file}', output_local_file])
    ps_call(xrdcp_args, verbose=1)

    check_download(output_local_file, expected_adler32sum=expected_adler32sum, remove_bad_file=True,
                   raise_error=True, remote_file=input_remote_file)

  if os.path.exists(output_local_file):
    os.remove(output_local_file)

  if input_remote_file.startswith("/store/"):
    optlist = [ (prefix, ) for prefix in prefixes ]
  else:
    optlist = [ ("", ) ]

  repeat_until_success(download, opt_list=optlist, raise_error=True,
                       error_message=f'Unable to copy {input_remote_file} from remote.', n_retries=n_retries,
                       retry_sleep_interval=retry_sleep_interval, verbose=verbose)

def gfal_copy_safe(input_file, output_file, voms_token=None, number_of_streams=2, timeout=7200,
                   expected_adler32sum=None, n_retries=4, retry_sleep_interval=10, verbose=1):
  if voms_token is None:
    voms_token = get_voms_proxy_info()['path']
  if expected_adler32sum is None:
    try:
      expected_adler32sum = gfal_sum(input_file, voms_token=voms_token, sum_type='adler32')
    except GfalError as e:
      if verbose > 0:
        print(f'WARNING: gfal_sum failed for "{input_file}".\n{e}')
  output_file_tmp = output_file + '.tmp'
  def download():
    if gfal_exists(output_file, voms_token=voms_token):
      gfal_rm(output_file, voms_token=voms_token, recursive=False)
    if gfal_exists(output_file_tmp, voms_token=voms_token):
      gfal_rm(output_file_tmp, voms_token=voms_token, recursive=False)
    gfal_copy(input_file, output_file_tmp, voms_token=voms_token, number_of_streams=number_of_streams, timeout=timeout,
              verbose=verbose)
    output_adler32sum = gfal_sum(output_file_tmp, voms_token=voms_token, sum_type='adler32')
    if expected_adler32sum is not None and output_adler32sum != expected_adler32sum:
      raise GfalError(f'Failed adler32sum check for "{output_file_tmp}".'
                      f' {output_adler32sum:x} != {expected_adler32sum:x}.')
    gfal_rename(output_file_tmp, output_file, voms_token=voms_token)
    if not gfal_exists(output_file, voms_token=voms_token):
      raise GfalError(f'Failed to rename "{output_file_tmp}" to "{output_file}".')

  repeat_until_success(download, raise_error=True, error_message=f'Unable to copy "{input_file}" to "{output_file}".',
                       n_retries=n_retries, retry_sleep_interval=retry_sleep_interval, verbose=verbose)

def gfal_copy(input_file, output_file, voms_token=None, number_of_streams=2, timeout=7200, verbose=1):
  if voms_token is None:
    voms_token = get_voms_proxy_info()['path']
  try:
    catch_output = verbose == 0
    ps_call([ 'gfal-copy', '--parent', '--nbstreams', str(number_of_streams), '--timeout', str(timeout),
                input_file, output_file ],
              shell=False, env={'X509_USER_PROXY': voms_token}, verbose=verbose,
              catch_stdout=catch_output, catch_stderr=catch_output)
  except PsCallError as e:
    raise GfalError(f'gfal_copy: unable to copy "{input_file}" to "{output_file}"\n{e}')

def gfal_ls(path, voms_token=None, catch_stderr=False, verbose=1):
  if voms_token is None:
    voms_token = get_voms_proxy_info()['path']
  try:
    _, output, _ = ps_call([ 'gfal-ls', '--long', '--all', '--time-style', 'long-iso', path ],
                           shell=False, env={'X509_USER_PROXY': voms_token}, catch_stdout=True,
                           catch_stderr=catch_stderr, split='\n', verbose=verbose)
  except PsCallError as e:
    raise GfalError(f'gfal_ls: unable to list "{path}"\n{e}')
  files = []
  for line in output:
    if len(line) == 0: continue
    items = re.match('^([rwx\-d]+) +[0-9]+ +[0-9]+ +[0-9]+ +([0-9]+) +([0-9\-]+ [0-9:]+) +(.*)$', line)
    if items is None:
      raise GfalError(f'gfal_ls: unable to parse "{line}"')
    file = FileInfo()
    file.name = items.group(4).strip()
    if file.name in ['.', '..']: continue
    if file.name == path:
      file.path, file.name = os.path.split(path)
    else:
      file.path = path
    file.size = int(items.group(2))
    file.date = datetime.datetime.strptime(items.group(3), '%Y-%m-%d %H:%M')
    file.is_dir = items.group(1).startswith('d')
    files.append(file)
  return files

def gfal_ls_recursive(path, voms_token=None, verbose=1):
  if voms_token is None:
    voms_token = get_voms_proxy_info()['path']

  all_files = []
  path_files = gfal_ls(path, voms_token=voms_token, verbose=verbose)
  for file in path_files:
    all_files.append(file)
    if file.is_dir:
      all_files.extend(gfal_ls_recursive(file.full_name, voms_token=voms_token, verbose=verbose))
  return sorted(set(all_files), key=lambda f: f.full_name)

def gfal_ls_safe(path, voms_token=None, catch_stderr=False, verbose=1):
  try:
    return gfal_ls(path, voms_token=voms_token, catch_stderr=catch_stderr, verbose=verbose)
  except GfalError:
    return None

def gfal_exists(path, voms_token=None):
  return gfal_ls_safe(path, voms_token=voms_token, catch_stderr=True, verbose=0) is not None

def gfal_sum(path, voms_token=None, sum_type='adler32'):
  if voms_token is None:
    voms_token = get_voms_proxy_info()['path']
  try:
    _, output, _ = ps_call(['gfal-sum', path, sum_type ],
                          shell=False, env={'X509_USER_PROXY': voms_token}, catch_stdout=True)
    sum_str = output.split(' ')[-1]
    sum_int = int(sum_str, 16)
  except PsCallError as e:
    raise GfalError(f'gfal_sum: unable to get {sum_type} for "{path}"\n{e}')
  except ValueError as e:
    raise GfalError(f'gfal_sum: unable to parse {sum_type} for "{path}".'
                    f'\ngfal-sum output:\n--------\n{output}--------\n{e}')
  return sum_int

def gfal_rm(path, voms_token=None, recursive=False):
  if voms_token is None:
    voms_token = get_voms_proxy_info()['path']
  cmd = ['gfal-rm']
  if recursive:
    cmd.append('-r')
  cmd.append(path)
  try:
    ps_call(cmd, shell=False, env={'X509_USER_PROXY': voms_token}, catch_stdout=True)
  except PsCallError as e:
    raise GfalError(f'gfal_rm: unable to remove "{path}"\n{e}')

def gfal_rename(path, new_path, voms_token=None):
  if voms_token is None:
    voms_token = get_voms_proxy_info()['path']
  try:
    ps_call(['gfal-rename', path, new_path], shell=False, env={'X509_USER_PROXY': voms_token}, catch_stdout=True)
  except PsCallError as e:
    raise GfalError(f'gfal_rename: unable to rename "{path}" to "{new_path}"\n{e}')

def lfn_to_pfn(server, lfn):
  from rucio.client import Client
  client = Client()
  key = f'user.jdoe:{lfn}'
  result = client.lfns2pfns(server, [ key ])
  return result[key]

def das_file_site_info(file, inputDBS='global', verbose=0):
  query = f'site file={file}'
  if inputDBS != 'global':
    query += f' instance=prod/{inputDBS}'
  _, output, _ = ps_call(['dasgoclient', '--json', '--query', query], catch_stdout=True, verbose=verbose)
  return json.loads(output)

def das_file_pfns(file, disk_only=True, return_adler32=False, inputDBS='global', verbose=0):
  site_info = das_file_site_info(file, inputDBS=inputDBS, verbose=verbose)
  pfns_disk = set()
  pfns_other = set()
  adler32 = None
  for entry in site_info:
    if "site" not in entry: continue
    for site in entry["site"]:
      if "pfns" not in site: continue
      for pfns_link, pfns_info in site["pfns"].items():
        if pfns_info.get("type", None) == "DISK":
          pfns_disk.add(pfns_link)
        else:
          pfns_other.add(pfns_link)
      if "adler32" in site:
        site_adler32 = int(site["adler32"], 16)
        if adler32 is not None and adler32 != site_adler32:
          raise RuntimeError(f"Inconsistent adler32 sum for {file}")
        adler32 = site_adler32
  pfns = list(pfns_disk)
  if not disk_only:
    pfns = list(pfns_disk) + list(pfns_other)
  if return_adler32:
    return pfns, adler32
  return pfns

def copy_remote_file(input_remote_file, output_local_file, inputDBS='global', n_retries=4, retry_sleep_interval=10,
                     custom_pfns_prefix='', voms_token=None, verbose=1):
  pfns_list, adler32 = das_file_pfns(input_remote_file, disk_only=False, return_adler32=True, inputDBS=inputDBS,
                                     verbose=verbose)
  if voms_token is None:
    voms_token = get_voms_proxy_info()['path']
  if os.path.exists(output_local_file):
    if adler32 is not None and check_download(output_local_file, expected_adler32sum=adler32):
      return
    os.remove(output_local_file)

  if len(pfns_list) == 0:
    if input_remote_file.startswith('/store/'):
      if custom_pfns_prefix != '':
        pfns_list = [custom_pfns_prefix+input_remote_file, input_remote_file]
      else:
        pfns_list = [input_remote_file]
    else:
      raise RuntimeError(f'Unable to find any remote location for {input_remote_file}.')

  def download(pfns):
    if verbose > 0:
      print(f"Trying to copy file from {pfns}")
    if pfns.startswith('root:') or pfns.startswith('/store/'):
      xrd_copy(pfns, output_local_file, expected_adler32sum=adler32, n_retries=1, prefixes=[''], verbose=verbose)
    elif pfns.startswith('srm:') or pfns.startswith('gsiftp') or pfns.startswith('davs:'):
      gfal_copy_safe(pfns, output_local_file, voms_token, expected_adler32sum=adler32, n_retries=1)
    else:
      raise RuntimeError('Skipping an unknown remote source "{pfns}".')

  repeat_until_success(download, opt_list=[ (pfns, ) for pfns in pfns_list ], raise_error=True,
                       error_message=f'Unable to copy {input_remote_file} from remote.', n_retries=n_retries,
                       retry_sleep_interval=retry_sleep_interval, verbose=verbose)

if __name__ == "__main__":
  import sys
  cmd = sys.argv[1]
  cmd_args = [ f'"{arg}"' for arg in sys.argv[2:] ]
  cmd_str = cmd + '(' + ','.join(cmd_args) + ')'
  print(f'> {cmd_str}')
  try:
    out = getattr(sys.modules[__name__], cmd)(*sys.argv[2:])
    if out is not None:
      try:
        out_str = json.dumps(out, indent=2)
      except TypeError:
        if type(out) == list:
          out_str = '\n'.join([ str(o) for o in out ])
        else:
          out_str = out
      print(out_str)
  except RuntimeError as e:
    print(f'ERROR: {type(e).__name__} -- {e}')
    sys.exit(1)
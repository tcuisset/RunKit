import datetime
import json
import os
import re
import subprocess
import sys
import time
import traceback
import zlib
from threading import Timer


class ShCallError(RuntimeError):
  def __init__(self, cmd_str, return_code, additional_message=None):
    msg = f'Error while running "{cmd_str}."'
    if return_code is not None:
      msg += f' Error code: {return_code}'
    if additional_message is not None:
      msg += f' {additional_message}'
    super(ShCallError, self).__init__(msg)
    self.cmd_str = cmd_str
    self.return_code = return_code
    self.message = additional_message

def sh_call(cmd, shell=False, catch_stdout=False, catch_stderr=False, decode=True, split=None, print_output=False,
            expected_return_codes=[0], env=None, cwd=None, timeout=None, verbose=0):
  cmd_str = []
  for s in cmd:
    if ' ' in s:
      s = f"'{s}'"
    cmd_str.append(s)
  cmd_str = ' '.join(cmd_str)
  if verbose > 0:
    print(f'>> {cmd_str}', file=sys.stderr)
  kwargs = {
    'shell': shell,
  }
  if catch_stdout:
    kwargs['stdout'] = subprocess.PIPE
  if catch_stderr:
    if print_output:
      kwargs['stderr'] = subprocess.STDOUT
    else:
      kwargs['stderr'] = subprocess.PIPE
  if env is not None:
    kwargs['env'] = env
  if cwd is not None:
    kwargs['cwd'] = cwd

  # psutil.Process.children does not work.
  def kill_proc(pid):
    child_list = subprocess.run(['ps', 'h', '--ppid', str(pid)], capture_output=True, encoding="utf-8")
    for line in child_list.stdout.split('\n'):
      child_info = line.split(' ')
      child_info = [ s for s in child_info if len(s) > 0 ]
      if len(child_info) > 0:
        child_pid = child_info[0]
        kill_proc(child_pid)
    subprocess.run(['kill', '-9', str(pid)], capture_output=True)

  proc = subprocess.Popen(cmd, **kwargs)
  def kill_main_proc():
    print(f'\nTimeout is reached while running:\n\t{cmd_str}', file=sys.stderr)
    print(f'Killing process tree...', file=sys.stderr)
    print(f'Main process PID = {proc.pid}', file=sys.stderr)
    kill_proc(proc.pid)

  timer = Timer(timeout, kill_main_proc) if timeout is not None else None
  try:
    if timer is not None:
      timer.start()
    if catch_stdout and print_output:
      output = b''
      err = b''
      for line in proc.stdout:
        output += line
        print(line.decode("utf-8"), end="")
      proc.stdout.close()
      proc.wait()
    else:
      output, err = proc.communicate()
  finally:
    if timer is not None:
      timer.cancel()
  if expected_return_codes is not None and proc.returncode not in expected_return_codes:
    raise ShCallError(cmd_str, proc.returncode)
  if decode:
    if catch_stdout:
      output_decoded = output.decode("utf-8")
      if split is None:
        output = output_decoded
      else:
        output = output_decoded.split(split)
    if catch_stderr:
      err_decoded = err.decode("utf-8")
      if split is None:
        err = err_decoded
      else:
        err = err_decoded.split(split)

  return proc.returncode, output, err

def get_voms_proxy_info():
  _, output, _ = sh_call(['voms-proxy-info'], catch_stdout=True, split='\n')
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

def update_kerberos_ticket(verbose=1):
  sh_call(['kinit', '-R'], verbose=verbose)

def timed_call_wrapper(fn, update_interval, verbose=0):
  last_update = None
  def update(*args, **kwargs):
    nonlocal last_update
    now = datetime.datetime.now()
    delta_t = (now - last_update).total_seconds() if last_update is not None else float("inf")
    if verbose > 0:
      print(f"timed_call for {fn.__name__}: delta_t = {delta_t} seconds")
    if delta_t >= update_interval:
      fn(*args, **kwargs)
      last_update = now
  return update

def adler32sum(file_name):
  block_size = 256 * 1024 * 1024
  asum = 1
  with open(file_name, 'rb') as f:
    while (data := f.read(block_size)):
      asum = zlib.adler32(data, asum)
  return asum

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

def repeat_until_success(fn, opt_list=([],), raise_error=True, error_message="", n_retries=4, retry_sleep_interval=10,
                         verbose=1):
  for n in range(n_retries):
    for opt in opt_list:
      try:
        fn(*opt)
        return True
      except:
        if verbose > 0:
          print(traceback.format_exc())
      if n != n_retries - 1:
        if verbose > 0:
          print(f'Waiting for {retry_sleep_interval} seconds before the next try.')
        time.sleep(retry_sleep_interval)

  if raise_error:
    raise RuntimeError(error_message)
  return False

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
    sh_call(xrdcp_args, verbose=1)

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

def webdav_copy(input_remote_file, output_local_file, voms_token, expected_adler32sum=None, n_retries=4,
                retry_sleep_interval=10, verbose=1):
  def download():
    if os.path.exists(output_local_file):
      os.remove(output_local_file)
    sh_call(['davix-get', input_remote_file, output_local_file, '-E', voms_token], verbose=verbose)
    check_download(output_local_file, expected_adler32sum=expected_adler32sum, remove_bad_file=True,
                   raise_error=True, remote_file=input_remote_file)

  repeat_until_success(download, raise_error=True, error_message=f'Unable to copy {input_remote_file} from remote.',
                       n_retries=n_retries, retry_sleep_interval=retry_sleep_interval, verbose=verbose)

def gfal_copy(input_remote_file, output_local_file, voms_token, number_of_streams=2, timeout=7200,
              expected_adler32sum=None, n_retries=4, retry_sleep_interval=10, verbose=1):
  def download():
    if os.path.exists(output_local_file):
      os.remove(output_local_file)
    sh_call(['gfal-copy', '-n', str(number_of_streams), '-t', str(timeout), input_remote_file, output_local_file,],
            shell=False, env={'X509_USER_PROXY': voms_token}, verbose=1)
    check_download(output_local_file, expected_adler32sum=expected_adler32sum, remove_bad_file=True,
                   raise_error=True, remote_file=input_remote_file)

  repeat_until_success(download, raise_error=True, error_message=f'Unable to copy {input_remote_file} from remote.',
                       n_retries=n_retries, retry_sleep_interval=retry_sleep_interval, verbose=verbose)

def gfal_ls_recursive(path, fileName):
  _, output, _ = sh_call(['gfal-ls', '-t 7200', path,],
                         shell=False, env={'X509_USER_PROXY': get_voms_proxy_info()['path']}, catch_stdout=True, verbose=0)
  output = [o for o in output.strip().split("\n")]
  outputFiles = []
  for o in output:
    if not o.endswith(".tar"):
      outputFiles.extend(gfal_ls_recursive(os.path.join(path, o), fileName))
    else:
      if o == fileName:
        outputFiles.append(os.path.join(path, o))
  return outputFiles

def das_file_site_info(file, inputDBS='global', verbose=0):
  query = f'site file={file}'
  if inputDBS != 'global':
    query += f' instance=prod/{inputDBS}'
  _, output, _ = sh_call(['dasgoclient', '--json', '--query', query], catch_stdout=True, verbose=verbose)
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
                     custom_pfns_prefix='', verbose=1):
  pfns_list, adler32 = das_file_pfns(input_remote_file, disk_only=False, return_adler32=True, inputDBS=inputDBS,
                                     verbose=verbose)
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
    # elif pfns.startswith('davs:'):
    #   voms_info = get_voms_proxy_info()
    #   webdav_copy(pfns, output_local_file, voms_info['path'], expected_adler32sum=adler32, n_retries=1)
    #   return
    elif pfns.startswith('srm:') or pfns.startswith('gsiftp') or pfns.startswith('davs:'):
      voms_info = get_voms_proxy_info()
      gfal_copy(pfns, output_local_file, voms_info['path'], expected_adler32sum=adler32, n_retries=1)
    else:
      raise RuntimeError('Skipping an unknown remote source "{pfns}".')

  repeat_until_success(download, opt_list=[ (pfns, ) for pfns in pfns_list ], raise_error=True,
                       error_message=f'Unable to copy {input_remote_file} from remote.', n_retries=n_retries,
                       retry_sleep_interval=retry_sleep_interval, verbose=verbose)

def natural_sort(l):
  convert = lambda text: int(text) if text.isdigit() else text.lower()
  alphanum_key = lambda key: [convert(c) for c in re.split('([0-9]+)', key)]
  return sorted(l, key=alphanum_key)

if __name__ == "__main__":
  import sys
  cmd = sys.argv[1]
  out = getattr(sys.modules[__name__], cmd)(*sys.argv[2:])
  if out is not None:
    out_t = type(out)
    if out_t in [list, dict]:
      print(json.dumps(out, indent=2))
    else:
      print(out)

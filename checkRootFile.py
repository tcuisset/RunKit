import os
import ROOT
import sys
import traceback

if __name__ == "__main__":
  file_dir = os.path.dirname(os.path.abspath(__file__))
  sys.path.append(os.path.dirname(file_dir))
  __package__ = 'RunKit'

from .run_tools import ps_call

def checkRootFile(root_file, tree_name, branches=None, verbose=1):
  try:
    df = ROOT.RDataFrame(tree_name, root_file)
    if branches is None or len(branches) == 0:
      branches = [ str(b) for b in df.GetColumnNames()]
    if verbose > 1:
      print(f'{root_file}: {tree_name} checking branches={branches}')
    hist_model = ROOT.RDF.TH1DModel('', '', 1, 0, 1)
    hists = [ df.Histo1D(hist_model, b) for b in branches]
    for h_idx, h in enumerate(hists):
      integral = h.GetValue().Integral()
      if verbose > 2:
        print(f'{root_file}: {tree_name}.{branches[h_idx]} integral={integral}')
    return True
  except:
    if verbose > 0:
      print(traceback.format_exc())
    return False

def checkRootFileSafe(root_file, tree_name, branches=None, verbose=1):
  cmd = [ sys.executable, __file__, '--run', '--tree', tree_name, '--verbose', str(verbose) ]
  if branches is not None and len(branches) > 0:
    cmd += ['--branches', ','.join(branches)]
  cmd += [ root_file ]
  returncode,_,_ = ps_call(cmd, expected_return_codes=None)
  return returncode == 0

if __name__ == "__main__":
  import argparse
  parser = argparse.ArgumentParser(description='Check that ROOT file is not corrupted.')
  parser.add_argument('--tree', required=True, type=str, help="Tree name")
  parser.add_argument('--branches', required=False, default=None, type=str,
                      help="Comma separated list of branches to check. If empty, all branches are checked.")
  parser.add_argument('--run', action='store_true',
                      help="Run the check in this process. Otherwise, run in a subprocess.")
  parser.add_argument('--verbose', required=False, type=int, default=1, help="verbosity level")
  parser.add_argument('root_file', type=str, nargs=1, help="root file to check")
  args = parser.parse_args()

  branches = None if args.branches is None else args.branches.split(',')
  if args.run:
    file_ok = checkRootFile(args.root_file[0], args.tree, args.branches, args.verbose)
  else:
    file_ok = checkRootFileSafe(args.root_file[0], args.tree, args.branches, args.verbose)
  exit_code = 0 if file_ok else 1
  if not args.run and args.verbose > 0:
    status = 'OK' if file_ok else 'CORRUPTED'
    print(f'{status} {args.root_file[0]}')
  sys.exit(exit_code)
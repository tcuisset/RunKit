import ROOT
import yaml
import os
import sys

if __name__ == "__main__":
  file_dir = os.path.dirname(os.path.abspath(__file__))
  sys.path.append(os.path.dirname(file_dir))
  __package__ = 'RunKit'

from .skim_tree import get_columns, select_items

def getColumns(input_file, tree, column_filters, verbose=0):
  df = ROOT.RDataFrame(tree, input_file)
  columns, _ = get_columns(df)
  if column_filters is not None:
    selected_columns = select_items(columns, column_filters, verbose=verbose)
    excluded_columns = [ c for c in columns if c not in selected_columns ]
  else:
    selected_columns = columns
    excluded_columns = []

  return selected_columns, excluded_columns

if __name__ == "__main__":
  import argparse
  parser = argparse.ArgumentParser(description='dump branches into yaml.')
  parser.add_argument('--trees', required=False, type=str, default=None, help="Comma separated list of trees")
  parser.add_argument('--output', required=True, type=str, help="Output yaml file")
  parser.add_argument('--config', required=False, type=str, default=None, help="Configuration with the skim setup.")
  parser.add_argument('--setups', required=False, type=str, default=None,
                      help="Comma separate names of the setup descriptors in the configuration file.")
  parser.add_argument('--verbose', required=False, type=int, default=1, help="verbosity level")
  parser.add_argument('input_file', type=str, nargs=1, help="input root file")
  args = parser.parse_args()

  output_dict = {}
  if args.config:
    for arg_name in [ 'trees' ]:
      value = getattr(args, arg_name)
      if value:
        raise RuntimeError(f"--config and --{arg_name} can not be specified together.")
    with open(args.config, 'r') as f:
      skim_config = yaml.safe_load(f)
    for setup_name in args.setups.split(','):
      if setup_name not in skim_config:
        raise RuntimeError(f"Setup {setup} not found in the configuration file.")
      setup = skim_config[setup_name]
      output_dict[setup_name] = {}
      column_filters = setup.get('column_filters', None)
      selected_columns, excluded_columns = getColumns(args.input_file[0], setup['input_tree'], column_filters,
                                                      verbose=args.verbose)
      output_dict[setup_name]['selected_columns'] = selected_columns
      output_dict[setup_name]['excluded_columns'] = excluded_columns
  else:
    column_filters = None
    for tree in args.trees.split(','):
      selected_columns, excluded_columns = getColumns(args.input_file[0], tree, column_filters, verbose=args.verbose)
      output_dict[tree] = selected_columns

  with open(args.output, 'w') as f:
    yaml.dump(output_dict, f)

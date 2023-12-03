import yaml
import re

if __name__ == "__main__":
  import argparse
  parser = argparse.ArgumentParser(description='create crabOverseer dataset yaml config from list of das samples.')
  parser.add_argument('--input', required=True, type=str, help="Input txt file with a dataset list")
  parser.add_argument('--output', required=True, type=str, help="Output yaml file")
  parser.add_argument('--pattern', required=False, type=str, default='^/([^/]*)/',
                      help="regex pattern to define user-friendly names")
  args = parser.parse_args()

  output_dict = {}
  with open(args.input, 'r') as f:
    for line in f.readlines():
      line = line.strip()
      if len(line) == 0 or line[0] == '#':
        continue
      match = re.match(args.pattern, line)
      if not match:
        raise RuntimeError(f"Failed to match {line} with {args.pattern}")
      name = match.group(1)
      print(f'{name}: {line}')
      output_dict[name] = line

  with open(args.output, 'w') as f:
    yaml.dump(output_dict, f)

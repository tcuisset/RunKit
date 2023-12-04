#/bin/bash

job_sh_dir="$(dirname "$0")"
job_py="$job_sh_dir/crabJob.py"
if ! [ -f "$job_py" ] ; then
  echo "ERROR: crabJob.py not found"
  exit 1
fi
export HOME="$job_sh_dir"
echo $(which python3)
echo "Running job: $job_py $@"
python3 -u "$job_py" "$@"
JOB_RESULT=$?
echo "Job result: $JOB_RESULT"

exit $JOB_RESULT

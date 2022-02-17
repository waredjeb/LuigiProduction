
import sys
sys.path.append("..")

import os
import argparse

from utils.utils import (
  setPureInputNamespace,
)

@setPureInputNamespace
def writeHTCondorDAGFiles_outputs(args):
  """
  Outputs are guaranteed to have the same length.
  Returns all separate paths to avoid code duplication.
  """
  outSubmDir = 'submission'
  submDir = os.path.join(args.localdir, 'jobs', args.tag, outSubmDir)
  os.system('mkdir -p {}'.format(jobDir))
  outCheckDir = 'outputs'
  checkDir = os.path.join(args.localdir, 'jobs', args.tag, outCheckDir)
  os.system('mkdir -p {}'.format(checkDir))

  name = 'workflow.dag'
  submFile  = os.path.join(submDir, name)

  return submFile

@setPureInputNamespace
def writeHTCondorEfficienciesAndScaleFactorsFiles(args):
    script = os.path.join(args.localdir, 'scripts', 'runEfficienciesAndScaleFactors.py')
    prog = 'python3 {}'.format(script)

    outs_submit = writeHTCondorEfficienciesAndScaleFactorsFiles_outputs(args)
    
    with open(outs_submit, 'w') as s:
        s.write('JOB  A  A.condor\n')
        s.write('JOB  B  B.condor\n')
        s.write('JOB  C  C.condor\n')
        s.write('JOB  D  D.condor\n')
        s.write('PARENT A CHILD B C\n')
        s.write('PARENT B C CHILD D\n')

# condor_submit_dag -no_submit diamond.dag
# condor_submit diamond.dag.condor.sub
# https://htcondor.readthedocs.io/en/latest/users-manual/dagman-workflows.html#optimization-of-submission-time
# DAGMAN_HOLD_CLAIM_TIME=30

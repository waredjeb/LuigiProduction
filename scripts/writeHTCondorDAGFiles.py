
import sys
sys.path.append("..")

import os
import argparse

from utils.utils import (
  setPureInputNamespace,
)

from scripts.writeHTCondorHistogramFiles import (
    writeHTCondorHistogramFiles_outputs,
)
from scripts.writeHTCondorHaddFiles import (
    writeHTCondorHaddFiles_outputs,
)
from scripts.writeHTCondorEfficienciesAndScaleFactorsFiles import (
    writeHTCondorEfficienciesAndScaleFactorsFiles_outputs,
)

@setPureInputNamespace
def writeHTCondorDAGFiles_outputs(args):
  """
  Outputs are guaranteed to have the same length.
  Returns all separate paths to avoid code duplication.
  """
  outSubmDir = 'submission'
  submDir = os.path.join(args.localdir, 'jobs', args.tag, outSubmDir)
  os.system('mkdir -p {}'.format(submDir))
  # outCheckDir = 'outputs'
  # checkDir = os.path.join(args.localdir, 'jobs', args.tag, outCheckDir)
  # os.system('mkdir -p {}'.format(checkDir))

  name = 'workflow.dag'
  return os.path.join(submDir, name)

@setPureInputNamespace
def writeHTCondorDAGFiles(args):
  """
  Writes the condor submission DAG file.
  """
  remExt = lambda x : os.path.basename(x).split('.')[0]

  def defineJobNames(afile, jobs):
    """First step to build a DAG"""
    if not isinstance(jobs, (list,tuple)):
      jobs = [jobs]
    for job in jobs:
      afile.write('JOB  {} {}\n'.format(remExt(job), job))
    afile.write('\n')

  out = writeHTCondorDAGFiles_outputs(args)
  with open(out, 'w') as s:
    # configuration
    #s.write('DAGMAN_HOLD_CLAIM_TIME=30\n')
    #s.write('\n')
    
    # job names
    defineJobNames(s, args.jobsHistos)
    defineJobNames(s, args.jobsCounts)
    defineJobNames(s, args.jobsHaddData)
    defineJobNames(s, args.jobsHaddMC)
    defineJobNames(s, args.jobsEffSF)
    defineJobNames(s, args.jobsDiscr)
    defineJobNames(s, args.jobsUnion)

    # histos to hadd for data
    s.write('PARENT ')
    for parent in args.jobsHistos:
      if args.data_name in parent:
        s.write('{} '.format( remExt(parent) ))
    s.write('CHILD {}\n'.format( remExt(args.jobsHaddData[0]) ))

    # histos to hadd for MC
    s.write('PARENT ')
    for parent in args.jobsHistos:
      if args.data_name not in parent:
        s.write('{} '.format( remExt(parent) ))
    s.write('CHILD {}\n\n'.format( remExt(args.jobsHaddMC[0]) ))

    # hadd aggregation for Data
    s.write('PARENT {} '.format( remExt(args.jobsHaddData[0]) ))
    s.write('CHILD {}\n'.format( remExt(args.jobsHaddData[1]) ))

    # hadd aggregation for MC
    s.write('PARENT {} '.format( remExt(args.jobsHaddMC[0]) ))
    s.write('CHILD {}\n\n'.format( remExt(args.jobsHaddMC[1]) ))

    # efficiencies/scale factors draw and saving
    s.write('PARENT {} {} '.format( remExt(args.jobsHaddData[1]),
                                    remExt(args.jobsHaddMC[1]) ))
    s.write('CHILD {}\n\n'.format( remExt(args.jobsEffSF) ))

    # variable discriminator
    s.write('PARENT {} '.format( remExt(args.jobsEffSF) ))
    s.write('CHILD ')
    for child in args.jobsDiscr:
      s.write('{} '.format(remExt(child)))
    s.write('\n\n')

    # union weights calculator
    for parent, child in zip(args.jobsDiscr,args.jobsUnion):
      s.write('PARENT {} CHILD {}\n'.format(remExt(parent), remExt(child)))
    s.write('\n')

# condor_submit_dag -no_submit diamond.dag
# condor_submit diamond.dag.condor.sub
# https://htcondor.readthedocs.io/en/latest/users-manual/dagman-workflows.html#optimization-of-submission-time


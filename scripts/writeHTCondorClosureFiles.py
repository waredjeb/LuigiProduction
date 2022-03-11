import sys
sys.path.append("..")

import os
import argparse

from utils.utils import (
  generateTriggerCombinations,
  joinNameTriggerIntersection as joinNTC,
  setPureInputNamespace,
)

@setPureInputNamespace
def writeHTCondorClosureFiles_outputs(args):
  """
  Outputs are guaranteed to have the same length.
  Returns all separate paths to avoid code duplication.
  """
  base_dir = os.path.join(args.localdir, 'jobs', args.tag)
  specific_str = 'Closure'
  
  jobDir = os.path.join(base_dir, 'submission')
  os.system('mkdir -p {}'.format(jobDir))

  checkDir = os.path.join(base_dir, 'outputs', specific_str)
  os.system('mkdir -p {}'.format(checkDir))

  name = 'job{}.{}'
  check_name = specific_str + '_C$(Cluster)P$(Process).o'

  jobFiles   = os.path.join(jobDir, name.format(specific_str, 'sh'))
  submFiles  = os.path.join(jobDir, name.format(specific_str, 'condor'))
  checkFiles = os.path.join(checkDir, check_name)

  return jobFiles, submFiles, checkFiles

@setPureInputNamespace
def writeHTCondorClosureFiles(args):
    script = os.path.join(args.localdir, 'scripts', 'runClosure.py')
    prog = 'python3 {}'.format(script)

    outs_job, outs_submit, outs_check = writeHTCondorClosureFiles_outputs(args)

    #### Write shell executable (python scripts must be wrapped in shell files to run on HTCondor)
    command =  ( '{prog} --indir_ref {inref} '.format(prog=prog, inref=args.indir_ref)
                 + '--indir_union {inunion} '.format(inunion=args.outdir)
                 + '--outdir {outdir} '.format(outdir=args.outdir)
                 + '--channel ${1} '
                 + '--variables {variables} '.format(variables=' '.join(args.variables))
                 + '--triggers {triggers} '.format(triggers=' '.join(args.triggers))
                 + '--subtag {subtag} '.format(subtag=args.subtag)
                )
    
    if args.debug:
        command += '--debug '
    command += '\n'

    with open(outs_job, 'w') as s:
        s.write('#!/bin/bash\n')
        s.write('export X509_USER_PROXY=~/.t3/proxy.cert\n')
        s.write('export EXTRA_CLING_ARGS=-O2\n')
        s.write('source /cvmfs/cms.cern.ch/cmsset_default.sh\n')
        s.write('cd {}/\n'.format(args.localdir))
        s.write('eval `scramv1 runtime -sh`\n')
        s.write(command)
        s.write('echo "runClosure for channel ${1} done."\n')
    os.system('chmod u+rwx '+ outs_job)

    #### Write submission file
    queue = 'short'
    queuevar = 'channel'
    triggercomb = generateTriggerCombinations(args.triggers)
    with open(outs_submit, 'w') as s:
        s.write('Universe = vanilla\n')
        s.write('Executable = {}\n'.format(outs_job))
        s.write('Arguments = $({}) \n'.format(queuevar))
        s.write('input = /dev/null\n')
        s.write('output = {}\n'.format(outs_check))
        s.write('error  = {}\n'.format(outs_check.replace('.o', '.e')))
        s.write('getenv = true\n')
        s.write('T3Queue = {}\n'.format(queue))
        s.write('WNTag=el7\n')
        s.write('+SingularityCmd = ""\n')
        s.write('include : /opt/exp_soft/cms/t3/t3queue |\n\n')
        s.write('queue {} from (\n'.format(queuevar))
        for chn in args.channels:
            s.write('  {}\n'.format(chn))
        s.write(')\n')

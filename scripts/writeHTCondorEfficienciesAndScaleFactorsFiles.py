
import sys
sys.path.append("..")

import os
import argparse

from utils.utils import (
  generate_trigger_combinations,
  join_name_trigger_intersection as joinNTC,
  setPureInputNamespace,
)

@setPureInputNamespace
def writeHTCondorEfficienciesAndScaleFactorsFiles_outputs(args):
  """
  Outputs are guaranteed to have the same length.
  Returns all separate paths to avoid code duplication.
  """
  base_dir = os.path.join(args.localdir, 'jobs', args.tag)
  
  jobDir = os.path.join(base_dir, 'submission')
  os.system('mkdir -p {}'.format(jobDir))

  checkDir = os.path.join(base_dir, 'outputs', 'EffAndScaleFactors')
  os.system('mkdir -p {}'.format(checkDir))

  name = 'jobEfficienciesAndSF.{}'
  check_name = 'EfficienciesAndSF_C$(Cluster)P$(Process).o'

  jobFiles   = os.path.join(jobDir, name.format('sh'))
  submFiles  = os.path.join(jobDir, name.format('condor'))
  checkFiles = os.path.join(checkDir, check_name)

  return jobFiles, submFiles, checkFiles

@setPureInputNamespace
def writeHTCondorEfficienciesAndScaleFactorsFiles(args):
    script = os.path.join(args.localdir, 'scripts', 'runEfficienciesAndScaleFactors.py')
    prog = 'python3 {}'.format(script)

    outs_job, outs_submit, outs_check = writeHTCondorEfficienciesAndScaleFactorsFiles_outputs(args)

    #### Write shell executable (python scripts must be wrapped in shell files to run on HTCondor)
    command =  ( ( '{prog} --indir {indir} --outdir {outdir} '
                   '--mc_processes {mc_processes} '
                   '--mc_name {mc_name} --data_name {data_name} '
                   '--triggercomb ${{1}} '
                   '--channels {channels} --variables {variables} '
                   '--binedges_filename {binedges_filename} --subtag {subtag} '
                   '--tprefix {tprefix} '
                   '--canvas_prefix {cprefix} '
                  ).format( prog=prog, indir=args.indir, outdir=args.outdir,
                            mc_processes=' '.join(args.mc_processes,),
                            mc_name=args.mc_name, data_name=args.data_name,
                            channels=' '.join(args.channels,), variables=' '.join(args.variables,),
                            binedges_filename=args.binedges_filename,
                            subtag=args.subtag,
                            draw_independent_MCs=1 if args.draw_independent_MCs else 0,
                            tprefix=args.tprefix,
                            cprefix=args.canvas_prefix,
                           )
                )

    if args.draw_independent_MCs:
        command += '--draw_independent_MCs '
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
        s.write('echo "runEfficienciesAndScaleFactors done."\n')
    os.system('chmod u+rwx '+ outs_job)

    #### Write submission file
    queue = 'short'
    queuevar = 'triggercomb'
    triggercomb = generate_trigger_combinations(args.triggers)
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
        for tcomb in triggercomb:
            s.write('  {}\n'.format(joinNTC(tcomb)))
        s.write(')\n')

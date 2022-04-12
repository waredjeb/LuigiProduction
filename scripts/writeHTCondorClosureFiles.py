import sys
sys.path.append("..")

import os
import argparse

from utils.utils import (
    build_prog_path,
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
    outs_job, outs_submit, outs_check = writeHTCondorClosureFiles_outputs(args)

    #### Write shell executable (python scripts must be wrapped in shell files to run on HTCondor)
    prog = build_prog_path(args.localdir, 'runClosure.py')
    command =  ( '{prog} --indir_eff {inref} '.format(prog=prog, inref=args.indir_eff)
                 + '--indir_union {inunion} '.format(inunion=args.indir_union)
                 + '--indir_json {injson} '.format(injson=args.indir_json)
                 + '--mc_processes {procs} '.format(procs=' '.join(args.mc_processes))
                 + '--outdir {outdir} '.format(outdir=args.outdir)
                 + '--in_prefix {inprefix} '.format(inprefix=args.inprefix)
                 + '--channel ${1} '
                 + '--closure_single_trigger ${2} '
                 + '--variables {variables} '.format(variables=' '.join(args.variables))
                 + '--subtag {subtag} '.format(subtag=args.subtag)
                 + '--binedges_fname {be} '.format(be=args.binedges_filename)
                 + '--data_name {dn} '.format(dn=args.data_name)
                 + '--mc_name {mn} '.format(mn=args.mc_name)
                 + '--eff_prefix {effprefix} '.format(effprefix=args.eff_prefix)
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
        s.write('echo "runClosure for channel ${1} and single trigger ${2} done."\n')
    os.system('chmod u+rwx '+ outs_job)

    #### Write submission file
    queue = 'short'
    queuevars = 'channel', 'closure_single_trigger'
    with open(outs_submit, 'w') as s:
        s.write('Universe = vanilla\n')
        s.write('Executable = {}\n'.format(outs_job))
        s.write('Arguments = $({}) $({}) \n'.format(queuevars[0],queuevars[1]))
        s.write('input = /dev/null\n')
        s.write('output = {}\n'.format(outs_check))
        s.write('error  = {}\n'.format(outs_check.replace('.o', '.e')))
        s.write('getenv = true\n')
        s.write('T3Queue = {}\n'.format(queue))
        s.write('WNTag=el7\n')
        s.write('+SingularityCmd = ""\n')
        s.write('include : /opt/exp_soft/cms/t3/t3queue |\n\n')
        s.write('queue {},{} from (\n'.format(queuevars[0],queuevars[1]))
        for chn in args.channels:
            for trig in args.closure_single_triggers:
                s.write('  {},{}\n'.format(chn,trig))
        s.write(')\n')

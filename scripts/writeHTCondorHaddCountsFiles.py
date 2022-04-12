import os
import sys
from utils import utils

@utils.setPureInputNamespace
def runHaddCounts_outputs(args):
    targets = []

    # add the merge of all the samples first
    _tbase1, _tbase2 = utils.hadd_subpaths(args)
    tbase = _tbase1 + _tbase2
    t = os.path.join( args.indir, tbase + '.txt' )
    targets.append( t )

    # add individual sample merges
    for smpl in args.samples:
        tbase = _tbase1 + '_' + smpl + _tbase2
        t = os.path.join( args.indir, tbase + '.txt' )
        targets.append( t )
        
    return targets

@utils.setPureInputNamespace
def writeHTCondorHaddCountsFiles_outputs(args):
    """
    Outputs are guaranteed to have the same length.
    Returns all separate paths to avoid code duplication.
    """
    base_dir = os.path.join(args.localdir, 'jobs', args.tag)
    
    jobDir = os.path.join(base_dir, 'submission')
    os.system('mkdir -p {}'.format(jobDir))

    dataset_folder = 'HaddCounts_' + args.dataset_name
    checkDir = os.path.join(base_dir, 'outputs', dataset_folder)
    os.system('mkdir -p {}'.format(checkDir))

    name = 'jobHaddCounts{}_{}.{}'
    check_name = '{}_{}HaddCounts_C$(Cluster)P$(Process).o'

    jobFiles   = [ os.path.join(jobDir, name.format('',    args.dataset_name, 'sh')),
                   os.path.join(jobDir, name.format('Agg', args.dataset_name, 'sh')) ]
    submFiles  = [ os.path.join(jobDir, name.format('',    args.dataset_name, 'condor')),
                   os.path.join(jobDir, name.format('Agg', args.dataset_name, 'condor')) ]
    checkFiles = [ os.path.join(checkDir, check_name.format(args.dataset_name, '')),
                   os.path.join(checkDir, check_name.format(args.dataset_name, 'Agg_')) ]

    return jobFiles, submFiles, checkFiles

@utils.setPureInputNamespace
def writeHTCondorHaddCountsFiles(args):
    """Adds TXT count files"""
    targets = runHaddCounts_outputs(args)
    outs_job, outs_submit, outs_check = writeHTCondorHaddCountsFiles_outputs(args)

    prog = utils.build_prog_path(args.localdir, 'addTriggerCounts.py')
    command_base =  ( '{prog} --indir {indir} '.format( prog=prog, indir=args.indir) +
                      '--outdir {outdir} '.format(outdir=args.outdir) +
                      '--subtag {subtag} '.format(subtag=args.subtag) +
                      '--tprefix {tprefix} '.format(tprefix=args.tprefix) +
                      '--dataset_name {dn} '.format(dn=args.dataset_name) +
                      '--outfile_counts ${1} '
                     )

    command_first_step = ( command_base +
                           '--sample ${2} ' +
                           ' --aggregation_step 0\n' )
    command_aggregation_step = ( command_base + '--infile_counts ${2} --aggregation_step 1 \n')
    
    #### Write shell executable (python scripts must be wrapped in shell files to run on HTCondor)
    for out in outs_job:
        with open(out, 'w') as s:
            s.write('#!/bin/bash\n')
            s.write('export X509_USER_PROXY=~/.t3/proxy.cert\n')
            s.write('export EXTRA_CLING_ARGS=-O2\n')
            s.write('source /cvmfs/cms.cern.ch/cmsset_default.sh\n')
            #s.write('cd /home/llr/cms/alves/CMSSW_12_2_0_pre1/src/\n')
            s.write('cd {}/\n'.format(args.localdir))
            s.write('eval `scramv1 runtime -sh`\n')
            if out == outs_job[0]:
                s.write(command_first_step)
                s.write('echo "HaddCounts {} done."\n'.format(args.dataset_name))
            elif out == outs_job[1]:
                s.write(command_aggregation_step)
                s.write('echo "HaddCounts Agg {} done."\n'.format(args.dataset_name))
        os.system('chmod u+rwx '+ out)

    #### Write submission file
    inputs_join = []
    queue = 'short'
    for out1,out2,out3 in zip(outs_job,outs_submit,outs_check):
        with open(out2, 'w') as s:
            s.write('Universe = vanilla\n')
            s.write('Executable = {}\n'.format(out1))
            if out1 == outs_job[0]:
                s.write('Arguments = $(myoutput) $(sample) \n')
            elif out1 == outs_job[1]:
                s.write('Arguments = $(myoutput) $(myinputs) \n')
            s.write('input = /dev/null\n')
            s.write('output = {}\n'.format(out3))
            s.write('error  = {}\n'.format(out3.replace('.o', '.e')))
            s.write('getenv = true\n')
            s.write('T3Queue = {}\n'.format(queue))
            s.write('WNTag=el7\n')
            s.write('+SingularityCmd = ""\n')
            s.write('include : /opt/exp_soft/cms/t3/t3queue |\n\n')

            if out1 == outs_job[0]:
                s.write('queue myoutput, sample from (\n')
                for t,smpl in zip(targets[1:], args.samples):
                    inputs = os.path.join(args.indir, smpl, args.tprefix + '*' + args.subtag + '.txt')
                    inputs_join.append(t)
                    # join subdatasets (different MC or Data subfolders, ex: TT_fullyHad, TT_semiLep, ...)
                    #s.write('  {}, {}\n'.format(t, inputs))
                    s.write('  {}, {}\n'.format(t,smpl))
            elif out1 == outs_job[1]:
                s.write('queue myoutput, myinputs from (\n')
                # join MC or Data subdatasets into a single one (ex: TT)
                s.write('  {}, {}\n'.format(targets[0], ' '.join(inputs_join)))
            s.write(')\n')

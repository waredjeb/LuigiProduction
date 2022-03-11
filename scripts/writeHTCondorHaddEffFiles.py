import os
import sys
from utils import utils

def _subpaths(args):
    _tbase1 = args.outprefix
    _tbase2 = '_Sum' + args.subtag
    return _tbase1, _tbase2

@utils.setPureInputNamespace
def runHaddEff_outputs(args):
    targets = []

    # add the merge of all the samples first
    _tbase1, _tbase2 = _subpaths(args)
    tbase = _tbase1 + _tbase2
    t = os.path.join( args.indir, tbase + '.root' )
    targets.append( t )

    # add individual sample merges
    for smpl in args.samples:
        tbase = _tbase1 + '_' + smpl + _tbase2
        t = os.path.join( args.indir, tbase + '.root' )
        targets.append( t )
        
    return targets

@utils.setPureInputNamespace
def writeHTCondorHaddEffFiles_outputs(args):
    """
    Outputs are guaranteed to have the same length.
    Returns all separate paths to avoid code duplication.
    """
    base_dir = os.path.join(args.localdir, 'jobs', args.tag)
    
    jobDir = os.path.join(base_dir, 'submission')
    os.system('mkdir -p {}'.format(jobDir))

    dataset_folder = 'HaddEff'
    checkDir = os.path.join(base_dir, 'outputs', dataset_folder)
    os.system('mkdir -p {}'.format(checkDir))

    name = 'jobHaddEff{}.{}'
    check_name = '{}HaddEff_C$(Cluster)P$(Process).o'

    jobFiles   = [ os.path.join(jobDir, name.format('', 'sh')),
                   os.path.join(jobDir, name.format('_Agg', 'sh')) ]
    submFiles  = [ os.path.join(jobDir, name.format('', 'condor')),
                   os.path.join(jobDir, name.format('_Agg', 'condor')) ]
    checkFiles = [ os.path.join(checkDir, check_name.format('')),
                   os.path.join(checkDir, check_name.format('Agg_')) ]

    return jobFiles, submFiles, checkFiles

@utils.setPureInputNamespace
def writeHTCondorHaddEffFiles(args):
    """Adds ROOT histograms"""
    targets = runHaddEff_outputs(args)
    outs_job, outs_submit, outs_check = writeHTCondorHaddEffFiles_outputs(args)
        
    #### Write shell executable (python scripts must be wrapped in shell files to run on HTCondor)
    command = 'hadd -f ${1} ${@:2}\n' #bash: ${@:POS} captures all arguments starting from POS
    for out in outs_job:
        with open(out, 'w') as s:
            s.write('#!/bin/bash\n')
            s.write('export X509_USER_PROXY=~/.t3/proxy.cert\n')
            s.write('export EXTRA_CLING_ARGS=-O2\n')
            s.write('source /cvmfs/cms.cern.ch/cmsset_default.sh\n')
            #s.write('cd /home/llr/cms/alves/CMSSW_12_2_0_pre1/src/\n')
            s.write('cd {}/\n'.format(args.localdir))
            s.write('eval `scramv1 runtime -sh`\n')
            s.write(command)
            if out == outs_job[0]:
                s.write('echo "HaddEff done."\n')
            elif out == outs_job[1]:
                s.write('echo "HaddEff Agg done."\n')
        os.system('chmod u+rwx '+ out)

    #### Write submission file
    inputs_join = []
    queue = 'short'
    for out1,out2,out3 in zip(outs_job,outs_submit,outs_check):
        with open(out2, 'w') as s:
            s.write('Universe = vanilla\n')
            s.write('Executable = {}\n'.format(out1))
            s.write('Arguments = $(myoutput) $(myinputs) \n')
            s.write('input = /dev/null\n')
            s.write('output = {}\n'.format(out3))
            s.write('error  = {}\n'.format(out3.replace('.o', '.e')))
            s.write('getenv = true\n')
            s.write('T3Queue = {}\n'.format(queue))
            s.write('WNTag=el7\n')
            s.write('+SingularityCmd = ""\n')
            s.write('include : /opt/exp_soft/cms/t3/t3queue |\n\n')
            s.write('queue myoutput, myinputs from (\n')

            if out1 == outs_job[0]:
                for t,smpl in zip(targets[1:], args.samples):
                    inputs = os.path.join(args.indir, smpl, args.outprefix + '*' + args.subtag + '.root ')
                    inputs_join.append(t)
                    # join subdatasets (different MC or Data subfolders, ex: TT_fullyHad, TT_semiLep, ...)
                    s.write('  {}, {}\n'.format(t, inputs))
            elif out1 == outs_job[1]:
                # join MC or Data subdatasets into a single one (ex: TT)
                s.write('  {}, {}\n'.format(targets[0], ' '.join(inputs_join)))
            s.write(')\n')

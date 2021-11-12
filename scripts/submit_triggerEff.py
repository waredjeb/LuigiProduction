###### DOCSTRING ####################################################
# Submits all the jobs required to obtain the trigger scale factors
# Run example:
# python3 submit_triggerEff.py
#         --indir /data_CMS/cms/portales/HHresonant_SKIMS/SKIMS_Radion_2018_fixedMETtriggers_mht_16Jun2021/
#         --outdir /data_CMS/cms/alves/TriggerScaleFactors
#         --tag v1
#         --proc MET2018A MET2018B MET2018C MET2018D
#         --channels all etau mutau tautau mumu
####################################################################

import os, sys
import argparse
import fileinput
import time
import glob
import random
import subprocess
from os.path import basename
import ROOT

# -- Parse options
parser = argparse.ArgumentParser(description='Command line parser')
parser.add_argument('-a', '--indir',     dest='indir',     required=True, help='in directory')
parser.add_argument('-o', '--outdir',    dest='outdir',    required=True, help='out directory')
parser.add_argument('-t', '--tag',       dest='tag',       required=True, help='tag')
parser.add_argument('-p', '--processes', dest='processes', required=True, nargs='+', type=str,
                    help='list of process names')
parser.add_argument('-c', '--channels',  dest='channels',  required=True, nargs='+', type=str,
                    help='Select the channels over which the workflow will be run.' )
args = parser.parse_args()

# -- Check input folder
if not os.path.exists(args.indir):
    print('input folder {} not existing, exiting'.format(args.indir))
    exit(1)

home = os.environ['HOME']
cmssw = os.path.join(os.environ['CMSSW_VERSION'], 'src')
prog = 'python3 {}'.format( os.path.join(home, cmssw, 'METTriggerStudies', 'scripts', 'getTriggerEffSig.py') )

# -- Job steering files
currFolder = os.getcwd()
jobsDir = os.path.join(currFolder, 'jobs')

for thisProc in args.processes:

    #### Input list
    inputfiles = os.path.join(args.indir, 'SKIM_' + thisProc + '/goodfiles.txt')

    #### Parse input list
    filelist=[]
    with open(inputfiles) as fIn:
        for line in fIn:
            if '.root' in line:
                filelist.append(line)

    #### Write shell executable (python scripts must be wrapped in shell files to run on HTCondor)
    outSubmDir = 'submission'
    jobDir = os.path.join(jobsDir, outSubmDir)
    jobFile = os.path.join(jobDir, 'job_{}.sh'.format(thisProc))

    command =  ( ( '{prog} --indir {indir} --outdir {outdir} --sample {sample} '
                   '--file ${{1}} --subtag metnomu200cut --channels {channels}\n' )
                 .format( prog=prog, indir=args.indir, outdir=os.path.join(args.outdir, args.tag+'/'),
                          sample=thisProc, channels=' '.join(args.channels) )
                )
    os.system('mkdir -p {}'.format(jobDir))

    with open(jobFile, 'w') as s:
        s.write('#!/bin/bash\n')
        s.write('export X509_USER_PROXY=~/.t3/proxy.cert\n')
        s.write('export EXTRA_CLING_ARGS=-O2\n')
        s.write('source /cvmfs/cms.cern.ch/cmsset_default.sh\n')
        s.write('cd /home/llr/cms/alves/CMSSW_12_2_0_pre1/src/\n')
        s.write('eval `scramv1 runtime -sh`\n')            
        s.write(command)
        s.write('echo "Process {} done."\n'.format(thisProc))
    os.system('chmod u+rwx '+ jobFile)

    #### Write submission file
    submDir = os.path.join(jobsDir, outSubmDir)
    submFile = os.path.join(submDir, 'job_'+thisProc+'.submit')
    outCheckDir = 'outputs'
    modstring = lambda x : x.replace(args.indir,'').replace('/','').replace('SKIM_','').replace(thisProc,'').replace('\n','')
    queue = 'short'
    os.system('mkdir -p {}'.format(submDir))
    
    with open(submFile, 'w') as s:
        s.write('Universe = vanilla\n')
        s.write('Executable = {}\n'.format(jobFile))
        s.write('Arguments = $(filename) \n'.format(jobFile))
        s.write('input = /dev/null\n')
        _outfile = '{}/{}/{}/Cluster$(Cluster)_Process$(Process)'.format(jobsDir, outCheckDir, thisProc)
        s.write('output = {}.o\n'.format(_outfile))
        s.write('error  = {}.e\n'.format(_outfile))
        s.write('getenv = true\n')
        s.write('T3Queue = {}\n'.format(queue))
        s.write('WNTag=el7\n')
        s.write('+SingularityCmd = ""\n')
        s.write('include : /opt/exp_soft/cms/t3/t3queue |\n\n')
        s.write('queue filename from (\n')
        for listname in filelist:
            s.write('  {}\n'.format( modstring(listname)))
        s.write(')\n')
    os.system('mkdir -p {}'.format( os.path.join(jobsDir, outCheckDir, thisProc) ))

    os.system('condor_submit -name llrt3condor {}'.format(submFile))

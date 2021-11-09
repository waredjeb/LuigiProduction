import os, sys
import argparse
import fileinput
import time
import glob
import subprocess
from os.path import basename
import ROOT


#Run example (but is being called by the corresponding bash script
#python3 /home/llr/cms/alves/METTriggerStudies/scripts/submit_triggerEff.py --indir /data_CMS/cms/portales/HHresonant_SKIMS/SKIMS_Radion_2018_fixedMETtriggers_mht_16Jun2021/ --outdir /data_CMS/cms/alves/FRAMEWORKTEST/ --proc MET2018A --channels all etau mutau tautau mumu

# -- Parse options
parser = argparse.ArgumentParser(description='Command line parser')
parser.add_argument('-a', '--indir',    dest='indir',    required=True, help='in directory')
parser.add_argument('-o', '--outdir',   dest='outdir',   required=True, help='out directory')
parser.add_argument('-p', '--proc',     dest='proc',     required=True, help='process name')
parser.add_argument('-c', '--channels', dest='channels', required=True, nargs='+', type=str,
                    help='Select the channels over which the workflow will be run.' )
args = parser.parse_args()

# -- Check input folder
if not os.path.exists(args.indir):
    print('input folder', args.indir, 'not existing, exiting')
    exit(1)

# -- Input list
inputfiles = os.path.join(args.indir, 'SKIM_' + args.proc + '/goodfiles.txt')

# -- Job steering files
currFolder = os.getcwd ()
jobsDir = os.path.join(currFolder, 'jobs')
n = int(0)

# -- Parse input list
filelist=[]
with open(inputfiles) as fIn:
    for line in fIn:
        if '.root' in line:
            filelist.append(line)

home = os.environ['HOME']
prog = 'python3 ' + os.path.join(home, 'METTriggerStudies/scripts/', 'getTriggerEffSig.py')

with open( os.path.join(jobsDir, 'submit.sh'), 'w') as commandFile:
    # -- Write steering files
    for listname in filelist:
        # - Define job command
        modstring = lambda x : x.replace(args.indir,'').replace('/','').replace('SKIM_','').replace(args.proc,'').replace('\n','')
        command = ( prog +
                    ' --indir '    + args.indir +
                    ' --outdir '   + args.outdir +
                    ' --sample '   + args.proc +
                    ' --file '     + modstring(listname) +
                    ' --channels ' + ' '.join(args.channels) +
                    ' --htcut '    + 'metnomu200cut\n'
                   )
            
        ####Write executable
        jobFile = os.path.join(jobsDir, 'job_'+args.proc+'_{}.sh'.format(n))
        with open(jobFile, 'w') as s:
            s.write('#!/bin/bash\n')
            s.write('export X509_USER_PROXY=~/.t3/proxy.cert\n')
            s.write('source /cvmfs/cms.cern.ch/cmsset_default.sh\n')
            s.write('cd /home/llr/cms/alves/CMSSW_11_1_0_pre6/src/\n')
            s.write('eval `scram r -sh`\n')
            s.write('cd {}\n'.format(currFolder))
            s.write(command)
            s.write('echo ${PYTHONPATH} \n')
            s.write('echo "Done for job {}" \n'.format(n))

        ####Write submission file
        submFile = os.path.join(jobsDir, 'job_'+args.proc+'_{}.submit'.format(n))
        queue = 'short'
        with open(submFile, 'w') as s:
            s.write('Universe = vanilla\n')
            s.write('Executable = {}\n'.format(jobFile))
            s.write('input = /dev/null\n')
            _outfile = '{}/Cluster$(Cluster)_Process$(Process)_$RANDOM_INTEGER(0,9999)'.format(jobsDir)
            s.write('output = {}.o\n'.format(_outfile))
            s.write('error  = {}.e\n'.format(_outfile))
            s.write('getenv = true\n')
            s.write('T3Queue = {}\n'.format(queue))
            s.write('WNTag=el7\n')
            s.write('+SingularityCmd = ""\n')
            s.write('include : /opt/exp_soft/cms/t3/t3queue |\n')
            s.write('queue')

        ### Submit job
        os.system('chmod u+rwx '+ jobFile)
        #os.system('/opt/exp_soft/cms/t3/t3submit -short ' + jobFile)
        os.system('condor_submit -name llrt3condor {}'.format(submFile))
        n = n + 1

        # - Save job command
        commandFile.write (command + '\n')

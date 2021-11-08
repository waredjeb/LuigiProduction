import os, sys
import argparse
import fileinput
import time
import glob
import subprocess
from os.path import basename
import ROOT

def isGoodFile(fileName):
    ff = ROOT.TFile(fname)
    if ff.IsZombie():
            return False
    if ff.TestBit(ROOT.TFile.kRecovered):
            return False
    return True

def parseInputFileList(fileName):
    filelist = []
    with open(fileName) as fIn:
        for line in fIn:
            line = (line.split("#")[0]).strip()
            if line:
                filelist.append(line)
    return filelist

if __name__ == "__main__":

    # -- Parse options
    parser = argparse.ArgumentParser(description='Command line parser')
    parser.add_argument('-a', '--indir',    dest='indir',    required=True, help='in directory')
    parser.add_argument('-o', '--outdir',   dest='outdir',   required=True, help='out directory')
    parser.add_argument('-p', '--proc',     dest='proc',     required=True, help='process name')
    parser.add_argument('-c', '--channels', dest='channels',
                        default=( 'all', 'etau', 'mutau', 'tautau', 'mumu' ),
                        help='channels considered')
    opt = parser.parse_args()

    # -- Check input folder
    if not os.path.exists(opt.indir):
        print('input folder', opt.indir, 'not existing, exiting')
        exit(1)
        
    # -- Input list
    inputfiles = os.path.join(opt.indir, 'SKIM_' + opt.proc + '/goodfiles.txt')

    # -- Job steering files
    currFolder = os.getcwd ()
    jobsDir = currFolder+'/jobs/'
    n = int(0)
    commandFile = open (jobsDir + '/submit.sh', 'w')

    # -- Parse input list
    filelist=[]
    with open(inputfiles) as fIn:
        for line in fIn:
            if '.root' in line:
                filelist.append(line)

    home = os.environ['HOME']
    prog = 'python ' + os.path.join(home, 'METTriggerStudies/scripts/', 'getTriggerEffSig.py')

    # -- Write steering files
    for listname in filelist:
        # - Define job command
        modstring = lambda x : x.replace(opt.indir,'').replace('/','').replace('SKIM_','').replace(opt.proc,'').replace('\n','')
        command = ( prog +
                    ' --indir '    + opt.indir +
                    ' --outdir '   + opt.outdir +
                    ' --sample '   + opt.proc +
                    ' --file '     + modstring(listname) +
                    ' --channels ' + ' '.join(opt.channels)
                   )

        print(command)
        quit()
        # - Setup
        jobFile = os.path.join(jobsDir, 'job_'+opt.proc+'_{}.sh'.format(n))

        with open(jobFile, 'w') as s:
            s.write('#!/bin/bash\n')
            s.write('export X509_USER_PROXY=~/.t3/proxy.cert\n')
            s.write('source /cvmfs/cms.cern.ch/cmsset_default.sh\n')
            s.write('cd /home/llr/cms/portales/hhbbtautau/CMSSW_11_1_0_pre6/src\n')
            s.write('eval `scram r -sh`\n')
            s.write('cd %s\n'%currFolder)
            s.write(command)
            s.write('echo "Done for job %d" \n'%n)

        # - Submit job (test)
        os.system('chmod u+rwx '+ jobFile)
        os.system('/opt/exp_soft/cms/t3/t3submit -short ' + jobFile)
        n = n + 1

        # - Save job command
        commandFile.write (command + '\n')

    commandFile.close()

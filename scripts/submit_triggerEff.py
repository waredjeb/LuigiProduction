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
    commandFile = open( os.path.join(jobsDir, 'submit.sh'), 'w')
    print(jobsDir)
    quit()

    # -- Parse input list
    filelist=[]
    with open(inputfiles) as fIn:
        for line in fIn:
            if '.root' in line:
                filelist.append(line)

    home = os.environ['HOME']
    prog = 'python3 ' + os.path.join(home, 'METTriggerStudies/scripts/', 'getTriggerEffSig.py')

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
                    ' --htcut '    + 'metnomu200cut'
                   )

        # - Setup
        jobFile = os.path.join(jobsDir, 'job_'+args.proc+'_{}.sh'.format(n))

        with open(jobFile, 'w') as s:
            s.write('#!/bin/bash\n')
            s.write('export X509_USER_PROXY=~/.t3/proxy.cert\n')
            s.write('source /cvmfs/cms.cern.ch/cmsset_default.sh\n')
            s.write('cd /home/llr/cms/portales/hhbbtautau/CMSSW_11_1_0_pre6/src\n')
            s.write('eval `scram r -sh`\n')
            s.write('cd {}\n'.format(currFolder))
            s.write(command)
            s.write('echo "Done for job {}" \n'.format(n))

        # - Submit job (test)
        os.system('chmod u+rwx '+ jobFile)
        os.system('/opt/exp_soft/cms/t3/t3submit -short ' + jobFile)
        n = n + 1

        # - Save job command
        commandFile.write (command + '\n')

    commandFile.close()

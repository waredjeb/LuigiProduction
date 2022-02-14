###### DOCSTRING ####################################################
# Submits all the jobs required to obtain the trigger scale factors
# Run example:
# python3 -m scripts.submitTriggerCounts
# --indir /data_CMS/cms/portales/HHresonant_SKIMS/SKIMS_Radion_2018_fixedMETtriggers_mht_16Jun2021/
# --outdir .
# --tag test_cuts
# --mc_processes TT_fullyHad
# --data MET2018A
# --triggers METNoMu120 METNoMu120_HT60 HT500
# --channels mutau
# --subtag SUBTAG
# --targetsPrefix hist_
####################################################################

import sys
sys.path.append("..")

import os
import argparse
import ROOT

from utils import utils
        
@utils.setPureInputNamespace
def submitTriggerCounts(args):
    home = os.environ['HOME']
    cmssw = os.path.join(os.environ['CMSSW_VERSION'], 'src')
    prog = 'python3 {}'.format( os.path.join(home, cmssw, 'METTriggerStudies', 'scripts', 'getTriggerCounts.py') )

    # -- Job steering files
    currFolder = os.getcwd()
    jobsDir = os.path.join(currFolder, 'jobs')

    _all_processes = args.data + args.mc_processes
    for thisProc in _all_processes:

        #### Check input folder
        inputfiles = [ os.path.join(idir, thisProc + '/goodfiles.txt') for idir in args.indir ]
        fexists = [ os.path.exists( inpf ) for inpf in inputfiles ]

        if sum(fexists) != 1: #check one and only one is True
            raise ValueError('The process {} could be found.'.format(thisProc))
        inputdir = args.indir[ fexists.index(True) ] #this is the only correct input directory

        inputfiles = os.path.join(inputdir, thisProc + '/goodfiles.txt')

        #### Parse input list
        filelist=[]
        with open(inputfiles) as fIn:
            for line in fIn:
                if '.root' in line:
                    filelist.append(line)

        #### Write shell executable (python scripts must be wrapped in shell files to run on HTCondor)
        outSubmDir = 'submission'
        jobDir = os.path.join(jobsDir, args.tag, outSubmDir)
        jobFile = os.path.join(jobDir, 'jobCounts_{}.sh'.format(thisProc))

        command =  ( ( '{prog} --indir {indir} --outdir {outdir} --sample {sample} --isData {isData} '
                       '--file ${{1}} --subtag {subtag} --channels {channels} '
                       '--triggers {triggers} --tprefix {tprefix} '
                       '\n' )
                     .format( prog=prog, indir=inputdir, outdir=args.outdir,
                              sample=thisProc, isData=int(thisProc in args.data),
                              subtag=args.subtag,
                              channels=' '.join(args.channels,),
                              triggers=' '.join(args.triggers,),
                              tprefix=args.tprefix,
                             )
                    )

        os.system('mkdir -p {}'.format(jobDir))

        with open(jobFile, 'w') as s:
            s.write('#!/bin/bash\n')
            s.write('export X509_USER_PROXY=~/.t3/proxy.cert\n')
            s.write('export EXTRA_CLING_ARGS=-O2\n')
            s.write('source /cvmfs/cms.cern.ch/cmsset_default.sh\n')
            s.write('cd /home/llr/cms/alves/CMSSW_12_2_0_pre1/src/\n')
            s.write('eval `scramv1 runtime -sh`\n')
            if args.debug:
                command += ' --debug'
            s.write(command)
            s.write('echo "Process {} done."\n'.format(thisProc))
        os.system('chmod u+rwx '+ jobFile)

        #### Write submission file
        submDir = os.path.join(jobsDir, args.tag, outSubmDir)
        submFile = os.path.join(submDir, 'jobCounts_' + thisProc + '.submit')

        outCheckDir = 'outputs'
        queue = 'short'
        os.system('mkdir -p {}'.format(submDir))

        with open(submFile, 'w') as s:
            s.write('Universe = vanilla\n')
            s.write('Executable = {}\n'.format(jobFile))
            s.write('Arguments = $(filename) \n'.format(jobFile))
            s.write('input = /dev/null\n')
            _outfile = ( '{d}/{t}/{o}/{p}/Cluster$(Cluster)_Process$(Process)_Counts'
                         .format(d=jobsDir, t=args.tag, o=outCheckDir, p=thisProc) )
            s.write('output = {}.o\n'.format(_outfile))
            s.write('error  = {}.e\n'.format(_outfile))
            s.write('getenv = true\n')
            s.write('T3Queue = {}\n'.format(queue))
            s.write('WNTag=el7\n')
            s.write('+SingularityCmd = ""\n')
            s.write('include : /opt/exp_soft/cms/t3/t3queue |\n\n')
            s.write('queue filename from (\n')
            for listname in filelist:
                s.write('  {}\n'.format( os.path.basename(listname).replace('\n','') ))

            s.write(')\n')
        os.system('mkdir -p {}'.format( os.path.join(jobsDir, args.tag, outCheckDir, thisProc) ))

        os.system('condor_submit -name llrt3condor {}'.format(submFile))

# -- Parse options
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Command line parser')

    parser.add_argument('-a', '--indir',      dest='indir',        required=True, help='in directory', nargs='+', type=str)
    parser.add_argument('-o', '--outdir',     dest='outdir',       required=True, help='out directory')
    parser.add_argument('-t', '--tag',        dest='tag',          required=True, help='tag')
    parser.add_argument('--subtag',           dest='subtag',       required=True, help='subtag')
    parser.add_argument('--tprefix',          dest='tprefix',      required=True, help='target prefix')
    parser.add_argument('--mc_processes',     dest='mc_processes', required=True, nargs='+', type=str,
                        help='list of MC process names')                
    parser.add_argument('--data',             dest='data',         required=True, nargs='+', type=str,
                        help='list of dataset s')                   
    parser.add_argument('-c', '--channels',   dest='channels',     required=True, nargs='+', type=str,
                        help='Select the channels over which the workflow will be run.' )
    parser.add_argument('--triggers',         dest='triggers',     required=True, nargs='+', type=str,
                        help='Select the triggers over which the workflow will be run.' )
    parser.add_argument('--debug', action='store_true', help='debug verbosity')
    args = parser.parse_args()

    submitTriggerCounts( args )

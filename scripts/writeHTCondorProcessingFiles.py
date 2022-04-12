###### DOCSTRING ####################################################
# Submits all the jobs required to obtain the trigger scale factors
# Run example:
# python3 -m scripts.submitTriggerEff
# --indir /data_CMS/cms/portales/HHresonant_SKIMS/SKIMS_Radion_2018_fixedMETtriggers_mht_16Jun2021/
# --outdir .
# --tag test_cuts
# --mc_processes TT_fullyHad
# --data MET2018A
# --triggers METNoMu120 METNoMu120_HT60 HT500
# --variables met_et HT20 mht_et metnomu_et mhtnomu_et
# --channels mutau
# --subtag SUBTAG
# --tprefix histos_
####################################################################

import sys
sys.path.append("..")

import os
import re
import argparse
import ROOT

from utils import utils

def runTrigger_outputs_sample(args, sample, param):
    """
    Produces all outputs of the submitTriggerEff task.
    Limitation: As soon as one file is not produced, luigi
    reruns everything.
    """
    assert(param in ('root', 'txt'))
    extension = '.' + param
    t = []
    exp = re.compile('.+output(_[0-9]{1,5}).root')

    inputs, _ = utils.get_root_input_files(sample, args.indir)

    folder = os.path.join( args.outdir, proc )
    for inp in inputs:
        number = exp.search(inp)
        proc_folder = os.path.dirname(inp).split('/')[-1]
        basename = args.tprefix + '_' + proc_folder + number.group(1)
        basename += args.subtag + extension
        t.append( os.path.join(folder, basename) )

    return t
    
@utils.setPureInputNamespace
def runTrigger_outputs(args, param='root'):
    """
    Produces all outputs of the submitTriggerEff task.
    Limitation: As soon as one file is not produced, luigi
    reruns everything.
    """
    t = []
    _all_processes = args.data + args.mc_processes
    for proc in _all_processes:
        t.extend( runTrigger_outputs_sample(args, proc, param) )
    return t

@utils.setPureInputNamespace
def writeHTCondorProcessingFiles_outputs(args):
    """
    Outputs are guaranteed to have the same length.
    Returns all separate paths to avoid code duplication.
    """
    out_jobs, out_submit, out_check = ([] for _ in range(3))
    base_dir = os.path.join(args.localdir, 'jobs', args.tag)
    
    jobDir = os.path.join(base_dir, 'submission')
    os.system('mkdir -p {}'.format(jobDir))

    checkDir = os.path.join(base_dir, 'outputs')
    os.system('mkdir -p {}'.format(checkDir))

    if args.mode == 'histos':
        name = 'jobHistos_{}.{}'
        check_name = 'Eff_C$(Cluster)P$(Process).o'
    elif args.mode == 'counts':
        name = 'jobCounts_{}.{}'
        check_name = 'Counts_C$(Cluster)P$(Process).o'
    else:
        raise ValueError('Mode {} is not supported.'.format(args.mode))

    _all_processes = args.data + args.mc_processes
    for thisProc in _all_processes:
        jobFile = os.path.join(jobDir, name.format(thisProc, 'sh'))
        out_jobs.append(jobFile)

        submFile = os.path.join(jobDir, name.format(thisProc, 'condor'))
        out_submit.append(submFile)

        checkDirProc = os.path.join(checkDir, thisProc)
        os.system('mkdir -p {}'.format(checkDirProc))

        checkFile = os.path.join(checkDirProc, check_name)
        out_check.append(checkFile)

    assert(len(out_jobs)==len(_all_processes))
    assert(len(out_submit)==len(_all_processes))
    assert(len(out_check)==len(_all_processes))
    return out_jobs, out_submit, out_check, _all_processes

@utils.setPureInputNamespace
def writeHTCondorProcessingFiles(args):
    prog = utils.build_prog_path(args.localdir, ('runTriggerEff.py' if args.mode == 'histos'
                                                 else 'runTriggerCounts.py') )

    outs_job, outs_submit, outs_check, _all_processes = writeHTCondorProcessingFiles_outputs(args)
    for i,thisProc in enumerate(_all_processes):
        filelist, inputdir = utils.get_root_input_files(thisProc, args.indir)
        
        #### Write shell executable (python scripts must be wrapped in shell files to run on HTCondor)
        command =  ( '{prog} --indir {indir} '.format( prog=prog, indir=inputdir) +
                     '--outdir {outdir} '.format(outdir=args.outdir) +
                     '--sample {sample} '.format(sample=thisProc) +
                     '--isdata {isdata} '.format(isdata=int(thisProc in args.data)) +
                     '--file ${1} ' +
                     '--subtag {subtag} '.format(subtag=args.subtag) +
                     '--channels {channels} '.format(channels=' '.join(args.channels,)) +
                     '--triggers {triggers} '.format(triggers=' '.join(args.triggers,)) +
                     '--tprefix {tprefix} '.format(tprefix=args.tprefix)
                    )
        
        if args.debug:
            command += '--debug '

        if args.mode == 'histos':
            command += ( '--binedges_fname {bename} '.format(bename=args.binedges_filename) +
                         '--intersection_str {inters} '.format(inters=args.intersection_str) +
                         '--variables {variables} '.format(variables=' '.join(args.variables,)) +
                         '--nocut_dummy_str {nocutstr}'.format(nocutstr=args.nocut_dummy_str)
                        ) 
        command += '\n'
            
        with open(outs_job[i], 'w') as s:
            s.write('#!/bin/bash\n')
            s.write('export X509_USER_PROXY=~/.t3/proxy.cert\n')
            s.write('export EXTRA_CLING_ARGS=-O2\n')
            s.write('source /cvmfs/cms.cern.ch/cmsset_default.sh\n')
            #s.write('cd /home/llr/cms/alves/CMSSW_12_2_0_pre1/src/\n')
            s.write('cd {}/\n'.format(args.localdir))
            s.write('eval `scramv1 runtime -sh`\n')
            s.write(command)
            s.write('echo "Process {} done in mode {}."\n'.format(thisProc,args.mode))
        os.system('chmod u+rwx '+ outs_job[i])

        #### Write submission file
        queue = 'short'
        with open(outs_submit[i], 'w') as s:
            s.write('Universe = vanilla\n')
            s.write('Executable = {}\n'.format(outs_job[i]))
            s.write('Arguments = $(filename) \n')
            s.write('input = /dev/null\n')
            s.write('output = {}\n'.format(outs_check[i]))
            s.write('error  = {}\n'.format(outs_check[i].replace('.o', '.e')))
            s.write('getenv = true\n')
            s.write('T3Queue = {}\n'.format(queue))
            s.write('WNTag=el7\n')
            s.write('+SingularityCmd = ""\n')
            s.write('include : /opt/exp_soft/cms/t3/t3queue |\n\n')
            s.write('queue filename from (\n')
            for listname in filelist:
                s.write('  {}\n'.format( listname.replace('\n','') ))
            s.write(')\n')

        # os.system('condor_submit -name llrt3condor {}'.format(submFile))

# -- Parse options
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Command line parser')

    parser.add_argument('--binedges_dataset', dest='binedges_dataset', required=True, help='in directory')
    parser.add_argument('--localdir', dest='localdir', default=os.getcwd(), help='out directory')
    parser.add_argument('--indir', dest='indir', required=True, help='in directory')
    parser.add_argument('--outdir', dest='outdir', required=True, help='out directory')
    parser.add_argument('--tag', dest='tag', required=True, help='tag')
    parser.add_argument('--subtag', dest='subtag', required=True, help='subtag')
    parser.add_argument('--tprefix', dest='tprefix', required=True, help='target prefix')
    parser.add_argument('--mc_processes', dest='mc_processes', required=True, nargs='+', type=str,
                        help='list of MC process names')                
    parser.add_argument('--data', dest='data', required=True, nargs='+', type=str,
                        help='list of dataset s')                       
    parser.add_argument('--channels',   dest='channels', required=True, nargs='+', type=str,
                        help='Select the channels over which the workflow will be run.' )
    parser.add_argument('--triggers', dest='triggers', required=True, nargs='+', type=str,
                        help='Select the triggers over which the workflow will be run.' )
    parser.add_argument('--variables', dest='variables', required=True, nargs='+', type=str,
                        help='Select the variables over which the workflow will be run.' )
    parser.add_argument('--intersection_str', dest='intersection_str', required=False, default='_PLUS_',
                        help='String used to represent set intersection between triggers.')
    parser.add_argument('--nocut_dummy_str', dest='nocut_dummy_str', required=True,
                        help='Dummy string associated to trigger histograms were no cuts are applied.')
    parser.add_argument('--debug', action='store_true', help='debug verbosity')
    args = parser.parse_args()

    submitTriggerEff( args )

import os
import sys
from utils import utils

def _subpaths(args):
    _tbase1 = args.tprefix + args.dataset_name
    _tbase2 = args.tsuffix + args.subtag
    return _tbase1, _tbase2

@utils.setPureInputNamespace
def runHadd_outputs(args):
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
def writeHTCondorHaddFiles_outputs(args):
    """
    Outputs are guaranteed to have the same length.
    Returns all separate paths to avoid code duplication.
    """
    out_jobs, out_submit, out_check = ([] for _ in range(3))

    outSubmDir = 'submission'
    jobDir = os.path.join(args.localdir, 'jobs', args.tag, outSubmDir)
    os.system('mkdir -p {}'.format(jobDir))
    outCheckDir = 'outputs'
    checkDir = os.path.join(args.localdir, 'jobs', args.tag, outCheckDir)
    os.system('mkdir -p {}'.format(checkDir))

    if args.mode == 'histos':
        name = 'jobHadd_{}.{}'
        check_name = 'Cluster$(Cluster)_Process$(Process)_Hadd.o'
    elif args.mode == 'counts':
        raise ValueError('Not supported.')

    _all_processes = args.data + args.mc_processes
    for thisProc in _all_processes:
        jobFile = os.path.join(jobDir, name.format(thisProc, 'sh'))
        out_jobs.append(jobFile)

        submFile = os.path.join(jobDir, name.format(thisProc, 'condor'))
        out_submit.append(submFile)

        checkFile = os.path.join(checkDir, thisProc, check_name)
        out_check.append(checkFile)

    assert(len(out_jobs)==len(_all_processes))
    assert(len(out_submit)==len(_all_processes))
    assert(len(out_check)==len(_all_processes))
    return out_jobs, out_submit, out_check, _all_processes

@utils.setPureInputNamespace
def writeHTCondorHaddFiles(args):
    """Adds ROOT histograms"""
    targets = haddTriggerEff_outputs(args)
    _tbase1, _tbase2 = _subpaths(args)

    #COPIAR PROCEDIMENTO EM /home/bruno/llr/scripts/writeHTCondorSubmissionFiles.py
    
    inputs_join = []
    _command = 'hadd -f'
    for t,smpl in zip(targets[1:], args.samples):
        inputs = os.path.join(args.indir, smpl + '/*' + args.subtag + '.root ')
        inputs_join.append(t)
        full_command = _command + ' ' + t + ' ' + inputs
        os.system( full_command )

    # join MC or Data subdatasets into a single one
    full_command_join = _command + ' ' + targets[0] + ' ' + ' '.join(inputs_join)
    os.system( full_command_join )

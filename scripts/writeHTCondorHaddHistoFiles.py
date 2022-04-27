import os
import sys
from utils import utils
from scripts.jobWriter import JobWriter

@utils.setPureInputNamespace
def runHaddHisto_outputs(args):
    targets = []

    # add the merge of all the samples first
    _tbase1, _tbase2 = utils.hadd_subpaths(args)
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
def writeHTCondorHaddHistoFiles_outputs(args):
    """
    Outputs are guaranteed to have the same length.
    Returns all separate paths to avoid code duplication.
    """
    base_dir = os.path.join(args.localdir, 'jobs', args.tag)
    
    jobDir = os.path.join(base_dir, 'submission')
    os.system('mkdir -p {}'.format(jobDir))

    dataset_folder = 'HaddHisto_' + args.dataset_name
    checkDir = os.path.join(base_dir, 'outputs', dataset_folder)
    os.system('mkdir -p {}'.format(checkDir))

    name = 'jobHaddHisto{}_{}.{}'
    check_name = '{}_{}HaddHisto_C$(Cluster)P$(Process).o'

    jobFiles   = [ os.path.join(jobDir, name.format('',    args.dataset_name, 'sh')),
                   os.path.join(jobDir, name.format('Agg', args.dataset_name, 'sh')) ]
    submFiles  = [ os.path.join(jobDir, name.format('',    args.dataset_name, 'condor')),
                   os.path.join(jobDir, name.format('Agg', args.dataset_name, 'condor')) ]
    checkFiles = [ os.path.join(checkDir, check_name.format(args.dataset_name, '')),
                   os.path.join(checkDir, check_name.format(args.dataset_name, 'Agg_')) ]

    return jobFiles, submFiles, checkFiles

@utils.setPureInputNamespace
def writeHTCondorHaddHistoFiles(args):
    """Adds ROOT histograms"""
    targets = runHaddHisto_outputs(args)
    outs_job, outs_submit, outs_check = writeHTCondorHaddHistoFiles_outputs(args)
    jw = JobWriter()
    
    #### Write shell executable (python scripts must be wrapped in shell files to run on HTCondor)
    command = 'hadd -f ${1} ${@:2}\n' #bash: ${@:POS} captures all arguments starting from POS

    for out in outs_job:
        jw.write_init(out, command, args.localdir)
        if out == outs_job[0]:
            jw.add_string('echo "HaddHisto {} done."'.format(args.dataset_name))
        elif out == outs_job[1]:
            jw.add_string('echo "HaddHisto Agg {} done."'.format(args.dataset_name))

    #### Write submission file
    inputs_join = []
    for out1,out2,out3 in zip(outs_job,outs_submit,outs_check):
        jw.write_init( filename=out2,
                       executable=out1,
                       outfile=out3,
                       queue='short' )

        qlines = []
        if out1 == outs_job[0]:
            for t,smpl in zip(targets[1:], args.samples):
                inputs = os.path.join(args.indir, smpl, args.tprefix + '*' + args.subtag + '.root')
                inputs_join.append(t)
                # join subdatasets (different MC or Data subfolders, ex: TT_fullyHad, TT_semiLep, ...)
                qlines.append('  {}, {}'.format(t, inputs))
        elif out1 == outs_job[1]:
            # join MC or Data subdatasets into a single one (ex: TT)
            qlines.append('  {}, {}'.format(targets[0], ' '.join(inputs_join)))
        
        jw.write_queue( qvars=('myoutput', 'myinputs'),
                        qlines=qlines )

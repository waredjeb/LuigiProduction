###### DOCSTRING ####################################################
# Submits all the jobs required to obtain the trigger scale factors
# Run example:
####################################################################

import sys
sys.path.append("..")

import os
import argparse
import ROOT

from utils import utils
from scripts.jobWriter import JobWriter

@utils.setPureInputNamespace
def writeHTCondorUnionWeightsCalculatorFiles_outputs(args):
    """
    Outputs are guaranteed to have the same length.
    Returns all separate paths to avoid code duplication.
    """
    out_jobs, out_submit, out_check = ([] for _ in range(3))
    base_dir = os.path.join(args.localdir, 'jobs', args.tag)
    
    jobdir = os.path.join(base_dir, 'submission')
    os.system('mkdir -p {}'.format(jobdir))

    checkdir = os.path.join(base_dir, 'outputs', 'UnionWeightsCalculator')
    os.system('mkdir -p {}'.format(checkdir))

    name = 'jobUnionWeightsCalculator_{}.{}'
    check_name = 'UnionWeightsCalculator_C$(Cluster)P$(Process).o'

    for proc in args.mc_processes:
        out_jobs.append( os.path.join(jobdir, name.format(proc, 'sh')) )
        out_submit.append( os.path.join(jobdir, name.format(proc, 'condor')) )

        check_dir_proc = os.path.join(checkdir, proc)
        os.system('mkdir -p {}'.format(check_dir_proc))
        out_check.append( os.path.join(check_dir_proc, check_name) )

    assert(len(out_jobs)==len(args.mc_processes))
    assert(len(out_submit)==len(args.mc_processes))
    assert(len(out_check)==len(args.mc_processes))
    return out_jobs, out_submit, out_check

@utils.setPureInputNamespace
def writeHTCondorUnionWeightsCalculatorFiles(args):
    prog = utils.build_prog_path(args.localdir, 'runUnionWeightsCalculator.py')
    jobs, subs, checks = writeHTCondorUnionWeightsCalculatorFiles_outputs(args)
    jw = JobWriter()

    for i,proc in enumerate(args.mc_processes):
        filelist, inputdir = utils.get_root_input_files(proc, args.indir_root)

        #### Write shell executable (python scripts must be wrapped in shell files to run on HTCondor)
        command =  ( '{prog} --indir_root {indir_root} '.format(prog=prog, indir_root=inputdir)
                     + '--indir_json {indir_json} '.format(indir_json=args.indir_json)
                     + '--indir_eff {indir_eff} '.format(indir_eff=args.indir_eff)
                     + '--outdir {outr} '.format(outr=args.outdir)
                     + '--inprefix {inprefix} '.format(inprefix=args.inprefix)
                     + '--outprefix {outprefix} '.format(outprefix=args.outprefix)
                     + '--sample {sample} '.format(sample=proc)
                     + '--channels {channels} '.format(channels=' '.join(args.channels,))
                     + '--triggers {triggers} '.format(triggers=' '.join(args.triggers,))
                     + '--file_name ${1} '
                     + '--closure_single_trigger ${2} '
                     + '--variables {variables} '.format(variables=' '.join(args.variables,))
                     + '--tag {tag} '.format(tag=args.tag)
                     + '--subtag {subtag} '.format(subtag=args.subtag)
                     + '--data_name {dataname} '.format(dataname=args.data_name)
                     + '--mc_name {mcname} '.format(mcname=args.mc_name)
                     + '--binedges_fname {be}'.format(be=args.binedges_filename)
                    )

        if args.debug:
            command += '--debug '

        jw.write_init(jobs[i], command, args.localdir)
        jw.add_string('echo "Process {} done."'.format(proc))

        #### Write submission file
        jw.write_init( filename=subs[i],
                       executable=jobs[i],
                       outfile=checks[i],
                       queue='short' )

        qlines = []
        for listname in filelist:
            for trig in args.closure_single_triggers:
                qlines.append('  {},{}'.format( os.path.basename(listname).replace('\n',''), trig ))
                        
        jw.write_queue( qvars=('filename', 'closure_single_trigger'),
                        qlines=qlines )

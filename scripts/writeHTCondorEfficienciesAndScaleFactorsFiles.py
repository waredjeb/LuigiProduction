
import sys
sys.path.append("..")

import os
import argparse

from utils.utils import (
    build_prog_path,
    generate_trigger_combinations,
    join_name_trigger_intersection as joinNTC,
    setPureInputNamespace,
)
from scripts.jobWriter import JobWriter

@setPureInputNamespace
def writeHTCondorEfficienciesAndScaleFactorsFiles_outputs(args):
    """
    Outputs are guaranteed to have the same length.
    Returns all separate paths to avoid code duplication.
    """
    base_dir = os.path.join(args.localdir, 'jobs', args.tag)
    
    jobDir = os.path.join(base_dir, 'submission')
    os.system('mkdir -p {}'.format(jobDir))
   
    checkDir = os.path.join(base_dir, 'outputs', 'EffAndScaleFactors')
    os.system('mkdir -p {}'.format(checkDir))
   
    name = 'jobEfficienciesAndSF.{}'
    check_name = 'EfficienciesAndSF_C$(Cluster)P$(Process).o'
   
    jobFiles   = os.path.join(jobDir, name.format('sh'))
    submFiles  = os.path.join(jobDir, name.format('condor'))
    checkFiles = os.path.join(checkDir, check_name)
   
    return jobFiles, submFiles, checkFiles

@setPureInputNamespace
def writeHTCondorEfficienciesAndScaleFactorsFiles(args):
    prog = build_prog_path(args.localdir, 'runEfficienciesAndScaleFactors.py')
    outs_job, outs_submit, outs_check = writeHTCondorEfficienciesAndScaleFactorsFiles_outputs(args)
    jw = JobWriter()

    #### Write shell executable (python scripts must be wrapped in shell files to run on HTCondor)
    command =  ( ( '{prog} --indir {indir} --outdir {outdir} '
                   '--mc_processes {mc_processes} '
                   '--mc_name {mc_name} --data_name {data_name} '
                   '--triggercomb ${{1}} '
                   '--channels {channels} --variables {variables} '
                   '--binedges_filename {binedges_filename} --subtag {subtag} '
                   '--tprefix {tprefix} '
                   '--canvas_prefix {cprefix} '
                  ).format( prog=prog, indir=args.indir, outdir=args.outdir,
                            mc_processes=' '.join(args.mc_processes,),
                            mc_name=args.mc_name, data_name=args.data_name,
                            channels=' '.join(args.channels,), variables=' '.join(args.variables,),
                            binedges_filename=args.binedges_filename,
                            subtag=args.subtag,
                            draw_independent_MCs=1 if args.draw_independent_MCs else 0,
                            tprefix=args.tprefix,
                            cprefix=args.canvas_prefix,
                           )
                )

    if args.draw_independent_MCs:
        command += '--draw_independent_MCs '
    if args.debug:
        command += '--debug '

    jw.write_init(outs_job, command, args.localdir)
    jw.add_string('echo "runEfficienciesAndScaleFactors done."')

    #### Write submission file
    jw.write_init( filename=outs_submit,
                   executable=outs_job,
                   outfile=outs_check,
                   queue='short' )

    qlines = []
    triggercomb = generate_trigger_combinations(args.triggers)
    for tcomb in triggercomb:
        qlines.append('  {}'.format(joinNTC(tcomb)))

    jw.write_queue( qvars=('triggercomb',),
                    qlines=qlines )

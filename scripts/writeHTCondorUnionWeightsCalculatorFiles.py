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

@utils.setPureInputNamespace
def writeHTCondorUnionWeightsCalculatorFiles_outputs(args):
    """
    Outputs are guaranteed to have the same length.
    Returns all separate paths to avoid code duplication.
    """
    out_jobs, out_submit, out_check = ([] for _ in range(3))
    base_dir = os.path.join(args.localdir, 'jobs', args.tag)
    
    jobDir = os.path.join(base_dir, 'submission')
    os.system('mkdir -p {}'.format(jobDir))

    checkDir = os.path.join(base_dir, 'outputs', 'UnionWeightsCalculator')
    os.system('mkdir -p {}'.format(checkDir))

    name = 'jobUnionWeightsCalculator_{}.{}'
    check_name = 'UnionWeightsCalculator_C$(Cluster)P$(Process).o'

    for proc in args.mc_processes:
        out_jobs.append( os.path.join(jobDir, name.format(proc, 'sh')) )
        out_submit.append( os.path.join(jobDir, name.format(proc, 'condor')) )
        out_check.append( os.path.join(checkDir, check_name) )

        checkDirProc = os.path.join(checkDir, proc)
        os.system('mkdir -p {}'.format(checkDirProc))
        out_check.append( os.path.join(checkDirProc, check_name) )

    assert(len(out_jobs)==len(args.mc_processes))
    assert(len(out_submit)==len(args.mc_processes))
    assert(len(out_check)==len(args.mc_processes))
    return out_jobs, out_submit, out_check

@utils.setPureInputNamespace
def writeHTCondorUnionWeightsCalculatorFiles(args):
    script = os.path.join(args.localdir, 'scripts')
    script = os.path.join(script, 'runUnionWeightsCalculator.py')
    prog = 'python3 {}'.format(script)

    jobs, subs, checks = writeHTCondorUnionWeightsCalculatorFiles_outputs(args)

    for i,proc in enumerate(args.mc_processes):
        filelist, inputdir = utils.get_root_input_files(proc, args)

        #### Write shell executable (python scripts must be wrapped in shell files to run on HTCondor)
        command =  ( '{prog} --indir_root {indir_root} '.format(prog=prog, indir=inputdir)
                     '--indir_json {indir_json} '.format(indir_json=args.indir_json)
                     '--indir_eff {indir_eff} '.format(indir_eff=args.indir_eff)
                     '--outdir_plots {outp} '.format(outp=args.outdir_plots)
                     '--outdir_root {outr} '.format(outr=args.outdir_root)
                     '--outprefix {outprefix} '.format(outprefix=args.outprefix)
                     '--sample {sample} '.format(sample=proc)
                     '--channels {channels} '.format(channels=' '.join(args.channels,))
                     '--triggers {triggers} '.format(triggers=' '.join(args.triggers,))
                     '--file ${{1}} '
                     '--variables {variables} '.format(variables=' '.join(args.variables,))
                     '--tag {tag} '.format(tag=args.tag)
                     '--subtag {subtag} '.format(subtag=args.subtag)
                     '--data_name {dataname} '.format(dataname=args.data_name)
                     '--mc_name {mcname} '.format(mcname=args.mc_name)
                     '--binedges_fname {be} '.format(be=args.binedges_filename)
                    )

        if args.debug:
            command += '--debug '
        command += '\n'

        # Technically one shell file would have been enough, but this solution is more flexible
        # for potential future changes, and is more readable when looking at the logs.
        with open(jobs[i], 'w') as s:
            s.write('#!/bin/bash\n')
            s.write('export X509_USER_PROXY=~/.t3/proxy.cert\n')
            s.write('export EXTRA_CLING_ARGS=-O2\n')
            s.write('source /cvmfs/cms.cern.ch/cmsset_default.sh\n')
            s.write('cd {}/\n'.format(args.localdir))
            s.write('eval `scramv1 runtime -sh`\n')
            s.write(command)
            s.write('echo "Process {} done."\n'.format(proc))
        os.system('chmod u+rwx '+ jobs[i])

        #### Write submission file
        queue = 'short'
        with open(subs[i], 'w') as s:
            s.write('Universe = vanilla\n')
            s.write('Executable = {}\n'.format(jobs[i]))
            s.write('input = /dev/null\n')
            s.write('output = {}\n'.format(checks[i]))
            s.write('error  = {}\n'.format(checks[i].replace('.o', '.e')))
            s.write('getenv = true\n')
            s.write('T3Queue = {}\n'.format(queue))
            s.write('WNTag=el7\n')
            s.write('+SingularityCmd = ""\n')
            s.write('include : /opt/exp_soft/cms/t3/t3queue |\n\n')
            s.write('queue filename from (\n')
            for listname in filelist:
                s.write('  {}\n'.format( os.path.basename(listname).replace('\n','') ))
            s.write(')\n')

    # os.system('condor_submit -name llrt3condor {}'.format(submFile))

# # -- Parse options
# if __name__ == '__main__':
#     parser = argparse.ArgumentParser(description='Command line parser')

#     parser.add_argument('--binedges_filename', dest='binedges_filename', required=True, help='where the bin edges are stored')
#     parser.add_argument('--localdir',         dest='localdir',         default=os.getcwd(),
#                         help='out directory')
#     parser.add_argument('--indir', dest='indir', required=True, help='in directory')
#     parser.add_argument('--indir_json', help='Input directory where discriminator JSON files are stored',
#                         required=True)
#     parser.add_argument('--indir_eff', help='Input directory where intersection efficiencies are stored',
#                         required=True)
#     parser.add_argument('--outdir_plots', dest='outdir_plots', required=True, help='out directory')
#     parser.add_argument('--outdir_root', dest='outdir_root', required=True, help='out directory')
#     parser.add_argument('--outprefix', dest='outprefix', required=True, help='Out histos prefix.')
#     parser.add_argument('--tag',        dest='tag',              required=True, help='tag')
#     parser.add_argument('--subtag',           dest='subtag',           required=True, help='subtag')
#     parser.add_argument('--mc_processes', dest='mc_processes', required=True, nargs='+', type=str,
#                         help='list of MC process names')                
#     parser.add_argument('--data_name', dest='data_name', required=True, help='Data sample name')
#     parser.add_argument('--mc_name', dest='mc_name', required=True, help='MC sample name')
#     parser.add_argument('--channels',   dest='channels',         required=True, nargs='+', type=str,
#                         help='Select the channels over which the workflow will be run.' )
#     parser.add_argument('--triggers',         dest='triggers',         required=True, nargs='+', type=str,
#                         help='Select the triggers over which the workflow will be run.' )
#     parser.add_argument('--variables',        dest='variables',        required=True, nargs='+', type=str,
#                         help='Select the variables over which the workflow will be run.' )
#     parser.add_argument('--debug', action='store_true', help='debug verbosity')
#     args = parser.parse_args()

#     submitTriggerEff( args )

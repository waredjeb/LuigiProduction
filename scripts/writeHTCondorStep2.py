import os
import sys
from utils import utils
from scripts.jobWriter import JobWriter

@utils.setPureInputNamespace
def writeHTCondorStep2_outputs(args):
    """
    Outputs are guaranteed to have the same length.
    Returns all separate paths to avoid code duplication.
    """
    base_dir = os.path.join(args.localdir, 'jobs')
    
    jobDir = os.path.join(base_dir, 'submission')
    os.system('mkdir -p {}'.format(jobDir))
    
    if(isinstance(args.sample_name, list)):
        sample_name = args.sample_name[0]
    else:
        sample_name = args.sample_name
    # print(jobDir)
    dataset_folder = 'Step2_' + sample_name
    checkDir = os.path.join(base_dir, 'outputs', dataset_folder)
    os.system('mkdir -p {}'.format(checkDir))

    name = 'jobStep2{}_{}.{}'
    check_name = '{}_{}Step2_C$(Cluster)P$(Process).o'

    jobFiles   = os.path.join(jobDir, name.format('',    sample_name, 'sh'))
    submFiles  = os.path.join(jobDir, name.format('',    sample_name, 'condor'))
    checkFiles = os.path.join(checkDir, check_name.format(sample_name, '')) 

    return jobFiles, submFiles, checkFiles

@utils.setPureInputNamespace
def writeHTCondorStep2(args):
    """Adds TXT count files"""
    outs_job, outs_submit, outs_check = writeHTCondorStep2_outputs(args)
    jw = JobWriter()
    if(isinstance(args.sample_file, list)):
        command = ('echo step2!')
    else:
        command = ('echo step2!')
    print(outs_job, command)
    jw.write_init(outs_job,  command, args.localdir, cmsswdir = "/afs/cern.ch/work/w/wredjeb/public/CondorCMSSW/ProductionLUIGI/data/CMSSW_12_4_0_pre2")

    # for out1,out2,out3 in zip(outs_job,outs_submit,outs_check):
    #     print(out1,out2,out3)
    jw.write_init( filename=outs_submit,
                    executable=outs_job,
                    transfer_files = "/afs/cern.ch/work/w/wredjeb/public/CondorCMSSW/ProductionLUIGI/data/CMSSW_12_4_0_pre2.tgz",
                    outfile=outs_check,
                    queue='short', )
    qvars = None
    qlines = []
        
    jw.write_queue( qvars=(), qlines=qlines )




















































































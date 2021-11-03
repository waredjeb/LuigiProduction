import os
import luigi
import law
import utils
from utils import utils

from luigi_conf.luigi_utils import ForceableEnsureRecentTarget, ForceParameter
from luigi_conf.luigi_utils import WorkflowDebugger
from luigi_conf.luigi_utils import is_force_mistake

from scripts.haddTriggerEff import haddTriggerEff, haddTriggerEff_outputs
from scripts.drawTriggerSF import drawTriggerSF, drawTriggerSF_outputs
from scripts.getTriggerEffSig import getTriggerEffSig

from luigi_conf.luigi_cfg import cfg, FLAGS
lcfg = cfg() #luigi configuration

import re
re_txt = re.compile('\.txt')

#/grid_mnt/vol_home/llr/cms/portales/hhbbtautau/KLUB_UL/CMSSW_11_1_0_pre6/src/KLUBAnalysis/studies/METtriggerStudies/draw_trigger_sf.C 
########################################################################
### LAW HTCONDOR WORKFLOW ##############################################
########################################################################

# the htcondor workflow implementation is part of a law contrib package
# so we need to explicitly load it
law.contrib.load("htcondor")


class LawBaseTask(law.Task):
    """
    Base task that we use to force a version parameter on all inheriting tasks, and that provides
    some convenience methods to create local file and directory targets at the default data path.
    """

    version = luigi.Parameter()

    def store_parts(self):
        return (self.__class__.__name__, self.version)

    def local_path(self, *path):
        return "LAWBASETASK_LOCAL_PATH.txt"

    def local_target(self, *path):
        return law.LocalFileTarget(self.local_path(*path))


class HTCondorWorkflow(law.htcondor.HTCondorWorkflow):
    """
    Batch systems are typically very heterogeneous by design, and so is HTCondor. Law does not aim
    to "magically" adapt to all possible HTCondor setups which would certainly end in a mess.
    Therefore we have to configure the base HTCondor workflow in law.contrib.htcondor to work with
    the CERN HTCondor environment. In most cases, like in this example, only a minimal amount of
    configuration is required.
    """

    max_runtime = law.DurationParameter(default=2.0, unit="h", significant=False,
        description="maximum runtime, default unit is hours, default: 2")

    def htcondor_output_directory(self):
        # the directory where submission meta data should be stored
        return law.LocalDirectoryTarget(self.local_path())

    def htcondor_bootstrap_file(self):
        # each job can define a bootstrap file that is executed prior to the actual job
        # in order to setup software and environment variables
        return law.util.rel_path(__file__, "bootstrap.sh")

    def htcondor_job_config(self, config, job_num, branches):
        # render_variables are rendered into all files sent with a job
        config.render_variables["x509_user_proxy"] = os.getenv("X509_USER_PROXY")
        # force to run on CC7, http://batchdocs.web.cern.ch/batchdocs/local/submit.html#os-choice
        config.custom_content.append(("requirements", "(OpSysAndVer =?= \"CentOS7\")"))
        # maximum runtime
        config.custom_content.append(("+MaxRuntime", int(math.floor(self.max_runtime * 3600)) - 1))
        # copy the entire environment
        config.custom_content.append(("getenv", "true"))

        config.custom_content.append(('source', '/cvmfs/cms.cern.ch/cmsset_default.sh'))
        # the CERN htcondor setup requires a "log" config, but we can safely set it to /dev/null
        # if you are interested in the logs of the batch system itself, set a meaningful value here
        config.custom_content.append(("log", "/dev/null"))
        return config

########################################################################
### HELPER FUNCTIONS ###################################################
########################################################################
def addstr(*args, connector='_'):
    s = ''
    for arg in args:
        if arg==args[0]:
            s = arg
        else:
            s += connector + arg
    return s

def get_target_path(taskname):
    target_path = os.path.join(lcfg.targets_folder,
                               re_txt.sub( '_'+taskname+'.txt',
                                           lcfg.targets_default_name ) ) 
    return luigi.Parameter( default=target_path )


########################################################################
### JOB SUBMISSION #####################################################
#########################################################################
#INDIR="/data_CMS/cms/portales/HHresonant_SKIMS/SKIMS_Radion_2018_fixedMETtriggers_mht_16Jun2021/"
#OUTDIR="/data_CMS/cms/alves/FRAMEWORKTEST/"
#PROC="MET2018A MET2018B MET2018C MET2018D"
class SubmitTriggerEff(LawBaseTask, HTCondorWorkflow, law.LocalWorkflow
                       #, ForceableEnsureRecentTarget
                       ):
    samples = luigi.ListParameter(significant=True)
    args = utils.dotDict(lcfg.submit_params)
    args.update( {'targetsPrefix': lcfg.targets_prefix,
                  'tag': lcfg.tag,
                  } )

    target_path = get_target_path( args.taskname )

    def create_branch_map(self):
        my_branch_map = dict()

        count = 0
        for smpl in self.samples:
            inputfiles = os.path.join( self.args.indir, 'SKIM_'+smpl, 'goodfiles.txt' )
            with open(inputfiles) as fIn:
                for line in fIn:
                    if '.root' in line:
                        my_branch_map[count] = (smpl, line)
                        count += 1
                        
        return my_branch_map
    
    @WorkflowDebugger(flag=FLAGS.debug_workflow)
    def output(self):
        #CHANGE to actually reflect the output ROOT files in get_trigger_eff_sig.py
        return self.local_target("SubmitTriggerEff_output_{}.json".format(self.branch))
    
    @WorkflowDebugger(flag=FLAGS.debug_workflow)
    def run(self):
        branch_sample, branch_file = self.branch_data
        getTriggerEffSig(indir=self.args.indir, outdir=self.args.outdir,
                         sample=branch_sample, fileName=branch_file,
                         channels=self.args.channels)

        # once self.output() above is CHANGED, the following lines will not be needed
        output = self.output()
        output.dump({"sample": branch_sample, "file": branch_file})

########################################################################
### HADD TRIGGER EFFICIENCIES ##########################################
########################################################################
class HaddTriggerEff(ForceableEnsureRecentTarget):
    samples = luigi.ListParameter(significant=True)
    
    args = utils.dotDict(lcfg.hadd_params)
    args.update( {'targetsPrefix': lcfg.targets_prefix,
                  'tag': lcfg.tag,
                  'samples': samples,
                  } )
    
    target_path = get_target_path( args.taskname )
    
    @WorkflowDebugger(flag=FLAGS.debug_workflow)
    def output(self):
        targets = []
        targets_list = haddTriggerEff_outputs( self.args )

        #define luigi targets
        for t in targets_list:
            targets.append( luigi.LocalTarget(t) )

        #write the target files for debugging
        utils.remove( self.target_path )
        with open( self.target_path, 'w' ) as f:
            for t in targets_list:
                f.write( t )

        return targets

    @WorkflowDebugger(flag=FLAGS.debug_workflow)
    def run(self):
        print("INPUTS: ")
        inputs = self.input()["collection"].targets
        print(inputs)
        
        haddTriggerEff( self.args )
        
    @WorkflowDebugger(flag=FLAGS.debug_workflow)
    def requires(self):
        force_flag = FLAGS.force > self.args.hierarchy
        #return SubmitTriggerEff(force=force_flag)
        return SubmitTriggerEff(version='1', samples=self.samples).req(self)

########################################################################
### COMPARE TRIGGERS ###################################################
########################################################################
# class CompareTriggers(ForceableEnsureRecentTarget):
#     p = utils.dotDict(lcfg.comp_params)
    
#     #drawsf_args
#     args = luigi.DictParameter(
#         default={'inDir': p.indir,
#                  'targetsPrefix': lcfg.targets_prefix,
#                  'tag': lcfg.tag,
#                  'processes': p.processes,
#                  } )
#     target_path = get_target_path( p.taskname )
    
#     @WorkflowDebugger(flag=FLAGS.debug_workflow)
#     def output(self):
#         targets = []
#         targets_list = drawTriggerSF_outputs( self.args )

#         #define luigi targets
#         for t in targets_list:
#             targets.append( luigi.LocalTarget(t) )

#         #write the target files for debugging
#         utils.remove( self.target_path )
#         with open( self.target_path, 'w' ) as f:
#             for t in targets_list:
#                 f.write( t )
                
#         return targets

#     @WorkflowDebugger(flag=FLAGS.debug_workflow)
#     def run(self):
#         drawTriggerSF( self.args )
            
#     @WorkflowDebugger(flag=FLAGS.debug_workflow)
#     def requires(self):
#         force_flag = FLAGS.force > p.hierarchy
#         return HaddTriggerEff(force=force_flag)

########################################################################
### DRAW TRIGGER SCALE FACTORS #########################################
########################################################################
class DrawTriggerScaleFactors(ForceableEnsureRecentTarget):
    args = utils.dotDict(lcfg.drawsf_params)
    args.update( {'targetsPrefix': lcfg.targets_prefix,
                  'tag': lcfg.tag,
                  } )
    
    target_path = get_target_path( args.taskname )
    
    @WorkflowDebugger(flag=FLAGS.debug_workflow)
    def output(self):
        targets = []
        targets_list = drawTriggerSF_outputs( self.args )

        #define luigi targets
        for t in targets_list:
            targets.append( luigi.LocalTarget(t) )

        #write the target files for debugging
        utils.remove( self.target_path )
        with open( self.target_path, 'w' ) as f:
            for t in targets_list:
                f.write( t )
                
        return targets

    @WorkflowDebugger(flag=FLAGS.debug_workflow)
    def run(self):
        drawTriggerSF( self.args )
            
    @WorkflowDebugger(flag=FLAGS.debug_workflow)
    def requires(self):
        force_flag = FLAGS.force > self.args.hierarchy
        return [ HaddTriggerEff(force=force_flag, samples=self.args.data),
                 #HaddTriggerEff(force=force_flag, samples=args.mc_processes) ]
                 SubmitTriggerEff(version='1', samples=self.args.mc_processes) ]


########################################################################
### MAIN ###############################################################
########################################################################
if __name__ == "__main__":
    """
    1. Safety check to avoid catastrophic data deletions
    2. Run the workflow
    """
    if is_force_mistake(FLAGS.force):
        print('Workflow interrupted.')
        exit(0)

    utils.create_single_dir( lcfg.tag_folder )
    utils.create_single_dir( lcfg.targets_folder )

    # for t in _tasks_tag:
    #     if t == 'preprocessing':
    #         for cat in cfg().pp_categories:
    #             fname = regex_txt.sub('_'+cat+'.txt', os.path.join(t_tag, cfg().targets_tag[t]))
    #             write_dummy_file(fname)
    #     else:
    #         fname = os.path.join(t_tag, cfg().targets_tag[t])
    #         write_dummy_file(fname)
    
    #8 categories => at most 8 workers required
    last_task = DrawTriggerScaleFactors(force=FLAGS.force>0)
    if FLAGS.scheduler == 'central':
        luigi.build([last_task], workers=FLAGS.workers, local_scheduler=False, log_level='INFO')
    if FLAGS.scheduler == 'local':
        luigi.build([last_task], local_scheduler=True, log_level='INFO')

else:
    raise RuntimeError('This script can only be run directly from the command line.')

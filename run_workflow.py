import os
import luigi
import utils
from utils import utils

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

def luigi_to_raw( param ):
    """
    Converts luigi parameters to their "raw" versions.
    Useful when passing them to functions.
    """
    if isinstance(param, (tuple,list)):
        return [x for x in param]
    else:
        raise NotImplementedError('[' + inspect.stack()[0][3] + ']: ' + 'only tuples/lsits implemented so far!')
        
########################################################################
### JOB SUBMISSION #####################################################
#########################################################################
#INDIR="/data_CMS/cms/portales/HHresonant_SKIMS/SKIMS_Radion_2018_fixedMETtriggers_mht_16Jun2021/"
#OUTDIR="/data_CMS/cms/alves/FRAMEWORKTEST/"
#PROC="MET2018A MET2018B MET2018C MET2018D"

# class SubmitTriggerEff(LawBaseTask, HTCondorWorkflow, law.LocalWorkflow):
#     samples = luigi.ListParameter(significant=True)
#     args = utils.dotDict(lcfg.submit_params)
#     #args = lcfg.submit_params
#     args.update( {'targetsPrefix': lcfg.targets_prefix,
#                   'tag': lcfg.tag,
#                   } )

#     target_path = get_target_path( args['taskname'] )

#     def create_branch_map(self):
#         my_branch_map = dict()

#         count = 0
#         for smpl in self.samples:
#             inputfiles = os.path.join( self.args['indir'], 'SKIM_'+smpl, 'goodfiles.txt' )
#             with open(inputfiles) as fIn:
#                 for line in fIn:
#                     if '.root' in line:
#                         my_branch_map[count] = (smpl, line)
#                         count += 1
                        
#         return my_branch_map
    
#     #@WorkflowDebugger(flag=FLAGS.debug_workflow)
#     def output(self):
#         #CHANGE to actually reflect the output ROOT files in get_trigger_eff_sig.py
#         return self.local_target("SubmitTriggerEff_output_{}.json".format(self.branch))
    
#     #@WorkflowDebugger(flag=FLAGS.debug_workflow)
#     def run(self):
#         branch_sample, branch_file = self.branch_data
#         getTriggerEffSig(indir=self.args['indir'], outdir=self.args['outdir'],
#                          sample=branch_sample, fileName=branch_file,
#                          channels=self.args.channels)

#         # once self.output() above is CHANGED, the following lines will not be needed
#         output = self.output()
#         output.dump({"sample": branch_sample, "file": branch_file})

########################################################################
### HADD TRIGGER EFFICIENCIES ##########################################
########################################################################
class HaddTriggerEff(luigi.Task):
    samples = luigi.ListParameter(significant=True)
    args = utils.dotDict(lcfg.hadd_params)
    args.update( {'targetsPrefix': lcfg.targets_prefix,
                  'tag': lcfg.tag,
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
        #print("INPUTS: ")
        inputs = self.input()["collection"].targets
        #print(inputs)

        self.args['samples'] = luigi_to_raw( self.samples )

        haddTriggerEff( self.args )
        
    # @WorkflowDebugger(flag=FLAGS.debug_workflow)
    # def requires(self):
    #     force_flag = FLAGS.force > self.args.hierarchy
    #     #return SubmitTriggerEff(force=force_flag)
    #     return SubmitTriggerEff(version='1', samples=self.samples)#.req(self)

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
class DrawTriggerScaleFactors(luigi.Task):
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
        #force_flag = FLAGS.force > self.args.hierarchy
        return [ HaddTriggerEff(samples=self.args.data),
                 #HaddTriggerEff(force=force_flag, samples=self.args.data),
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
    #last_task = DrawTriggerScaleFactors(force=FLAGS.force>0)
    last_task = DrawTriggerScaleFactors()
    if FLAGS.scheduler == 'central':
        luigi.build([last_task], workers=FLAGS.workers, local_scheduler=False, log_level='INFO')
    if FLAGS.scheduler == 'local':
        luigi.build([last_task], local_scheduler=True, log_level='INFO')

else:
    raise RuntimeError('This script can only be run directly from the command line.')

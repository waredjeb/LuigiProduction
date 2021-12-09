import os
import time
import luigi
import utils
import inspect
from utils import utils

from luigi_conf.luigi_utils import WorkflowDebugger
from luigi_conf.luigi_utils import is_force_mistake, ForceableEnsureRecentTarget

from scripts.submitTriggerEff import submitTriggerEff, submitTriggerEff_outputs
from scripts.haddTriggerEff import haddTriggerEff, haddTriggerEff_outputs
from scripts.drawTriggerSF import drawTriggerSF, drawTriggerSF_outputs
from scripts.draw2DTriggerSF import draw2DTriggerSF, draw2DTriggerSF_outputs

from luigi_conf.luigi_cfg import cfg, FLAGS

lcfg = cfg() #luigi configuration

import re
re_txt = re.compile('\.txt')

#/grid_mnt/vol_home/llr/cms/portales/hhbbtautau/KLUB_UL/CMSSW_11_1_0_pre6/src/KLUBAnalysis/studies/METtriggerStudies/draw_trigger_sf.C 
########################################################################
### HELPER FUNCTIONS ###################################################
########################################################################
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
### SUBMIT TRIGGER EFFICIENCIES USING HTCONDOR #########################
########################################################################
class SubmitTriggerEff(ForceableEnsureRecentTarget):
    args = utils.dotDict(lcfg.submit_params)
    args.update( {'targetsPrefix': lcfg.targets_prefix,
                  'tag': lcfg.tag,
                  } )
    
    target_path = get_target_path( args.taskname )
    
    @WorkflowDebugger(flag=FLAGS.debug_workflow)
    def output(self):
        targets = []
        targets_list = submitTriggerEff_outputs( self.args )

        #define luigi targets
        for t in targets_list:
            targets.append( luigi.LocalTarget(t) )

        #write the target files for debugging
        utils.remove( self.target_path )
        with open( self.target_path, 'w' ) as f:
            for t in targets_list:
                f.write( t + '\n' )

        return targets

    @WorkflowDebugger(flag=FLAGS.debug_workflow)
    def run(self):
        submitTriggerEff( self.args )

        time.sleep(1.0)
        os.system('condor_q')
        

########################################################################
### HADD TRIGGER EFFICIENCIES ##########################################
########################################################################
class HaddTriggerEff(ForceableEnsureRecentTarget):
    samples = luigi.ListParameter()
    target_suffix = luigi.Parameter()
    dataset_name = luigi.Parameter()
    args = utils.dotDict(lcfg.hadd_params)
    args.update( {'targetsPrefix': lcfg.targets_prefix,
                  'tag': lcfg.tag,
                  } )
    
    target_path = get_target_path( args.taskname )
    
    @WorkflowDebugger(flag=FLAGS.debug_workflow)
    def output(self):
        self.args['samples'] = luigi_to_raw( self.samples )
        self.args['target_suffix'] = self.target_suffix
        self.args['dataset_name'] = self.dataset_name
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
        self.args['samples'] = luigi_to_raw( self.samples )
        self.args['target_suffix'] = self.target_suffix
        self.args['dataset_name'] = self.dataset_name
        haddTriggerEff( self.args )
        
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
### DRAW 1D TRIGGER SCALE FACTORS #######################################
########################################################################
class Draw1DTriggerScaleFactors(ForceableEnsureRecentTarget):
    args = utils.dotDict(lcfg.drawsf_params)
    args.update( {'targetsPrefix': lcfg.targets_prefix,
                  'tag': lcfg.tag,
                  } )
    target_path = get_target_path( args.taskname )
    
    @WorkflowDebugger(flag=FLAGS.debug_workflow)
    def output(self):
        targets = []
        targets_list, _ = drawTriggerSF_outputs( self.args )

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
        return [ HaddTriggerEff(force=force_flag, samples=self.args.data,
                                target_suffix=self.args.target_suffix,
                                dataset_name=self.args.data_name),
                 HaddTriggerEff(force=force_flag, samples=self.args.mc_processes,
                                target_suffix=self.args.target_suffix,
                                dataset_name=self.args.mc_name) ]

########################################################################
### DRAW 2D TRIGGER SCALE FACTORS #######################################
########################################################################
class Draw2DTriggerScaleFactors(ForceableEnsureRecentTarget):
    args = utils.dotDict(lcfg.drawsf_params)
    args.update( {'targetsPrefix': lcfg.targets_prefix,
                  'tag': lcfg.tag,
                  } )
    target_path = get_target_path( args.taskname )
    
    @WorkflowDebugger(flag=FLAGS.debug_workflow)
    def output(self):
        targets = []
        targets_list, _ = draw2DTriggerSF_outputs( self.args )

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
        draw2DTriggerSF( self.args )

    @WorkflowDebugger(flag=FLAGS.debug_workflow)
    def requires(self):
        force_flag = FLAGS.force > self.args.hierarchy
        return [ HaddTriggerEff(force=force_flag, samples=self.args.data,
                                target_suffix=self.args.target_suffix,
                                dataset_name=self.args.data_name),
                 HaddTriggerEff(force=force_flag, samples=self.args.mc_processes,
                                target_suffix=self.args.target_suffix,
                                dataset_name=self.args.mc_name) ]

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
    if FLAGS.submit:
        last_task = SubmitTriggerEff(force=FLAGS.force>0)
    else:
        last_tasks = [ Draw1DTriggerScaleFactors(force=FLAGS.force>0),
                       Draw2DTriggerScaleFactors(force=FLAGS.force>0) ]
    if FLAGS.scheduler == 'central':
        luigi.build([last_task] if FLAGS.submit else last_tasks,
                    workers=FLAGS.workers, local_scheduler=False, log_level='INFO')
    if FLAGS.scheduler == 'local':
        luigi.build([last_task] if FLAGS.submit else last_tasks,
                    local_scheduler=True, log_level='INFO')

else:
    raise RuntimeError('This script can only be run directly from the command line.')

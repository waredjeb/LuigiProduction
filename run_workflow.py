import os
import luigi
import utils

from luigi_utils import ForceableEnsureRecentTarget, ForceParameter
from luigi_utils import WorkflowDebugger
from luigi_utils import is_force_mistake

from scripts.haddTriggerEff import haddTriggerEff, haddTriggerEff_outputs
from scripts.drawTriggerSF import drawTriggerSF, drawTriggerSF_outputs

from luigi_cfg import cfg, FLAGS
lcfg = cfg() #luigi configuration

#make each independent script/step target-agnostic (all should be taken care of in this file)
#this will require some additional work in case the targets are only known at runtime, but that will likely not happen

import re
re_txt = re.compile('\.txt')

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
### HADD TRIGGER EFFICIENCIES ##########################################
########################################################################
class HaddTriggerEff(ForceableEnsureRecentTarget):
    hadd_args = luigi.DictParameter( default={'inDir': lcfg.hadd_indir,
                                              'targetsPrefix': lcfg.targets_prefix,
                                              'tag': lcfg.tag,
                                              'processes': lcfg.hadd_processes,
                                              } )
    
    target_path = get_target_path( lcfg.hadd_taskname )
    
    @WorkflowDebugger(flag=FLAGS.debug_workflow)
    def output(self):
        targets = []
        print( self.hadd_args )
        targets_list = haddTriggerEff_outputs( self.hadd_args )

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
        haddTriggerEff( self.hadd_args )
        
    # @WorkflowDebugger(flag=FLAGS.debug_workflow)
    # def requires(self):
    #     force_flag = FLAGS.force > cfg().hadd_hierarchy
    #     return None

########################################################################
### DRAW TRIGGER SCALE FACTORS #########################################
########################################################################
class DrawTriggerScaleFactors(ForceableEnsureRecentTarget):
    tn = lcfg.drawsf_taskname
    
    #drawsf_args
    params = dict()
    params[addstr(tn, 'args')] = luigi.DictParameter(
        default={'inDir': getattr(lcfg, addstr(tn, 'indir')),
                 'targetsPrefix': lcfg.targets_prefix,
                 'tag': lcfg.tag,
                 'processes': getattr(lcfg, addstr(tn, 'processes')),
                 } )
    target_path = get_target_path( tn )
    
    @WorkflowDebugger(flag=FLAGS.debug_workflow)
    def output(self):
        targets = []
        print(type(self.params[addstr(self.tn, 'args')]))
        targets_list = drawTriggerSF_outputs( self.params[addstr(self.tn, 'args')] )

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
        print(type(self.params[addstr(self.tn, 'args')]))
        drawTriggerSF( self.params[addstr(self.tn, 'args')] )
            
    @WorkflowDebugger(flag=FLAGS.debug_workflow)
    def requires(self):
        force_flag = FLAGS.force > getattr(lcfg, addstr(self.tn, 'hierarchy'))
        return HaddTriggerEff(force=force_flag)


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

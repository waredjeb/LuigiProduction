import os
import time
import luigi
import utils
import inspect
from utils import utils

from luigi_conf.luigi_utils import WorkflowDebugger
from luigi_conf.luigi_utils import (
    ForceableEnsureRecentTarget,
    is_force_mistake,
    )
from luigi_conf.luigi_cfg import cfg, FLAGS

from luigi_conf import (
    _placeholder_cuts,
    )
lcfg = cfg() #luigi configuration

# includes of individual tasks
from scripts.defineBinning import defineBinning, defineBinning_outputs
from scripts.submitTriggerEff import submitTriggerEff, submitTrigger_outputs
from scripts.submitTriggerCounts import submitTriggerCounts
from scripts.haddTriggerEff import haddTriggerEff, haddTriggerEff_outputs
from scripts.addTriggerCounts import addTriggerCounts, addTriggerCounts_outputs
from scripts.drawTriggerSF import drawTriggerSF, drawTriggerSF_outputs
from scripts.draw2DTriggerSF import draw2DTriggerSF, draw2DTriggerSF_outputs
from scripts.drawDistributions import drawDistributions, drawDistributions_outputs

import re
re_txt = re.compile('\.txt')

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
        raise NotImplementedError('[' + inspect.stack()[0][3] + ']: ' + 'only tuples/lists implemented so far!')

def set_force_boolean(hierarchy):
    """
    Decide whether a particular task should be force-run.
    Works if the task's targets already exist, circumventing Luigi's default behaviour.
    Deletes the data produced by the task, and should be therefore used with care.
    Useful for testing and debugging.
    """
    return FLAGS.force > hierarchy
    
########################################################################
### CALCULATE THE MOST ADEQUATE BINNING BASED ON DATA ##################
########################################################################
class DefineBinning(ForceableEnsureRecentTarget):
    args = utils.dotDict(lcfg.bins_params)
    args.update( {'tag': lcfg.tag} )
    
    target_path = get_target_path( args.taskname )
    
    @WorkflowDebugger(flag=FLAGS.debug_workflow)
    def output(self):
        target = defineBinning_outputs( self.args )

        #write the target files for debugging
        utils.remove( self.target_path )
        with open( self.target_path, 'w' ) as f:
            f.write( target + '\n' )

        return luigi.LocalTarget(target)

    @WorkflowDebugger(flag=FLAGS.debug_workflow)
    def run(self):
        defineBinning( self.args )

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
        targets_list = submitTrigger_outputs( self.args, param='root' )

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

    @WorkflowDebugger(flag=FLAGS.debug_workflow)
    def requires(self):
        force_flag = set_force_boolean(self.args.hierarchy)
        return DefineBinning(force=force_flag)


########################################################################
### SUBMIT TRIGGER COUNTS USING HTCONDOR ###############################
########################################################################
class SubmitTriggerCounts(ForceableEnsureRecentTarget):
    args = utils.dotDict(lcfg.submit_params)
    args.update( {'targetsPrefix': lcfg.targets_prefix,
                  'tag': lcfg.tag,
                  } )
    
    target_path = get_target_path( args.taskname )
    
    @WorkflowDebugger(flag=FLAGS.debug_workflow)
    def output(self):
        targets = []
        targets_list = submitTrigger_outputs( self.args, param='txt' )

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
        submitTriggerCounts( self.args )

        time.sleep(1.0)
        os.system('condor_q')

    @WorkflowDebugger(flag=FLAGS.debug_workflow)
    def requires(self):
        force_flag = set_force_boolean(self.args.hierarchy)
        return DefineBinning(force=force_flag)


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
            newt = t.replace(_placeholder_cuts,'')
            targets.append( luigi.LocalTarget(newt) )

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
        force_flag = set_force_boolean(self.args.hierarchy)
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
            for elem in t:
                targets.append( luigi.LocalTarget(elem) )

        #write the target files for debugging
        utils.remove( self.target_path )
        with open( self.target_path, 'w' ) as f:
            for t in targets_list:
                for elem in t:
                    f.write( elem )
                
        return targets

    @WorkflowDebugger(flag=FLAGS.debug_workflow)
    def run(self):
        draw2DTriggerSF( self.args )

    @WorkflowDebugger(flag=FLAGS.debug_workflow)
    def requires(self):
        force_flag = set_force_boolean( self.args.hierarchy )
        return [ HaddTriggerEff(force=force_flag, samples=self.args.data,
                                target_suffix=self.args.target_suffix,
                                dataset_name=self.args.data_name),
                 HaddTriggerEff(force=force_flag, samples=self.args.mc_processes,
                                target_suffix=self.args.target_suffix,
                                dataset_name=self.args.mc_name) ]

########################################################################
### DRAW VARIABLES' DISTRIBUTIONS ######################################
########################################################################
class DrawDistributions(ForceableEnsureRecentTarget):
    args = utils.dotDict(lcfg.drawcounts_params)
    args.update( {'targetsPrefix': lcfg.targets_prefix,
                  'tag': lcfg.tag,
                  } )
    target_path = get_target_path( args.taskname )
    
    @WorkflowDebugger(flag=FLAGS.debug_workflow)
    def output(self):
        targets = []
        targets_list, _ = drawDistributions_outputs( self.args )

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
        drawDistributions( self.args )

    @WorkflowDebugger(flag=FLAGS.debug_workflow)
    def requires(self):
        force_flag = set_force_boolean(self.args.hierarchy)
        return [ HaddTriggerEff(force=force_flag, samples=self.args.data,
                                target_suffix=self.args.target_suffix,
                                dataset_name=self.args.data_name),
                 HaddTriggerEff(force=force_flag, samples=self.args.mc_processes,
                                target_suffix=self.args.target_suffix,
                                dataset_name=self.args.mc_name) ]

########################################################################
### DRAW TRIGGER INTERSECTION COUNTS ###################################
########################################################################
class DrawTriggerCounts(ForceableEnsureRecentTarget):
    samples = luigi.ListParameter()
    dataset_name = luigi.Parameter()
    args = utils.dotDict(lcfg.drawcounts_params)
    args.update( {'tag': lcfg.tag,
                  } )
    
    target_path = get_target_path( args.taskname )
    
    @WorkflowDebugger(flag=FLAGS.debug_workflow)
    def output(self):
        self.args['samples'] = luigi_to_raw( self.samples )
        self.args['dataset_name'] = self.dataset_name
        targets = []
        targets_png, targets_txt = addTriggerCounts_outputs( self.args )

        #define luigi targets
        for tpng,ttxt in zip(targets_png,targets_txt):
            targets.append( luigi.LocalTarget(tpng) )
            targets.append( luigi.LocalTarget(ttxt) )

        #write the target files for debugging
        utils.remove( self.target_path )
        with open( self.target_path, 'w' ) as f:
            for tpng,ttxt in zip(targets_png,targets_txt):
                f.write( tpng )
                f.write( ttxt )

        return targets

    @WorkflowDebugger(flag=FLAGS.debug_workflow)
    def run(self):
        self.args['samples'] = luigi_to_raw( self.samples )
        self.args['dataset_name'] = self.dataset_name
        addTriggerCounts( self.args )

########################################################################
### MAIN ###############################################################
########################################################################
if __name__ == "__main__":
    """
    1. Safety check to avoid catastrophic data deletions
    2. Run the workflow
    """
    if is_force_mistake(FLAGS.force, FLAGS.submit):
        print('Workflow interrupted.')
        exit(0)

    utils.createSingleDir( lcfg.tag_folder )
    utils.createSingleDir( lcfg.targets_folder )

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
        last_tasks = [ SubmitTriggerCounts(force=FLAGS.force>0),
                       SubmitTriggerEff(force=FLAGS.force>0),
                      ]
        if FLAGS.counts: #overwrites
            last_tasks = [ SubmitTriggerCounts(force=FLAGS.force>0), ]

    else:
        count_tasks = [ DrawTriggerCounts(force=FLAGS.force>0,
                                          samples=lcfg._selected_data,
                                          dataset_name=FLAGS.data),
                        DrawTriggerCounts(force=FLAGS.force>0,
                                          samples=lcfg._selected_mc_processes,
                                          dataset_name=FLAGS.mc_process),
                       ]

        last_tasks = count_tasks[:]
        if FLAGS.distributions:
            last_tasks += [ DrawDistributions(force=FLAGS.force>0) ]
            
        if FLAGS.distributions != 2:
            last_tasks += [ Draw1DTriggerScaleFactors(force=FLAGS.force>0),
                            #Draw2DTriggerScaleFactors(force=FLAGS.force>0)
            ]

        if FLAGS.counts: #overwrites
            last_tasks = count_tasks
            
    if FLAGS.scheduler == 'central':
        luigi.build(last_tasks,
                    workers=FLAGS.workers, local_scheduler=False, log_level='INFO')
    if FLAGS.scheduler == 'local':
        luigi.build(last_tasks,
                    local_scheduler=True, log_level='INFO')

else:
    raise RuntimeError('This script can only be run directly from the command line.')

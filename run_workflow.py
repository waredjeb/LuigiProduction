import os
import time
import utils
import inspect

os.environ['LUIGI_CONFIG_PATH'] = os.environ['PWD']+'/luigi_conf/luigi.cfg'
assert os.path.exists(os.environ['LUIGI_CONFIG_PATH'])
import luigi
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

from scripts.defineBinning import (
    defineBinning,
    defineBinning_outputs,
)
from scripts.writeHTCondorHistogramFiles import (
    writeHTCondorHistogramFiles,
    writeHTCondorHistogramFiles_outputs,
)
from scripts.writeHTCondorHaddFiles import (
    writeHTCondorHaddFiles,
    writeHTCondorHaddFiles_outputs,
)
from scripts.writeHTCondorEfficienciesAndScaleFactorsFiles import (
    writeHTCondorEfficienciesAndScaleFactorsFiles,
    writeHTCondorEfficienciesAndScaleFactorsFiles_outputs,
)
from scripts.writeHTCondorDiscriminatorFiles import (
    writeHTCondorDiscriminatorFiles,
    writeHTCondorDiscriminatorFiles_outputs,
)
from scripts.writeHTCondorUnionWeightsCalculatorFiles import (
    writeHTCondorUnionWeightsCalculatorFiles,
    writeHTCondorUnionWeightsCalculatorFiles_outputs,
)
from scripts.writeHTCondorDAGFiles import (
    writeHTCondorDAGFiles,
    writeHTCondorDAGFiles_outputs,
)
from scripts.addTriggerCounts import (
    addTriggerCounts,
    addTriggerCounts_outputs
    )
from scripts.draw2DTriggerSF import draw2DTriggerSF, draw2DTriggerSF_outputs
from scripts.drawDistributions import drawDistributions, drawDistributions_outputs

from utils import utils

import re
re_txt = re.compile('\.txt')

########################################################################
### HELPER FUNCTIONS ###################################################
########################################################################
def convertToLuigiLocalTargets(targets):
    """Converts a list of files into a list of luigi targets."""
    if not isinstance(targets, (tuple,list,set)):
        targets = [ targets ]
    return [ luigi.LocalTarget(t) for t in targets ]

def getTargetPath(taskname):
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
    
    target_path = getTargetPath( args.taskname )
    
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
### WRITE HTCONDOR FILES FOR TOTAL AND PASSED TRIGGER HISTOGRAMS #######
########################################################################
class WriteHTCondorProcessingFiles(ForceableEnsureRecentTarget):
    params = utils.dotDict(lcfg.histos_params)

    mode = luigi.ChoiceParameter(choices=lcfg.modes.keys(),
                                 var_type=str)

    target_path = getTargetPath( params.taskname )
    
    @WorkflowDebugger(flag=FLAGS.debug_workflow)
    def output(self):
        self.params['mode'] = self.mode
        o1, o2, _, _ = writeHTCondorHistogramFiles_outputs(self.params)

        #write the target files for debugging
        utils.remove( self.target_path )
        with open( self.target_path, 'w' ) as f:
            for t in o1: f.write( t + '\n' )
            for t in o2: f.write( t + '\n' )

        _c1 = convertToLuigiLocalTargets(o1)
        _c2 = convertToLuigiLocalTargets(o2)
        return _c1 + _c2
    
    @WorkflowDebugger(flag=FLAGS.debug_workflow)
    def run(self):
        self.params['mode'] = self.mode
        writeHTCondorHistogramFiles(self.params)

    @WorkflowDebugger(flag=FLAGS.debug_workflow)
    def requires(self):
        force_flag = set_force_boolean(self.params.hierarchy)
        return DefineBinning(force=force_flag)


########################################################################
### WRITE HTCONDOR FILES FOR HADD'ING HISTOGRAMS IN ROOT FILES #########
########################################################################
class WriteHTCondorHaddFiles(ForceableEnsureRecentTarget):
    samples = luigi.ListParameter()
    dataset_name = luigi.Parameter()
    args = utils.dotDict(lcfg.hadd_params)
    args['tprefix'] = lcfg.modes['histos']
    
    target_path = getTargetPath( args.taskname )
    
    @WorkflowDebugger(flag=FLAGS.debug_workflow)
    def output(self):
        self.args['samples'] = luigi_to_raw( self.samples )
        self.args['dataset_name'] = self.dataset_name
        o1, o2, _ = writeHTCondorHaddFiles_outputs( self.args )
        
        #write the target files for debugging
        utils.remove( self.target_path )
        with open( self.target_path, 'w' ) as f:
            for t in o1: f.write( t + '\n' )
            for t in o2: f.write( t + '\n' )

        _c1 = convertToLuigiLocalTargets(o1)
        _c2 = convertToLuigiLocalTargets(o2)
        return _c1 + _c2

    @WorkflowDebugger(flag=FLAGS.debug_workflow)
    def run(self):
        self.args['samples'] = luigi_to_raw( self.samples )
        self.args['dataset_name'] = self.dataset_name
        writeHTCondorHaddFiles( self.args )

########################################################################
### WRITE HTCONDOR FILES FOR EFFICIENCIES AND SCALE FACTORS ############
########################################################################
class WriteHTCondorEfficienciesAndScaleFactorsFiles(ForceableEnsureRecentTarget):
    params = utils.dotDict(lcfg.drawsf_params)
    params['tprefix'] = lcfg.modes['histos']
    target_path = getTargetPath( params.taskname )
    
    @WorkflowDebugger(flag=FLAGS.debug_workflow)
    def output(self):
        o1, o2, _ = writeHTCondorEfficienciesAndScaleFactorsFiles_outputs(self.params)

        #write the target files for debugging
        utils.remove( self.target_path )
        with open( self.target_path, 'w' ) as f:
            f.write( o1 + '\n' )
            f.write( o2 + '\n' )

        _c1 = convertToLuigiLocalTargets(o1)
        _c2 = convertToLuigiLocalTargets(o2)
        return _c1 + _c2

    @WorkflowDebugger(flag=FLAGS.debug_workflow)
    def run(self):
        writeHTCondorEfficienciesAndScaleFactorsFiles(self.params)

########################################################################
### WRITE HTCONDOR FILES FOR THE VARIABLE DISCRIMINATOR ################
########################################################################
class WriteHTCondorDiscriminatorFiles(ForceableEnsureRecentTarget):
    params = utils.dotDict(lcfg.discriminator_params)
    target_path = getTargetPath( params.taskname )
    
    @WorkflowDebugger(flag=FLAGS.debug_workflow)
    def output(self):
        o1, o2, _ = writeHTCondorDiscriminatorFiles_outputs(self.params)

        #write the target files for debugging
        utils.remove( self.target_path )
        with open( self.target_path, 'w' ) as f:
            for t in o1: f.write( t + '\n' )
            for t in o2: f.write( t + '\n' )

        _c1 = convertToLuigiLocalTargets(o1)
        _c2 = convertToLuigiLocalTargets(o2)
        return _c1 + _c2

    @WorkflowDebugger(flag=FLAGS.debug_workflow)
    def run(self):
        writeHTCondorDiscriminatorFiles(self.params)

########################################################################
### WRITE HTCONDOR FILES FOR SCALE FACTOR CALCULATOR ###################
########################################################################
class WriteHTCondorUnionWeightsCalculatorFiles(ForceableEnsureRecentTarget):
    params = utils.dotDict(lcfg.calculator_params)
    target_path = getTargetPath( params.taskname )
    
    @WorkflowDebugger(flag=FLAGS.debug_workflow)
    def output(self):
        o1, o2, _ = writeHTCondorUnionWeightsCalculatorFiles_outputs(self.params)

        #write the target files for debugging
        utils.remove( self.target_path )
        with open( self.target_path, 'w' ) as f:
            for t in o1: f.write( t + '\n' )
            for t in o2: f.write( t + '\n' )

        _c1 = convertToLuigiLocalTargets(o1)
        _c2 = convertToLuigiLocalTargets(o2)
        return _c1 + _c2

    @WorkflowDebugger(flag=FLAGS.debug_workflow)
    def run(self):
        writeHTCondorUnionWeightsCalculatorFiles(self.params)

        
########################################################################
### DRAW 2D TRIGGER SCALE FACTORS #######################################
########################################################################
class Draw2DTriggerScaleFactors(ForceableEnsureRecentTarget):
    args = utils.dotDict(lcfg.drawsf_params)
    args.update( {'tprefix': lcfg.modes['histos'], } )
    target_path = getTargetPath( args.taskname )
    
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
                                dataset_name=self.args.data_name),
                 HaddTriggerEff(force=force_flag, samples=self.args.mc_processes,
                                dataset_name=self.args.mc_name) ]

########################################################################
### DRAW VARIABLES' DISTRIBUTIONS ######################################
########################################################################
class DrawDistributions(ForceableEnsureRecentTarget):
    args = utils.dotDict(lcfg.drawcounts_params)
    args.update( {'tprefix': lcfg.modes['histos'], } )
    target_path = getTargetPath( args.taskname )
    
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
                                dataset_name=self.args.data_name),
                 HaddTriggerEff(force=force_flag, samples=self.args.mc_processes,
                                dataset_name=self.args.mc_name) ]

########################################################################
### DRAW TRIGGER INTERSECTION COUNTS ###################################
########################################################################
class DrawTriggerCounts(ForceableEnsureRecentTarget):
    samples = luigi.ListParameter()
    dataset_name = luigi.Parameter()
    args = utils.dotDict(lcfg.drawcounts_params)
    args.update( {'tprefix': lcfg.modes['counts'],} )
    
    target_path = getTargetPath( args.taskname )
    
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
### TRIGGERING ALL HTCONDOR WRITING CLASSES ############################
########################################################################
class WriteDAG(ForceableEnsureRecentTarget):
    params  = utils.dotDict(lcfg.write_params)
    pHistos = utils.dotDict(lcfg.histos_params)
    pHadd   = utils.dotDict(lcfg.hadd_params)
    pEffSF  = utils.dotDict(lcfg.drawsf_params)
    pDisc   = utils.dotDict(lcfg.discriminator_params)
    pSFCalc = utils.dotDict(lcfg.calculator_params)
    
    pHadd['tprefix']  = lcfg.modes['histos']
    pEffSF['tprefix'] =  lcfg.modes['histos']

    target_path = getTargetPath( params.taskname )
    
    @WorkflowDebugger(flag=FLAGS.debug_workflow)
    def output(self):
        o1 = writeHTCondorDAGFiles_outputs(self.params)

        #write the target files for debugging
        utils.remove( self.target_path )
        with open( self.target_path, 'w' ) as f:
            for t in o1: f.write( t + '\n' )

        _c1 = convertToLuigiLocalTargets(o1)
        return _c1

    @WorkflowDebugger(flag=FLAGS.debug_workflow)
    def run(self):
        self.pHistos['mode'] = 'histos'
        _, submHistos, _, _ = writeHTCondorHistogramFiles_outputs(self.pHistos)
        self.pHistos['mode'] = 'counts'
        _, submCounts, _, _ = writeHTCondorHistogramFiles_outputs(self.pHistos)

        self.pHadd['dataset_name'] = FLAGS.data
        _, submHaddData, _   = writeHTCondorHaddFiles_outputs(self.pHadd)
        self.pHadd['dataset_name'] = FLAGS.mc_process
        _, submHaddMC, _   = writeHTCondorHaddFiles_outputs(self.pHadd)
        
        _, submEffSF, _  = writeHTCondorEfficienciesAndScaleFactorsFiles_outputs(self.pEffSF)

        _, submEffDisc, _  = writeHTCondorDiscriminatorFiles_outputs(self.pDisc)

        _, submSFCalc, _  = writeHTCondorUnionWeightsCalcatorFiles_outputs(self.pSFCalc)

        self.params['jobsHistos']   = submHistos
        self.params['jobsCounts']   = submCounts
        self.params['jobsHaddData'] = submHaddData
        self.params['jobsHaddMC']   = submHaddMC
        self.params['jobsEffSF']    = submEffSF
        self.params['jobsEffDiscr'] = submEffDisc
        self.params['jobsSFCalc']   = submEffCalc
        writeHTCondorDAGFiles( self.params )
        
class SubmitDAG(luigi.Task):
    """
    Dummy submission class.
    Makes sures all submission files are written.
    """
    @WorkflowDebugger(flag=FLAGS.debug_workflow)
    def run(self):
        out = WriteDAG().output()[0].path
        os.system('condor_submit_dag -no_submit {}'.format(out))
        #os.system('condor_submit {}.condor.sub'.format(out))
        #os.system('sleep 2')
        #os.system('condor_q')

    def complete(self):
        """This task is never complete, i.e., its requirements all always checked."""
        return False 
        
    @WorkflowDebugger(flag=FLAGS.debug_workflow)
    def requires(self):
        return [ WriteHTCondorProcessingFiles( mode='histos' ),
                 WriteHTCondorProcessingFiles( mode='counts' ),
                 WriteHTCondorHaddFiles( dataset_name=FLAGS.data,
                                         samples=lcfg._selected_data ),
                 WriteHTCondorHaddFiles( dataset_name=FLAGS.mc_process,
                                         samples=lcfg._selected_mc_processes ),
                 WriteHTCondorEfficienciesAndScaleFactorsFiles(),
                 WriteHTCondorDiscriminatorFiles(),
                 WriteHTCondorUnionWeightsCalculatorFiles(),
                 WriteDAG(),
                ]
    
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

    utils.createSingleDir( lcfg.data_storage )
    utils.createSingleDir( lcfg.targets_folder )
    
    if FLAGS.submit:
        last_tasks = [ SubmitDAG() ]
        
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
            triggercomb = utils.generateTriggerCombinations(FLAGS.triggers)

            #one task per trigger combination
            for tcomb in triggercomb:
                last_tasks += [
                    Draw1DTriggerScaleFactors(
                        force=FLAGS.force>0,
                        trigger_combination=tcomb)
                ]
            
            # last_tasks += [ Draw1DTriggerScaleFactors(force=FLAGS.force>0),
            #                 Draw2DTriggerScaleFactors(force=FLAGS.force>0) ]

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

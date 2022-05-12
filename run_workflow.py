import os
import time
import utils
import inspect

os.environ['LUIGI_CONFIG_PATH'] = os.environ['PWD']+'/luigi_conf/luigi.cfg'
assert os.path.exists(os.environ['LUIGI_CONFIG_PATH'])
import luigi
from luigi_conf.luigi_utils import  (
    ForceRun,
    WorkflowDebugger,
)
from luigi_conf.luigi_cfg import cfg, FLAGS

from luigi_conf import (
    _placeholder_cuts,
)
lcfg = cfg() #luigi configuration

from scripts.writeHTCondorStep1 import (
    writeHTCondorStep1_outputs,
    writeHTCondorStep1,
)

from scripts.writeHTCondorStep2 import (
    writeHTCondorStep2_outputs,
    writeHTCondorStep2,
)

from scripts.writeHTCondorDAGFiles import (
    WriteDAGManager,
    writeHTCondorDAGFiles_outputs,
)

from utils import utils
from scripts.jobWriter import JobWriter

import re
re_txt = re.compile('\.txt')

########################################################################
### HELPER FUNCTIONS ###################################################
########################################################################
def convert_to_luigi_local_targets(targets):
    """Converts a list of files into a list of luigi targets."""
    if not isinstance(targets, (tuple,list,set)):
        targets = [ targets ]
    return [ luigi.LocalTarget(t) for t in targets ]

def get_target_path(taskname):
    target_path = os.path.join(lcfg.targets_folder, re_txt.sub( '_'+taskname+'.txt',
                                                                lcfg.targets_default_name ) ) 
    return target_path

def luigi_to_raw( param ):
    """
    Converts luigi parameters to their "raw" versions.
    Useful when passing them to functions.
    """
    if isinstance(param, (tuple,list)):
        return [x for x in param]
    else:
        raise NotImplementedError('[' + inspect.stack()[0][3] + ']: ' + 'only tuples/lists implemented so far!')
    



########################################################################
### WRITE HTCONDOR FILES FOR HADDING TXT COUNT FILES ###################
########################################################################
class WriteHTCondorStep1Files(ForceRun):
    args = utils.dotDict(lcfg.step1_params)
    sample_file = args['sample_file']
    sample_name = args['sample_name']
    
    @WorkflowDebugger(flag=FLAGS.debug_workflow)
    def output(self):
        self.args['sample_file'] = luigi_to_raw( [self.sample_file] )
        self.args['sample_name'] = self.sample_name
        o1, o2, _ = writeHTCondorStep1_outputs( self.args )
        
        #write the target files for debugging
        target_path = get_target_path( self.__class__.__name__ )
        utils.remove( target_path )
        with open( target_path, 'w' ) as f:
            for t in o1: f.write( t + '\n' )
            for t in o2: f.write( t + '\n' )
        print(f"Luigi Local target {o1} {o2}")
        _c1 = convert_to_luigi_local_targets(o1)
        _c2 = convert_to_luigi_local_targets(o2)
        return _c1 + _c2

    @WorkflowDebugger(flag=FLAGS.debug_workflow)
    def run(self):
        self.args['sample_file'] = luigi_to_raw( [self.sample_file] )
        self.args['sample_name'] = luigi_to_raw( [self.sample_name] )
        writeHTCondorStep1( self.args )

class WriteDAG(ForceRun):
    params      = utils.dotDict(lcfg.write_params)
    pStep1 = utils.dotDict(lcfg.step1_params)
    
    @WorkflowDebugger(flag=FLAGS.debug_workflow)
    def output(self):
        o1 = writeHTCondorDAGFiles_outputs(self.params)

        #write the target files for debugging
        target_path = get_target_path( self.__class__.__name__ )
        utils.remove( target_path )
        with open( target_path, 'w' ) as f:
            f.write( o1 + '\n' )

        _c1 = convert_to_luigi_local_targets(o1)
        return _c1

    @WorkflowDebugger(flag=FLAGS.debug_workflow)
    def run(self):
        _, subStep1, _   = writeHTCondorStep1_outputs(self.pStep1)
        _, subStep2, _   = writeHTCondorStep2_outputs(self.pStep1)
        print(f"SUBSTEP1 {subStep1} ")

        jobs = { 
                 'jobsStep1': subStep1,
                 'jobsStep2': subStep2
                }

        dag_manager = WriteDAGManager( self.params['localdir'], self.params['tag'], self.params['data_name'],
                                       jobs )
        dag_manager.write_all()
        
class SubmitDAG(ForceRun):
    """
    Submission class.
    """
    def edit_condor_submission_file(self, out):
        jw = JobWriter()
        with open(out, 'r') as f:
            contents = f.readlines()
        ncontents = len(contents)
        new_content = jw.llr_condor_specific_content(queue='short')
        contents.insert(ncontents-1, new_content + '\n')
        with open(out, 'w') as f:
            contents = "".join(contents)
            f.write(contents)
        
    @WorkflowDebugger(flag=FLAGS.debug_workflow)
    def run(self):
        out = self.input()[-1][0].path
        os.system('condor_submit_dag -no_submit -f {}'.format(out))
        os.system('sleep 1')
        self.edit_condor_submission_file(out + '.condor.sub')
        os.system('sleep 1')
        # os.system('condor_submit {}.condor.sub'.format(out))
        os.system('sleep 1')
        os.system('condor_q')

    @WorkflowDebugger(flag=FLAGS.debug_workflow)
    def output(self):
        # WriteDag dependency is the last one
        target = self.input()[-1][0].path + '.condor.sub'
        return luigi.LocalTarget(target)

    @WorkflowDebugger(flag=FLAGS.debug_workflow)
    def requires(self):
        return [ WriteHTCondorStep1Files(),
                 WriteDAG(),
                ]
   
########################################################################
### MAIN ###############################################################
########################################################################
if __name__ == "__main__":
    print(f"DATA STORAGE {lcfg.data_storage} {lcfg.targets_folder}")
    utils.create_single_dir( lcfg.data_storage )
    utils.create_single_dir( lcfg.targets_folder )
    
    last_tasks = [ SubmitDAG() ]
            
    if FLAGS.scheduler == 'central':
        luigi.build(last_tasks,
                    workers=1, local_scheduler=False, log_level='INFO')
    if FLAGS.scheduler == 'local':
        luigi.build(last_tasks,
                    local_scheduler=True, log_level='INFO')

else:
    raise RuntimeError('This script can only be run directly from the command line.')

import os
import argparse
import luigi
from luigi.util import inherits

from scripts.haddTriggerEff import haddTriggerEff

########################################################################
### ARGUMENT PARSING ###################################################
########################################################################
_tasks = ( 'hadd', 'drawsf' )
_triggers = ['all', #all trig
             '9', '10', '11', '12',     #single trig
             '13', '14'
             ]
_processes = dict( Radions =    ('Radion_m300',
                                 'Radion_m400',
                                 'Radion_m500',
                                 'Radion_m600',
                                 'Radion_m700',
                                 'Radion_m800',
                                 'Radion_m900',),
                  
                   SingleMuon = ('SingleMuon2018',
                                 'SingleMuon2018A',
                                 'SingleMuon2018B',
                                 'SingleMuon2018C',
                                 'SingleMuon2018D'),
                   
                   MET =        ('MET2018A',
                                 'MET2018B',
                                 'MET2018C',
                                 'MET2018D',),
                   
                   TT =         ('TTall',
                                 'TT_fullyHad',
                                 'TT_fullyLep',
                                 'TT_semiLep',),
                   
                   DY =         ('DY',
                                 'DYall',
                                 'DY_lowMass',),
                  )
    
parser = argparse.ArgumentParser()
choices = [x for x in range(len(_tasks)+1)]
parser.add_argument(
    '--force',
    type=int,
    choices=choices,
    default=0,
    help="Force running a certain number of tasks, even if the corresponding targets exist.\n The value '" + str(choices) + "' runs the highest-level task and so on up to '" + str(choices[-1]) + "').\n It values follow the hierarchies defined in the cfg() class."
)
parser.add_argument(
    '--workers',
    type=int,
    default=1,
    help="Maximum number of worker which can be used to run the pipeline."
)
parser.add_argument(
    '--scheduler',
    type=str,
    choices=['local', 'central'],
    default='local',
    help='Select the scheduler for luigi.'
)
parser.add_argument(
    '--processes',
    nargs='+', #1 or more arguments
    type=str,
    required=True,
    choices=_processes.keys(),
    help='Select the processes over which the workflow will be run.'
)
parser.add_argument(
    '--triggers',
    nargs='+', #1 or more arguments
    type=str,
    required=True,
    choices=_triggers,
    help='Select the processes over which the workflow will be run.'
)
parser.add_argument(
    '--tag',
    type=str,
    required=True,
    help='Specifies a tag to differentiate workflow runs.'
)
parser.add_argument(
    '--debug_workflow',
    action='store_true',
    help="Explicitly print the functions being run for each task, for workflow debugging purposes."
)
FLAGS, _ = parser.parse_known_args()

########################################################################
### HELPER FUNCTIONS ###################################################
########################################################################
def task_index( t ):
    return luigi.IntParameter(default=_tasks.index(t)+1)

def set_task_and_hierarchy( task_name ):
    assert( task_name in _tasks )
    return ( luigi.Parameter(default=task_name),
             luigi.IntParameter( task_index(task_name)) )

########################################################################
### LUIGI CONFIGURATION ################################################
########################################################################
class cfg(luigi.Config):
    base_name = 'FRAMEWORKTEST'
    data_base = os.path.join( '/data_CMS/', 'cms' )
    user = os.environ['USER']
    data_storage = os.path.join(data_base, user, base_name)

    ### Define luigi parameters ###
    # general
    tag = luigi.Parameter( FLAGS.tag )
    tag_folder = luigi.Parameter( os.path.join(data_storage, FLAGS.tag) )
    targets_folder = luigi.Parameter( os.path.join(data_storage, FLAGS.tag,
                                                   'targets/') )
    targets_default_name = luigi.Parameter( default='DefaultTarget.txt' )
    targets_prefix = luigi.Parameter(default='hist_')
    
    # haddTriggerEff
    hadd_taskname, hadd_hierarchy = set_task_and_hierarchy( 'hadd' )
    hadd_processes = luigi.DictParameter( default=FLAGS.processes )
    hadd_indir = luigi.Parameter(default=data_storage)

    # drawTriggerScaleFactors
    drawsf_taskname, hadd_hierarchy = set_task_and_hierarchy( 'drawsf' )
    drawsf_indir = luigi.Parameter(default=data_storage)
    drawsf_processes = luigi.DictParameter( default=FLAGS.processes )
    drawsf_triggers = luigi.DictParameter( default=FLAGS.triggers )
    drawdf_htcut = luigi.Parameter(default='metnomu200cut')

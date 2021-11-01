import os
import argparse
import luigi
from luigi.util import inherits

from scripts.haddTriggerEff import haddTriggerEff

########################################################################
### ARGUMENT PARSING ###################################################
########################################################################
_tasks = ( 'hadd', 'comp', 'drawsf' )
_triggers = ('all', #all trig
             '9', '10', '11', '12',     #single trig
             '13', '14'
             )
_channels = ( 'all', 'etau', 'mutau', 'tautau', 'mumu' )
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
    '--channels',
    nargs='+', #1 or more arguments
    type=str,
    default=_channels,
    help='Select the channels over which the workflow will be run.'
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
def set_task_name(n):
    "handles the setting of each task name"
    assert( n in _tasks )
    return n

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
    _rawname = set_task_name('hadd')
    hadd_params = luigi.DictParameter(
        default={ 'taskname': _rawname,
                  'hierarchy': _tasks.index(_rawname)+1,
                  'processes': FLAGS.processes,
                  'indir': data_storage } )

    # compareTriggers
    _rawname = set_task_name('comp')
    comp_params = luigi.DictParameter(
        default={ 'taskname': _rawname,
                  'hierarchy': _tasks.index(_rawname)+1,
                  'processes': FLAGS.processes,
                  'indir': data_storage } )
    
    # drawTriggerScaleFactors
    _rawname = set_task_name('drawsf')
    drawsf_params = luigi.DictParameter(
        default={ 'taskname': _rawname,
                  'hierarchy': _tasks.index(_rawname)+1,
                  'processes': FLAGS.processes,
                  'indir': data_storage,
                  'triggers': FLAGS.triggers,
                  'channels': FLAGS.channels,
                  'htcut': 'metnomu200cut',
                  'data_type': 'MET2018_sum' } )

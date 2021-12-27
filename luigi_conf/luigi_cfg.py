import os
import argparse
import luigi
from luigi.util import inherits

from . import _data, _mc_processes, _triggers_map, _channels
from . import _variables_eff, _variables_dist
from . import _nonStandTriggers, _trigger_custom, _trigger_shift, _triggers_map

######################################################################## 
### ARGUMENT PARSING ###################################################
########################################################################
# hierarchies are used in conjunction with the '--force' flag
_tasks_before_htcondor = { 'bins': 2, 'submit': 1 }
_tasks_after_htcondor = { 'hadd': 2, 'comp': 1, 'drawsf': 1, 'drawdist': 1 }
max_task_number = ( len(_tasks_after_htcondor.keys())
                    if len(_tasks_after_htcondor.keys()) > len(_tasks_after_htcondor)
                    else len(_tasks_after_htcondor.keys()) )

parser = argparse.ArgumentParser()
choices = [x for x in range(max_task_number+1)]
parser.add_argument(
    '--force',
    type=int,
    choices=choices,
    default=0,
    help="Force running a certain number of tasks, even if the corresponding targets exist.\n The value '" + str(choices) + "' runs the highest-level task and so on up to '" + str(choices[-1]) + "').\n It values follow the hierarchies defined in the cfg() class."
)
parser.add_argument(
    '--nbins',
    type=int,
    default=8,
    help="Number of histogram bins. If fine-grained control is required modify the variable `_bins` in the luigi configuration file."
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
    '--data',
    type=str,
    required=True,
    choices=_data.keys(),
    help='Select the data over which the workflow will be run.'
)
parser.add_argument(
    '--mc_process',
    type=str,
    required=True,
    choices=_mc_processes.keys(),
    help='Select the MC processes over which the workflow will be run.'
)
parser.add_argument(
    '--triggers',
    nargs='+', #1 or more arguments
    type=str,
    required=True,
    choices=_triggers_map.keys(),
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
    '--variables_for_efficiencies',
    nargs='+', #1 or more arguments
    type=str,
    default=_variables_eff,
    help='Select the variables to be used for the calculation of efficiencies.'
)
parser.add_argument(
    '--variables_for_distributions',
    nargs='+', #1 or more arguments
    type=str,
    default=_variables_dist,
    help='Select the variables to be used for the display of distributions.'
)
parser.add_argument(
    '--outuser',
    type=str,
    default=os.environ['USER'],
    help='The username required to write the output scale factor plots.'
)
parser.add_argument(
    '--tag',
    type=str,
    required=True,
    help='Specifies a tag to differentiate workflow runs.'
)
parser.add_argument(
    '--subtag',
    type=str,
    default='default',
    help='Specifies a subtag, for instance an additional cut within the same tag. We force its first character to be an underscore.'
)
parser.add_argument(
    '--submit',
    action='store_true',
    help="Executes the submission to HTCondor."
)
parser.add_argument(
    '--distributions',
    type=int,
    choices=[0,1,2],
    default=0,
    help="0: Does not draws the distributions.\n1: Also draws the distributions.\n2: Only draws the distributions."
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
    assert( n in _tasks_before_htcondor.keys()
            or n in _tasks_after_htcondor.keys()  )
    return n

########################################################################
### LUIGI CONFIGURATION ################################################
########################################################################
class cfg(luigi.Config):
    base_name = 'TriggerScaleFactors'
    data_base = os.path.join( '/data_CMS/', 'cms' )
    user = os.environ['USER']
    data_storage = os.path.join(data_base, user, base_name)
    data_target = 'MET2018_sum'
    web_storage = os.path.join('/eos/', 'home-b',
                               FLAGS.outuser, 'www', base_name)
    
    ### Define luigi parameters ###
    # general
    tag = FLAGS.tag
    subtag = ( FLAGS.subtag if FLAGS.subtag==''
               else ( '_' + FLAGS.subtag if FLAGS.subtag[0] != '_' else FLAGS.subtag ) )
    tag_folder = os.path.join(data_storage, tag)
    web_folder = os.path.join(web_storage, tag)
    targets_folder = os.path.join(data_storage, tag, 'targets')
    targets_default_name = 'DefaultTarget.txt'
    targets_prefix = 'hist_eff_'

    data_input = '/data_CMS/cms/portales/HHresonant_SKIMS/SKIMS_Radion_2018_fixedMETtriggers_mht_16Jun2021/'

    binedges_filename = os.path.join(tag_folder, 'binedges.hdf5')

    variables_join = list(set(FLAGS.variables_for_efficiencies + FLAGS.variables_for_distributions))

    ####
    #### defineBinning
    ####   
    _rawname = set_task_name('bins')
    bins_params = luigi.DictParameter(
        default={ 'taskname': _rawname,
                  'hierarchy': _tasks_before_htcondor[_rawname],
                  'nbins': FLAGS.nbins,
                  'binedges_filename': binedges_filename,
                  'indir': data_input,
                  'outdir': tag_folder,
                  'data': _data[FLAGS.data],
                  'variables': variables_join,
                  'channels': FLAGS.channels,
                  'subtag': subtag,
                  'debug': FLAGS.debug_workflow} )

    ####
    #### submitTriggerEff
    ####
    _rawname = set_task_name('submit')
    submit_params = luigi.DictParameter(
        default={ 'taskname': _rawname,
                  'hierarchy': _tasks_before_htcondor[_rawname],
                  'binedges_filename': binedges_filename,
                  'indir': data_input,
                  'outdir': tag_folder,
                  'data': _data[FLAGS.data],
                  'mc_processes': _mc_processes[FLAGS.mc_process],
                  'triggers': FLAGS.triggers,
                  'channels': FLAGS.channels,
                  'variables': variables_join,
                  'targetsPrefix': targets_prefix,
                  'subtag': subtag,
                  'debug': FLAGS.debug_workflow} )

    ####
    #### haddTriggerEff
    ####
    _rawname = set_task_name('hadd')
    hadd_params = luigi.DictParameter(
        default={ 'taskname': _rawname,
                  'hierarchy': _tasks_after_htcondor[_rawname],
                  'indir': tag_folder,
                  'subtag': subtag} )

    ####
    #### compareTriggers
    ####
    _rawname = set_task_name('comp')
    comp_params = luigi.DictParameter(
        default={ 'taskname': _rawname,
                  'hierarchy': _tasks_after_htcondor[_rawname],
                  'indir': tag_folder } )

    ####
    #### drawTriggerScaleFactors
    ####
    _rawname = set_task_name('drawsf')
    # clever way to flatten a nested list
    #_selected_mc_process = sum([ _mc_processes[proc] for proc in FLAGS.mc_process ], [])
    #_selected_data = sum([ _data[x] for x in FLAGS.data ], [])
    _selected_mc_process = _mc_processes[FLAGS.mc_process]
    _selected_data = _data[FLAGS.data]
    
    drawsf_params = luigi.DictParameter(
        default={ 'taskname': _rawname,
                  'hierarchy': _tasks_after_htcondor[_rawname],
                  'data_name': FLAGS.data,
                  'mc_name': FLAGS.mc_process,
                  'data': _selected_data,
                  'mc_processes': _selected_mc_process,
                  'indir': tag_folder,
                  'outdir': web_folder,
                  'triggers': FLAGS.triggers,
                  'channels': FLAGS.channels,
                  'variables': FLAGS.variables_for_efficiencies,
                  'binedges_filename': binedges_filename,
                  'subtag': subtag,
                  'target_suffix': '_Sum',
                  'debug': FLAGS.debug_workflow,} )
    
    ####
    #### drawDistributions
    ####
    _rawname = set_task_name('drawdist')
    _selected_mc_process = _mc_processes[FLAGS.mc_process]
    _selected_data = _data[FLAGS.data]
    
    drawdist_params = luigi.DictParameter(
        default={ 'taskname': _rawname,
                  'hierarchy': _tasks_after_htcondor[_rawname],
                  'data_name': FLAGS.data,
                  'mc_name': FLAGS.mc_process,
                  'data': _selected_data,
                  'mc_processes': _selected_mc_process,
                  'indir': tag_folder,
                  'outdir': web_folder,
                  'triggers': FLAGS.triggers,
                  'channels': FLAGS.channels,
                  'variables': FLAGS.variables_for_distributions,
                  'binedges_filename': binedges_filename,
                  'subtag': subtag,
                  'target_suffix': '_Sum',
                  'debug': FLAGS.debug_workflow,} )

"""
DATA:
@ bit position - path
0 - HLT_IsoMu24_v
1 - HLT_IsoMu27_v
2 - HLT_Ele32_WPTight_Gsf_v
3 - HLT_Ele35_WPTight_Gsf_v
4 - HLT_DoubleTightChargedIsoPFTau35_Trk1_TightID_eta2p1_Reg_v
5 - HLT_DoubleMediumChargedIsoPFTau40_Trk1_TightID_eta2p1_Reg_v
6 - HLT_DoubleTightChargedIsoPFTau40_Trk1_eta2p1_Reg_v
7 - HLT_DoubleMediumChargedIsoPFTauHPS35_Trk1_eta2p1_Reg_v
8 - HLT_IsoMu20_eta2p1_LooseChargedIsoPFTauHPS27_eta2p1_CrossL1_v
9 - HLT_IsoMu20_eta2p1_LooseChargedIsoPFTau27_eta2p1_CrossL1_v
10 - HLT_Ele24_eta2p1_WPTight_Gsf_LooseChargedIsoPFTauHPS30_eta2p1_CrossL1_v
11 - HLT_Ele24_eta2p1_WPTight_Gsf_LooseChargedIsoPFTau30_eta2p1_CrossL1_v
12 - HLT_VBF_DoubleLooseChargedIsoPFTau20_Trk1_eta2p1_v
13 - HLT_VBF_DoubleLooseChargedIsoPFTauHPS20_Trk1_eta2p1_v
14 - HLT_PFHT500_PFMET100_PFMHT100_IDTight_v
15 - HLT_PFMETNoMu120_PFMHTNoMu120_IDTight_v
16 - HLT_PFMETNoMu120_PFMHTNoMu120_IDTight_PFHT60_v
17 - HLT_MediumChargedIsoPFTau50_Trk30_eta2p1_1pr_MET100_v
18 - HLT_MediumChargedIsoPFTau50_Trk30_eta2p1_1pr_MET110_v
19 - HLT_MediumChargedIsoPFTau50_Trk30_eta2p1_1pr_MET130_v

MC:
@ bit position - path
0 - HLT_IsoMu24_v
1 - HLT_IsoMu27_v
2 - HLT_Ele32_WPTight_Gsf_v
3 - HLT_Ele35_WPTight_Gsf_v
4 - HLT_DoubleMediumChargedIsoPFTauHPS35_Trk1_eta2p1_Reg_v
5 - HLT_IsoMu20_eta2p1_LooseChargedIsoPFTauHPS27_eta2p1_CrossL1_v
6 - HLT_Ele24_eta2p1_WPTight_Gsf_LooseChargedIsoPFTauHPS30_eta2p1_CrossL1_v
7 - HLT_VBF_DoubleLooseChargedIsoPFTau20_Trk1_eta2p1_v
8 - HLT_VBF_DoubleLooseChargedIsoPFTauHPS20_Trk1_eta2p1_v
9 - HLT_PFHT500_PFMET100_PFMHT100_IDTight_v
10 - HLT_PFMETNoMu120_PFMHTNoMu120_IDTight_v
11 - HLT_PFMETNoMu120_PFMHTNoMu120_IDTight_PFHT60_v
12 - HLT_MediumChargedIsoPFTau50_Trk30_eta2p1_1pr_MET100_v
13 - HLT_MediumChargedIsoPFTau50_Trk30_eta2p1_1pr_MET110_v
14 - HLT_MediumChargedIsoPFTau50_Trk30_eta2p1_1pr_MET130_v
"""

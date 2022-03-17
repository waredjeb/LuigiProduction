import os
import argparse
from argparse import RawTextHelpFormatter
import luigi
from luigi.util import inherits

from . import _inputs, _data, _mc_processes, _triggers_map, _channels
from . import _variables_eff, _variables_dist
from . import _trigger_shift, _triggers_map

######################################################################## 
### ARGUMENT PARSING ###################################################
########################################################################
parser = argparse.ArgumentParser(formatter_class=RawTextHelpFormatter)
parser.add_argument(
    '--submit',
    action='store_true',
    help="Executes the submission to HTCondor."
)
parser.add_argument(
    '--nbins',
    type=int,
    default=6,
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
    required=False,
    default=list(_triggers_map.keys()),
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
    '--distributions',
    type=int,
    choices=[0,1,2],
    default=0,
    help="0: Does not draw the distributions (default).\n1: Also draws the distributions.\n2: Only draws the distributions."
)
parser.add_argument(
    '--counts',
    action='store_true',
    help="Only runs the 'counting' workflow: check how many events pass each intersection of triggers. The default is to run the full workflow."
)
parser.add_argument(
    '--debug_workflow',
    action='store_true',
    help="Explicitly print the functions being run for each task, for workflow debugging purposes."
)
FLAGS, _ = parser.parse_known_args()

########################################################################
### LUIGI CONFIGURATION ################################################
########################################################################
class cfg(luigi.Config):
    # auxiliary, not used by scripts
    base_name = 'TriggerScaleFactors'
    data_base = os.path.join( '/data_CMS/', 'cms' )
    user = os.environ['USER']
    tag = FLAGS.tag
    
    _storage = os.path.join(data_base, user, base_name, tag)
    data_storage = os.path.join(_storage, 'Data')
    out_storage = os.path.join(_storage, 'Outputs')

    local_home = os.environ['HOME']
    local_cmssw = os.path.join(os.environ['CMSSW_VERSION'], 'src')
    local_analysis_folder = 'METTriggerStudies'

    # general
    modes = {'histos': 'hist_',
             'counts': 'counts_'}
    _closure_prefix = 'Closure'

    subtag = ( FLAGS.subtag if FLAGS.subtag==''
               else ( '_' + FLAGS.subtag if FLAGS.subtag[0] != '_' else FLAGS.subtag ) )
    local_folder = os.path.join(local_home, local_cmssw, local_analysis_folder)
    targets_folder = os.path.join(data_storage, 'targets')
    targets_default_name = 'DefaultTarget.txt'
    intersection_str = '_PLUS_'
    nocut_dummy_str = 'NoCut'
    
    binedges_filename = os.path.join(data_storage, 'binedges.hdf5')

    variables_join = list(set(FLAGS.variables_for_efficiencies + FLAGS.variables_for_distributions))

    ####
    #### defineBinning
    ####   
    bins_params = luigi.DictParameter(
        default={ 'nbins': FLAGS.nbins,
                  'binedges_filename': binedges_filename,
                  'indir': _inputs,
                  'outdir': data_storage,
                  'data': _data[FLAGS.data],
                  'variables': variables_join,
                  'channels': FLAGS.channels,
                  'tag': tag,
                  'subtag': subtag,
                  'debug': FLAGS.debug_workflow} )

    ####
    #### writeHTCondorDAGFiles
    ####
    write_params = { 'data_name': FLAGS.data,
                     'localdir': local_folder,
                     'tag': tag }
    
    ####
    #### submitTriggerEff, submitTriggerCounts
    ####
    histos_params = { 'binedges_filename': binedges_filename,
                      'indir': _inputs,
                      'outdir': data_storage,
                      'localdir': local_folder,
                      'data': _data[FLAGS.data],
                      'mc_processes': _mc_processes[FLAGS.mc_process],
                      'triggers': FLAGS.triggers,
                      'channels': FLAGS.channels,
                      'variables': variables_join,
                      'tag': tag,
                      'subtag': subtag,
                      'intersection_str': intersection_str,
                      'nocut_dummy_str': nocut_dummy_str,
                      'debug': FLAGS.debug_workflow
                     }

    ####
    #### haddHisto
    ####
    haddhisto_params = luigi.DictParameter(
        default={ 'indir': data_storage,
                  'localdir': local_folder,
                  'tag': tag,
                  'subtag': subtag} )

    ####
    #### drawTriggerScaleFactors
    ####
    _selected_mc_processes = _mc_processes[FLAGS.mc_process]
    _selected_data = _data[FLAGS.data]
    
    drawsf_params = luigi.DictParameter(
        default={ 'data_name': FLAGS.data,
                  'mc_name': FLAGS.mc_process,
                  'data': _selected_data,
                  'mc_processes': _selected_mc_processes,
                  'draw_independent_MCs': False,
                  'indir': data_storage,
                  'outdir': out_storage,
                  'localdir': local_folder,
                  'triggers': FLAGS.triggers,
                  'channels': FLAGS.channels,
                  'variables': FLAGS.variables_for_efficiencies,
                  'binedges_filename': binedges_filename,
                  'tag': tag,
                  'subtag': subtag,
                  'intersection_str': intersection_str,
                  'nocut_dummy_str': nocut_dummy_str,
                  'debug': FLAGS.debug_workflow,} )
    
    ####
    #### drawDistributions
    ####
    # _selected_mc_processes =_mc_processes[FLAGS.mc_process]
    # _selected_data = _data[FLAGS.data]
    
    # drawdist_params = luigi.DictParameter(
    #     default={ 'data_name': FLAGS.data,
    #               'mc_name': FLAGS.mc_process,
    #               'data': _selected_data,
    #               'mc_processes': _selected_mc_processes,
    #               'draw_independent_MCs': False,
    #               'indir': data_storage,
    #               'outdir': out_storage,
    #               'triggers': FLAGS.triggers,
    #               'channels': FLAGS.channels,
    #               'variables': FLAGS.variables_for_distributions,
    #               'binedges_filename': binedges_filename,
    #               'tag': tag,
    #               'subtag': subtag,
    #               'debug': FLAGS.debug_workflow,} )


    ####
    #### drawCounts
    ####
    # _selected_mc_processes = _mc_processes[FLAGS.mc_process]
    # _selected_data = _data[FLAGS.data]
    
    # drawcounts_params = luigi.DictParameter(
    #     default={ 'data_name': FLAGS.data,
    #               'mc_name': FLAGS.mc_process,
    #               'data': _selected_data,
    #               'mc_processes': _selected_mc_processes,
    #               'indir': data_storage,
    #               'outdir': out_storage,
    #               'triggers': FLAGS.triggers,
    #               'channels': FLAGS.channels,
    #               'variables': FLAGS.variables_for_distributions,
    #               'binedges_filename': binedges_filename,
    #               'tag': tag,
    #               'subtag': subtag,
    #               'debug': FLAGS.debug_workflow,} )

    ####
    #### variableImportanceDiscriminator
    ####
    discriminator_params = luigi.DictParameter(
        default={ 'data_name': FLAGS.data,
                  'mc_name': FLAGS.mc_process,
                  'indir': data_storage,
                  'outdir': data_storage,
                  'localdir': local_folder,
                  'triggers': FLAGS.triggers,
                  'channels': FLAGS.channels,
                  'variables': FLAGS.variables_for_efficiencies,
                  'tag': tag,
                  'subtag': subtag,
                  'intersection_str': intersection_str,
                  'debug': FLAGS.debug_workflow,} )

    ####
    #### scale factor calculator
    ####
    calculator_params = luigi.DictParameter(
        default={ 'binedges_filename': binedges_filename,
                  'indir_root': _inputs,
                  'indir_json': data_storage,
                  'indir_eff': out_storage,
                  'outdir': data_storage,
                  'outprefix': _closure_prefix,
                  'data_name': FLAGS.data,
                  'mc_name': FLAGS.mc_process,
                  'mc_processes': _mc_processes[FLAGS.mc_process],
                  'localdir': local_folder,
                  'triggers': FLAGS.triggers,
                  'channels': FLAGS.channels,
                  'variables': FLAGS.variables_for_efficiencies,
                  'tag': tag,
                  'subtag': subtag,
                  'debug': FLAGS.debug_workflow,} )


    ####
    #### haddEff
    ####
    # haddeff_params = luigi.DictParameter(
    #     default={ 'indir': data_storage,
    #               'localdir': local_folder,
    #               'outprefix': _outprefix,
    #               'tag': tag,
    #               'subtag': subtag} )

    ####
    #### draw single efficiencies closure
    ####
    closure_params = luigi.DictParameter(
        default={ 'binedges_filename': binedges_filename,
                  'indir_ref': data_storage,
                  'indir_union': data_storage,
                  'outdir': data_storage,
                  'inprefix': _closure_prefix,
                  'mc_processes': _mc_processes[FLAGS.mc_process],
                  'out_weighted_prefix': _closure_prefix,
                  'out_original_prefix': modes['histos'],
                  'localdir': local_folder,
                  'triggers': FLAGS.triggers,
                  'channels': FLAGS.channels,
                  'variables': FLAGS.variables_for_efficiencies,
                  'tag': tag,
                  'subtag': subtag,
                  'debug': FLAGS.debug_workflow } )

"""
'pass_triggerbit' leaf

data:
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
14 - HLT_PFMETNoMu120_PFMHTNoMu120_IDTight_v
15 - HLT_MediumChargedIsoPFTau50_Trk30_eta2p1_1pr_MET100_v
16 - HLT_MediumChargedIsoPFTau180HighPtRelaxedIso_Trk50_eta2p1_v

MC:
0 - HLT_IsoMu24_v
1 - HLT_IsoMu27_v
2 - HLT_Ele32_WPTight_Gsf_v
3 - HLT_Ele35_WPTight_Gsf_v
4 - HLT_DoubleMediumChargedIsoPFTauHPS35_Trk1_eta2p1_Reg_v
5 - HLT_IsoMu20_eta2p1_LooseChargedIsoPFTauHPS27_eta2p1_CrossL1_v
6 - HLT_Ele24_eta2p1_WPTight_Gsf_LooseChargedIsoPFTauHPS30_eta2p1_CrossL1_v
7 - HLT_VBF_DoubleLooseChargedIsoPFTau20_Trk1_eta2p1_v
8 - HLT_VBF_DoubleLooseChargedIsoPFTauHPS20_Trk1_eta2p1_v
9 - HLT_PFMETNoMu120_PFMHTNoMu120_IDTight_vgithgith
10 - HLT_MediumChargedIsoPFTau50_Trk30_eta2p1_1pr_MET100_v
11 - HLT_MediumChargedIsoPFTau180HighPtRelaxedIso_Trk50_eta2p1_v

Leg splitting:

4: one tau, one tau
5: one muon, one tau
6: one electron, one tau
7/8: remove (dedicated space region)
9: MET, MHT
10: remove
11: one tau






















'triggerbit' leaf

data and MC:
0 - HLT_IsoMu24_v
1 - HLT_IsoMu27_v
2 - HLT_Ele32_WPTight_Gsf_v
3 - HLT_Ele35_WPTight_Gsf_v
4 - HLT_MediumChargedIsoPFTau180HighPtRelaxedIso_Trk50_eta2p1_v
5 - HLT_MediumChargedIsoPFTau200HighPtRelaxedIso_Trk50_eta2p1_v
6 - HLT_MediumChargedIsoPFTau220HighPtRelaxedIso_Trk50_eta2p1_v
7 - HLT_MediumChargedIsoPFTau180HighPtRelaxedIso_Trk50_eta2p1_1pr_v
8 - HLT_IsoMu20_eta2p1_LooseChargedIsoPFTauHPS27_eta2p1_CrossL1_v
9 - HLT_IsoMu20_eta2p1_LooseChargedIsoPFTau27_eta2p1_CrossL1_v
10 - HLT_Ele24_eta2p1_WPTight_Gsf_LooseChargedIsoPFTauHPS30_eta2p1_CrossL1_v
11 - HLT_Ele24_eta2p1_WPTight_Gsf_LooseChargedIsoPFTau30_eta2p1_CrossL1_v
12 - HLT_DoubleMediumChargedIsoPFTauHPS35_Trk1_eta2p1_Reg_v
13 - HLT_DoubleMediumChargedIsoPFTau40_Trk1_TightID_eta2p1_Reg_v
14 - HLT_DoubleTightChargedIsoPFTau35_Trk1_TightID_eta2p1_Reg_v
15 - HLT_DoubleTightChargedIsoPFTau40_Trk1_eta2p1_Reg_v
16 - HLT_VBF_DoubleLooseChargedIsoPFTauHPS20_Trk1_eta2p1_v
17 - HLT_VBF_DoubleLooseChargedIsoPFTau20_Trk1_eta2p1_v
18 - HLT_MediumChargedIsoPFTau50_Trk30_eta2p1_1pr_MET90_v
19 - HLT_MediumChargedIsoPFTau50_Trk30_eta2p1_1pr_MET100_v
20 - HLT_MediumChargedIsoPFTau50_Trk30_eta2p1_1pr_MET110_v
21 - HLT_MediumChargedIsoPFTau50_Trk30_eta2p1_1pr_MET130_v
22 - HLT_Ele28_eta2p1_WPTight_Gsf_HT150_v
23 - HLT_Ele32_WPTight_Gsf_L1DoubleEG_v
24 - HLT_Diphoton30_18_R9IdL_AND_HE_AND_IsoCaloId_NoPixelVeto_v
25 - HLT_Ele50_CaloIdVT_GsfTrkIdT_PFJet165_v
26 - HLT_PFHT330PT30_QuadPFJet_75_60_45_40_TriplePFBTagDeepCSV_4p5_v
27 - HLT_Mu50_v
28 - HLT_TkMu100_v
29 - HLT_OldMu100_v
30 - HLT_MonoCentralPFJet80_PFMETNoMu120_PFMHTNoMu120_IDTight_v
31 - HLT_Mu17_TrkIsoVVL_Mu8_TrkIsoVVL_DZ_Mass3p8_v
32 - HLT_Mu17_Photon30_IsoCaloId_v
33 - HLT_DoubleMu4_Mass3p8_DZ_PFHT350_v
34 - HLT_DoubleMu3_DCA_PFMET50_PFMHT60_v
35 - HLT_AK8PFJet330_TrimMass30_PFAK8BoostedDoubleB_np4_v
36 - HLT_QuadPFJet103_88_75_15_DoublePFBTagDeepCSV_1p3_7p7_VBF1_v
37 - HLT_Photon35_TwoProngs35_v
38 - HLT_PFHT500_PFMET100_PFMHT100_IDTight_v
39 - HLT_AK8PFJet400_TrimMass30_v
40 - HLT_PFMETNoMu120_PFMHTNoMu120_IDTight_v
41 - HLT_PFMETNoMu120_PFMHTNoMu120_IDTight_PFHT60_v

all remaining bits are defined as zero.
"""

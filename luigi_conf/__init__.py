"""
Configuration file for the Luigi trigger scale factors framework.
Sanity checks included.
"""
_extensions = ( 'png', 'pdf',
                #'C'
               )
_channels = ( 'all', 'etau', 'mutau', 'tautau', 'mumu' )
_sel = { 'all':    {'pairType': ('<',  3),},
         'mutau':  {'pairType': ('==', 0),},
         'etau':   {'pairType': ('==', 1),},
         'tautau': {'pairType': ('==', 2),},
         'mumu':   {'pairType': ('==', 3),}, # passMu missing for the mumu channel
         'ee':     {'pairType': ('==', 4),} }

#######################################################################################################
########### VARIABLES ##################################################################################
#######################################################################################################
# variables considered for calculating and plotting efficiencies
_variables_eff = ['HT20', 'met_et', 'mht_et', 'metnomu_et', 'mhtnomu_et',
                  'dau1_pt', 'dau2_pt', 'dau1_eta', 'dau2_eta']
# variables considered for plotting MC/data comparison distributions
_variables_dist = ['dau1_pt', 'HH_mass']
# joining the two lists above
_variables_join = set(_variables_eff + _variables_dist)

#######################################################################################################
########### TRIGGERS ##################################################################################
#######################################################################################################
_nonStandTriggers = ['IsoMuIsoTau', 'METNoMu120', ] #example for custom trigger combination, currently meaningless
_trigger_custom = lambda x : {'mc': _nonStandTriggers, 'data': _nonStandTriggers}
_trigger_shift = lambda x : {'mc': x, 'data': x+5}
_triggers_map = {'nonStandard': _trigger_custom('nonStandard'),
                 #others: 0-3 map directly, 4 maps to 7
                 'IsoMuIsoTau': {'mc': 5, 'data': 8},
                 'EleIsoTau':   {'mc': 6, 'data': 10},
                 'VBFTau':     _trigger_shift(7),
                 'VBFTauHPS':  _trigger_shift(8),
                 'METNoMu120': _trigger_shift(9),
                 'IsoTau50':   _trigger_shift(10),
                 'IsoTau180':  _trigger_shift(11), }
assert( set(_nonStandTriggers).issubset( set(_triggers_map.keys()) ) )

#######################################################################################################
########### CUTS ######################################################################################
#######################################################################################################
_cuts = {#'METNoMu120': {'metnomu_et': ('>', [120,140,160,180,200]), 'mhtnomu_et': ('>', [120,140,160,180,200])},
    'METNoMu120': {'met_et': ('>', [120,140,160,180,200]), 'mht_et': ('>', [120,140,160,180,200])},
    'IsoTau50':   {'dau1_pt': ('>', [80]), 'dau1_eta': ('<', [2.0]), 'met_et': ('>', [150])},
}
assert( set(_cuts.keys()).issubset(set(_triggers_map.keys())) )
for x in _cuts.values():
    assert( set(x.keys()).issubset(set(_variables_eff)) )
_cuts_ignored = { 'HT20':       [],
                  'met_et':     ['metnomu_et',],
                  'mht_et':     ['mhtnomu_et',],
                  'metnomu_et': ['met_et',],
                  'mhtnomu_et': ['mht_et',],
                  'dau1_pt':    [],
                  'dau2_pt':    [],
                 }
assert( set(_cuts_ignored.keys()).issubset(_variables_join) )
for x in _cuts_ignored.values():
    assert( set(x).issubset(_variables_join) )
for k,v in _cuts_ignored.items():
    if k in v:
        raise ValueError('[configuration, var={}] It is redundant to specify the same variable: cuts are never applied to variables being displayed. Remove it.'.format(k))

#######################################################################################################
########### 2D PLOTS ##################################################################################
#######################################################################################################
_2Dpairs = (('metnomu_et', 'mhtnomu_et'),)
for x in _2Dpairs():
    for val in x:
        assert( val in _variables_eff )

#######################################################################################################
########### BINNING ###################################################################################
#######################################################################################################
_binedges = {} #Example: {'met_et': {'mumu': [100,200,300,400,500,600]},}
assert( set(_binedges.keys()).issubset(_variables_join) )
for x in _binedges.values():
    assert( len(x) == len(list(_binedges.values())[0]) )

#######################################################################################################
########### DATA AND MC SAMPLES #######################################################################
#######################################################################################################
#_inputs = '/data_CMS/cms/portales/HHresonant_SKIMS/SKIMS_Radion_2018_fixedMETtriggers_mht_16Jun2021/'
_inputs = [ '/data_CMS/cms/portales/HHresonant_SKIMS/SKIMS_2018_UL_data_test11Jan22/', #data
            '/data_CMS/cms/portales/HHresonant_SKIMS/SKIMS_2018_UL_signal_test11Jan22/', #MC signal
            '/data_CMS/cms/portales/HHresonant_SKIMS/SKIMS_2018_UL_backgrounds_test11Jan22/', #MC backgrounds
]

# names of the subfolders under '_inputs' above
_data = dict( MET2018 = ['SKIM_MET2018',] )
_mc_processes = dict( ggfRadions = [],
                      ggfBulkGraviton = [],
                      vbfRadion = [],
                      vbfBulkRadion = [],
                      SingleMuon = [],
                      TT =  ['SKIM_TT_fullyHad',
                             'SKIM_TT_fullyLep',
                             'SKIM_TT_semiLep',],
                      DY = [],
                     )

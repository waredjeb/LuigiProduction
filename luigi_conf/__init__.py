"""
Configuration file for the Luigi trigger scale factors framework.
Some sanity checks included.
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
_variables_eff = ['HT20', 'met_et', 'mht_et', 'metnomu_et', 'mhtnomu_et', 'dau1_pt', 'dau2_pt']
# variables considered for plotting MC/data comparison distributions
_variables_dist = ['dau1_pt', 'HH_mass']
# joining the two lists above
_variables_join = set(_variables_eff + _variables_dist)

#######################################################################################################
########### TRIGGERS ##################################################################################
#######################################################################################################
_nonStandTriggers = ['HT500', 'METNoMu120', 'METNoMu120_HT60', 'MediumMET100', 'MediumMET110', 'MediumMET130']
_trigger_custom = lambda x : {'mc': _nonStandTriggers, 'data': _nonStandTriggers}
_trigger_shift = lambda x : {'mc': x, 'data': x+5}
_triggers_map = {'nonStandard': _trigger_custom('nonStandard'), #>=9
                 'HT500': _trigger_shift(9),
                 'METNoMu120': _trigger_shift(10),
                 'METNoMu120_HT60': _trigger_shift(11),
                 'MediumMET100': _trigger_shift(12),
                 'MediumMET110': _trigger_shift(13),
                 'MediumMET130': _trigger_shift(14) }
assert( set(_nonStandTriggers).issubset( set(_triggers_map.keys()) ) )

#######################################################################################################
########### CUTS ######################################################################################
#######################################################################################################
_cuts = {'METNoMu120':      {'metnomu_et': ('>', 200), 'mhtnomu_et': ('>', 200)},
         'METNoMu120_HT60': {'metnomu_et': ('>', 200), 'mhtnomu_et': ('>', 200), 'HT20': ('>', 80)}
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
_2Dpairs = {'METNoMu120':      (('metnomu_et', 'mhtnomu_et'),),
            'METNoMu120_HT60': (('metnomu_et', 'mhtnomu_et'),),
         }
assert( set(_2Dpairs.keys()).issubset(set(_triggers_map.keys())) )
for x in _2Dpairs.values():
    for pair in x:
        assert( pair[0] in _variables_eff and pair[1] in _variables_eff )

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
_data = dict( MET2018 = ['MET2018A',
                         'MET2018B',
                         'MET2018C',
                         'MET2018D',] )
_mc_processes = dict( Radions = ['Radion_m300',
                                 'Radion_m400',
                                 'Radion_m500',
                                 'Radion_m600',
                                 'Radion_m700',
                                 'Radion_m800',
                                 'Radion_m900',],
                      
                      SingleMuon = ['SingleMuon2018',
                                    'SingleMuon2018A',
                                    'SingleMuon2018B',
                                    'SingleMuon2018C',
                                    'SingleMuon2018D'],
                      
                      TT =         ['TT_fullyHad',
                                    'TT_fullyLep',
                                    'TT_semiLep',],
                      
                      DY =         ['DY',
                                    ],
                     )

_nonStandTriggers = ['HT500', 'METNoMu120', 'METNoMu200cut', 'MediumMET100', 'MediumMET110' 'MediumMET130']
_trigger_custom = lambda x : {'mc': _nonStandTriggers, 'data': _nonStandTriggers}
_trigger_shift = lambda x : {'mc': x, 'data': x+5}
_triggers_map = {'nonStandard': _trigger_custom('nonStandard'), #>=9
                 'HT500': _trigger_shift(9),
                 'METNoMu120': _trigger_shift(10),
                 'METNoMu120_HT60': _trigger_shift(11),
                 'MediumMET100': _trigger_shift(12),
                 'MediumMET110': _trigger_shift(13),
                 'MediumMET130': _trigger_shift(14) }

_variables = ['met_et', 'HT20', 'mht_et', 'metnomu_et', 'mhtnomu_et', 'dau1_pt', 'dau2_pt']
_channels = ( 'all', 'etau', 'mutau', 'tautau', 'mumu' )
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
                                    'DYall',
                                    'DY_lowMass',],
                     )


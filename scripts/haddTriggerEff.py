import os
import sys
from utils import utils

@utils.set_pure_input_namespace
def haddTriggerEff_outputs(args):
    target = os.path.join( args.indir , args.targetsPrefix + args.data_target + '.' + args.tag + '.root' )
    return [target]

@utils.set_pure_input_namespace
def haddTriggerEff(args):
    """
    adds the ROOT histograms produced in the preceding step
    """
    #dir_in = '/data_CMS/cms/portales/HHresonant_SKIMS/SKIMS_Radion_2018_fixedMETtriggers_mht_16Jun2021/output_trigEffBkg_TTCR_fixedtrig/'
    dir_in = '/data_CMS/cms/alves/FRAMEWORKTEST/'

    tag = 'metnomu200cut'
    processes = [
        #'DY',
        #'DY_lowMass',

        #'TT_fullyHad',
        #'TT_fullyLep',
        #'TT_semiLep',

        'Radion_m300',
        'Radion_m400',
        'Radion_m500',
        'Radion_m600',
        'Radion_m700',
        'Radion_m800',
        'Radion_m900',

        #'SingleMuon2018A',
        #'SingleMuon2018B',
        #'SingleMuon2018C',
        #'SingleMuon2018D',

        #'MET2018A',
        #'MET2018B',
        #'MET2018C',
        #'MET2018D',
    ]

    target = haddTriggerEff_outputs(args)
    command = 'hadd -f'
    inputs = ''
    for smpl in args.samples:
        inputs += os.path.join(dir_in, smpl + '/*' + args.tag + '.root ')

    # This likely has to be adapted if there is more than one target
    for t in target:
        os.system( command + ' ' + t + ' ' + inputs )

import os
import sys
from utils import set_pure_input_namespace

@set_pure_input_namespace
def haddTriggerEff_outputs(args):
    targets = []

    for proc in args.processes:
        targets.append( os.path.join( args.inDir , args.targetsPrefix + proc + '.' + args.tag + '.root' ) )
    return targets

@set_pure_input_namespace
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

    for target, proc in zip(haddTriggerEff_outputs(args), args.processes):
        command = 'hadd -f'
        inputs = os.path.join( dir_in, proc + '/*' + args.tag + '.root' )
        os.system( command + ' ' + target + ' ' + inputs )

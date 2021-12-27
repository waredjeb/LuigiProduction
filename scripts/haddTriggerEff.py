import os
import sys
from utils import utils

@utils.set_pure_input_namespace
def haddTriggerEff_outputs(args):
    targets = []

    # add the merg eof all the samples first
    tbase = args.targetsPrefix + args.dataset_name + args.target_suffix + args.subtag
    t = os.path.join( args.indir, tbase + '.root' )
    targets.append( t )

    # add individual sample merges
    for smpl in args.samples:
        tbase = args.targetsPrefix + smpl + args.target_suffix + args.subtag
        t = os.path.join( args.indir, tbase + '.root' )
        targets.append( t )
        
    print(targets)
    print('BREAK HADD!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
    quit()

    return targets

@utils.set_pure_input_namespace
def haddTriggerEff(args):
    """Adds ROOT histograms"""
    targets = haddTriggerEff_outputs(args)
    
    _command = 'hadd -f'
    for t,smpl in zip(targets[:-1], args.samples):
        inputs = os.path.join(args.indir, smpl + '/*' + args.subtag + '.root ')
        full_command = _command + ' ' + t + ' ' + inputs
        os.system( full_command )

        
    # This likely has to be adapted if there is more than one target
    for t in target:
        comm = command + ' ' + t + ' ' + inputs
        os.system( comm )


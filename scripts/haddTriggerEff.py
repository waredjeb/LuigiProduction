import os
import sys
from utils import utils

def _subpaths(args):
    _tbase1 = args.targetsPrefix + args.dataset_name
    _tbase2 = args.target_suffix + args.subtag
    return _tbase1, _tbase2

@utils.setPureInputNamespace
def haddTriggerEff_outputs(args):
    targets = []

    # add the merg eof all the samples first
    _tbase1, _tbase2 = _subpaths(args)
    tbase = _tbase1 + _tbase2
    t = os.path.join( args.indir, tbase + '.root' )
    targets.append( t )

    # add individual sample merges
    for smpl in args.samples:
        tbase = _tbase1 + '_' + smpl + _tbase2
        t = os.path.join( args.indir, tbase + '.root' )
        targets.append( t )
        
    return targets

@utils.setPureInputNamespace
def haddTriggerEff(args):
    """Adds ROOT histograms"""
    targets = haddTriggerEff_outputs(args)
    _tbase1, _tbase2 = _subpaths(args)

    inputs_join = []
    _command = 'hadd -f'
    for t,smpl in zip(targets[1:], args.samples):
        inputs = os.path.join(args.indir, smpl + '/*' + args.subtag + '.root ')
        inputs_join.append(t)
        full_command = _command + ' ' + t + ' ' + inputs
        os.system( full_command )

    # join MC or Data subdatasets into a single one
    full_command_join = _command + ' ' + targets[0] + ' ' + ' '.join(inputs_join)
    os.system( full_command_join )

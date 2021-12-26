import os
import sys
from utils import utils

@utils.set_pure_input_namespace
def haddTriggerEff_outputs(args):
    target = os.path.join( args.indir , args.targetsPrefix + args.dataset_name + args.target_suffix + args.subtag + '.root' )
    return [target]

@utils.set_pure_input_namespace
def haddTriggerEff(args):
    """
    adds the ROOT histograms produced in the preceding step
    """
    target = haddTriggerEff_outputs(args)
    
    command = 'hadd -f'
    inputs = ''
    for smpl in args.samples:
        inputs += os.path.join(args.indir,
                               smpl + '/*' + args.subtag + '.root ')

    # This likely has to be adapted if there is more than one target
    for t in target:
        comm = command + ' ' + t + ' ' + inputs
        os.system( comm )


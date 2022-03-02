import os
import glob
import re
import json
import argparse

import sys
sys.path.append( os.environ['PWD'] ) 

def discriminator(args, chn):
    """
    Associates each trigger combination to a set of variables, ordered by importance.
    The variables will be used to retrieve the corresponding trigger efficiencies when evaluating the scale factors.
    """
    result = {}

    triggercomb = generateTriggerCombinations(args.triggers)
    for tcomb in triggercomb:
        result[joinNTC(tcomb)] = [ 'dau1_pt' ] #CHANGE!!!!!!!!!!!!!!!!!!!

    return result
        
def discriminatorExecutor(args, chn):
    match = re.compile('')
    
    if args.debug:
        print('[=debug=] Open file: {}'.format(name_data))

    outputs = discriminatorExecutor_outputs(args, chn)
    
    orderedVars = discriminator(args, chn)
    with open(out, 'w') as f:
        json.dump(orderedVars, f)


parser = argparse.ArgumentParser(description='Choose the most significant variables to draw the efficiencies.')

parser.add_argument('--indir',  help='Inputs directory',  required=True)
parser.add_argument('--outdir', help='Outputs directory', required=True)
parser.add_argument('--data_name', dest='data_name', required=True, help='Data sample name')
parser.add_argument('--mc_name', dest='mc_name', required=True, help='MC sample name')
parser.add_argument('--triggers', dest='triggers', nargs='+', type=str,
                    required=True, help='Triggers included in the workfow.')
parser.add_argument('--channel',   dest='channel', required=True,
                    help='Select the channel over which the discrimination will be run.' )
parser.add_argument('--variables',   dest='variables', required=True, nargs='+', type=str,
                    help='Select the variables over which the workflow will be run.' )
parser.add_argument('-t', '--tag', help='string to diferentiate between different workflow runs', required=True)
parser.add_argument('--subtag', dest='subtag', required=True, help='subtag')
parser.add_argument('--debug', action='store_true', help='debug verbosity')
args = parser.parse_args()

discriminatorExecutor(args, args.channel)

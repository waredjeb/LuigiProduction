import os
import glob
import re
import json
import argparse

import sys
sys.path.append( os.environ['PWD'] ) 

from utils.utils import (
    generate_trigger_combinations,
    join_name_trigger_intersection as joinNTC,
    print_configuration,
)

def discriminator(args, chn):
    """
    Associates each trigger combination to a set of variables, ordered by importance.
    The variables will be used to retrieve the corresponding trigger efficiencies when evaluating the scale factors.
    """
    result = {}

    triggercomb = generate_trigger_combinations(args.triggers)
    constant_list = ['dau1_pt', 'dau1_eta', 'dau2_pt', 'dau2_eta']
    for tcomb in triggercomb:
        # CHANGE!!!!!!!!!!!!!!!!!!!
        result[joinNTC(tcomb)] = [ constant_list, #always the same 1D variables
                                   [], #1D changing variables
                                   [], #2D pairs of changing variables
                                  ]

    return result

def discriminatorExecutor_outputs(args, chn):
    out = os.path.join(args.outdir, '{}_{}.json'.format( os.path.basename(__file__).split('.')[0], chn))
    return out

def discriminatorExecutor(args, chn):
    match = re.compile('')
    
    if args.debug:
        print('[=debug=] Open file: {}'.format(name_data))

    out = discriminatorExecutor_outputs(args, chn)
    ordered_vars = discriminator(args, chn)

    with open(out, 'w') as f:
        json.dump(ordered_vars, f)


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
parser.add_argument('--tag', help='string to differentiate between different workflow runs', required=True)
parser.add_argument('--subtag', dest='subtag', required=True, help='subtag')
parser.add_argument('--debug', action='store_true', help='debug verbosity')
args = parser.parse_args()
print_configuration(args)

discriminatorExecutor(args, args.channel)

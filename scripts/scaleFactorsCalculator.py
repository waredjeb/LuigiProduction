import os
import glob
import re
import json
import argparse

import sys
sys.path.append( os.environ['PWD'] ) 
import ROOT
ROOT.gROOT.SetBatch(True)
from ROOT import TFile

from utils.utils import (
    generate_trigger_combinations,
    join_name_trigger_intersection as joinNTC,
)

def discriminator(args, chn):
    """
    Associates each trigger combination to a set of variables, ordered by importance.
    The variables will be used to retrieve the corresponding trigger efficiencies when evaluating the scale factors.
    """
    result = {}

    triggercomb = generate_trigger_combinations(args.triggers)
    for tcomb in triggercomb:
        result[joinNTC(tcomb)] = 'dau1_pt' #CHANGE!!!!!!!!!!!!!!!!!!!

        # inBaseName = ( 'trigSF_' + args.data_name + '_' + args.mc_name + '_' +
        #                chn + '_' + var + '_' + joinNTC(tcomb) + args.subtag + '_CUTS*.root' )
        # inName = os.path.join(args.indir, chn, var, inBaseName)
        # inName = glob.glob(inName)

        # if len(inName) != 0:
        #     inName = inName[0] # same decision for all cuts
        #     inFile = TFile.Open(inName, 'READ')
        #     keyList = ROOT.TIter(inFile.GetListOfKeys())

        # for key in keyList:
        #     cl = ROOT.gROOT.GetClass(key.GetClassName())
        #     if not cl.InheritsFrom("TGraph"):
        #         continue
        #     h = key.ReadObj()

        #     for datapoint in range(h.GetN()):
        #         print(h.GetPointX(datapoint), h.GetPointY(datapoint))
        #         print(h.GetErrorXlow(datapoint), h.GetErrorXhigh(datapoint))
        #         print(h.GetErrorYlow(datapoint), h.GetErrorYhigh(datapoint))
        #         print()

    return result

def discriminatorExecutor_outputs(args):
    return [ os.path.join(args.outdir, '{}_{}.json'.format(os.path.basename(__file__), chn))
             for chn in args.channels ]
        
def discriminatorExecutor(args):
    match = re.compile('')
    
    if args.debug:
        print('[=debug=] Open file: {}'.format(name_data))

    outputs = discriminatorExecutor_outputs(args)
    assert(len(outputs) == len(args.channels))
    
    for i,chn in enumerate(args.channels):
        # initialize discriminator related variables
        # obtain vars ordered by relative importance + metric (variance for instance)
        
        orderedVars = discriminator(args, chn)
        # with open(outputs[i], 'w') as f:
        #     json.dump(orderedVars, f)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Choose the most significant variables to draw the efficiencies.')

    parser.add_argument('--indir',  help='Inputs directory',  required=True)
    parser.add_argument('--outdir', help='Outputs directory', required=True)
    parser.add_argument('--data_name', dest='data_name', required=True, help='Data sample name')
    parser.add_argument('--mc_name', dest='mc_name', required=True, help='MC sample name')
    parser.add_argument('--triggers', dest='triggers', nargs='+', type=str,
                        required=True, help='Triggers included in the workfow.')
    parser.add_argument('--channels',   dest='channels',         required=True, nargs='+', type=str,
                        help='Select the channels over which the workflow will be run.' )
    parser.add_argument('-t', '--tag', help='string to diferentiate between different workflow runs', required=True)
    parser.add_argument('--subtag', dest='subtag', required=True, help='subtag')
    parser.add_argument('--debug', action='store_true', help='debug verbosity')
    args = parser.parse_args()

    discriminatorExecutor(args)

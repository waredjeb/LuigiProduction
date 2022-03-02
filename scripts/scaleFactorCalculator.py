import os
import glob
import re
import numpy as np
import json
import argparse

import sys
sys.path.append( os.environ['PWD'] ) 
import ROOT
ROOT.gROOT.SetBatch(True)
from ROOT import TFile

from utils.utils import (
    generateTriggerCombinations,
    joinNameTriggerIntersection as joinNTC,
)

def effExtractor(args, chn, dvars, nbins):
    """
    Extracts the efficiencies for data and MC to be used as scale factors: e_data / e_MC.
    Returns a dictionary with al efficiencies.
    """
    efficiencies_data, efficiencies_data_ehigh, efficiencies_data_elow = ({} for _ in range(3))
    efficiencies_MC, efficiencies_MC_ehigh, efficiencies_MC_elow = ({} for _ in range(3)) 
    
    triggercomb = generateTriggerCombinations(args.triggers)
    for tcomb in triggercomb:
        variables = dvars[ joinNTC(tcomb) ]
        assert(len(variables)==1)
        var = variables[0]
        
        inBaseName = ( 'trigSF_' + args.data_name + '_' + args.mc_name + '_' +
                       chn + '_' + var + '_' + joinNTC(tcomb) + args.subtag + '_CUTS*.root' )
        inName = os.path.join(args.indir, chn, var, inBaseName)
        inName = min( glob.glob(inName), key=len) #select the shortest string (NoCut)

        if len(inName) != 0:
            efficiencies_data[joinNTC(tcomb)] = []
            efficiencies_MC[joinNTC(tcomb)] = []
            
            inName = inName # same decision for all cuts
            inFile = TFile.Open(inName, 'READ')
            keyList = ROOT.TIter(inFile.GetListOfKeys())

            for key in keyList:
                print(key)
                
                cl = ROOT.gROOT.GetClass(key.GetClassName())
                if not cl.InheritsFrom("TGraph"):
                    continue
                h = key.ReadObj()

                assert(nbins[var][chn] == h.GetN())
                for datapoint in range(h.GetN()):
                    efficiencies_data[joinNTC(tcomb)].append( h.GetPointY(datapoint) )
                    efficiencies_data_elow[joinNTC(tcomb)].append( h.GetErrorYlow(datapoint) )
                    efficiencies_data_ehigh[joinNTC(tcomb)].append( h.GetErrorYhigh(datapoint) )

    return ( (efficiencies_data, efficiencies_data_ehigh, efficiencies_data_elow),
             (efficiencies_MC,   efficiencies_MC_ehigh,   efficiencies_MC_elow) )

def find_bin(edges, value):
    return np.digitize(value, edges)

def effCalculator(args, efficiencies, eventvars, dvars, binedges):
    effData, effMC = (0 for _ in range(2))
    
    triggercomb = generateTriggerCombinations(args.triggers)
    for tcomb in triggercomb:
        variable = dvars[ joinNTC(triggercomb) ]
        assert(len(dvars[ joinNTC(triggercomb) ])==1)

        binId = find_bin(binedges[variable][chn], eventvars[variable])
        assert(binId!=0 and binId!=len(binedges[variable][chn])) #check for out-of-bounds
        
        termData = efficiencies[0][0][joinNTC(tcomb)][binId]
        termMC   = efficiencies[1][0][joinNTC(tcomb)][binId]

        ###CHANGE!!!!!!!!!!!!!!!!!! this is a simplification
        if len(tcomb) > 3:
            continue

        
        if len(tcomb)%2==0:
            effData -= termData
            effMC   -= termMC
        else:
            effData += termData
            effMC   += termMC

    return effData, effMC
            
def scaleFactorMain_outputs(args):
    return [ os.path.join(args.outdir, '{}_{}.json'.format(os.path.basename(__file__), chn))
             for chn in args.channels ]
        
def scaleFactorMain(args):
    match = re.compile('')
    
    if args.debug:
        print('[=debug=] Open file: {}'.format(name_data))

    outputs = scaleFactorMain_outputs(args)
    assert(len(outputs) == len(args.channels))

    binedges, nbins = loadBinning(afile=args.binedges_filename, key=args.subtag,
                                  variables=args.variables, channels=args.channels)

    for i,chn in enumerate(args.channels):
        # initialize discriminator related variables
        # obtain vars ordered by relative importance + metric (variance for instance)
        jsonFileName = os.path.join( args.indir, 'variableImportanceDiscriminator_{}.json'.format(chn) )
        varDict = json.load(jsonFileName)

        efficiencies = effExtractor(args, chn, varDict, nbins)

        eventVars = ...
        effData, effMC = effCalculator(args, efficiencies, eventVars, varDict, binedges)
        
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

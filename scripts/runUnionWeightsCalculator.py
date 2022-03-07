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
    loadBinning,
)

from luigi_conf import (
    _variables_unionweights,
)

def meanbins(m1,m2,nelem):
    arr = np.linspace(m1, m2, nelem)
    return (arr[:-1]+arr[1:])/2

def effExtractor(args, chn, dvars, nbins):
    """
    Extracts the efficiencies for data and MC to be used as scale factors: e_data / e_MC.
    Returns a dictionary with al efficiencies.
    """
    efficiencies_data, efficiencies_data_ehigh, efficiencies_data_elow = ({} for _ in range(3))
    efficiencies_MC, efficiencies_MC_ehigh, efficiencies_MC_elow = ({} for _ in range(3)) 
    
    triggercomb = generateTriggerCombinations(args.triggers)
    for tcomb in triggercomb:
        comb_vars = dvars[ joinNTC(tcomb) ]
        assert len(comb_vars)==2
        var = comb_vars[0]
        
        inBaseName = ( 'trigSF_' + args.data_name + '_' + args.mc_name + '_' +
                       chn + '_' + var + '_' + joinNTC(tcomb) + args.subtag + '_CUTS*.root' )
        inName = os.path.join(args.indir, chn, var, inBaseName)
        globName = glob.glob(inName)

        if len(globName) != 0: #some triggers do not fire for some channels: Ele32 for mutau (for example)
            efficiencies_data[joinNTC(tcomb)] = []
            efficiencies_MC[joinNTC(tcomb)] = []

            inFileName = min(globName, key=len) #select the shortest string (NoCut)            
            inFile = TFile.Open(inFileName, 'READ')
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

def findBin(edges, value):
    """Find the bin id corresponding to one value, given the bin edges."""
    return np.digitize(value, edges)

def effCalculator(args, efficiencies, eventvars, channel, dvars, binedges):
    eff_data, eff_mc = (0 for _ in range(2))

    triggercomb = generateTriggerCombinations(args.triggers)
    for tcomb in triggercomb:
        joincomb = joinNTC(tcomb)

        if joincomb in efficiencies[1][0] and joincomb not in efficiencies[0][0]:
            raise ValueError('This should never happen. Cannot be in MC but not in data.')
        
        #some triggers do not fire for some channels: Ele32 for mutau (for example)
        if joincomb in efficiencies[0][0]:

            variables = dvars[joincomb] 
            assert len(variables) == 2 #Change according to the discriminator

            binid = findBin(binedges[variables[0]][channel], 40.) #SHOULD DEPEND ON EVENTVARS
            # check for out-of-bounds
            assert binid!=0 and binid!=len(binedges[variables[0]][channel])

            term_data = efficiencies[0][0][joinNTC(tcomb)][binid]
            term_mc   = efficiencies[1][0][joinNTC(tcomb)][binid]

            if len(tcomb)%2==0:
                eff_data -= term_data
                ef_mc    -= term_mc
            else:
                eff_data += term_data
                eff_mc   += term_mc

    return eff_data, eff_mc

def runUnionWeightsCalculator_outputs(args, chn):
    outputs = []
    for v in _variables_unionweights:
        for ext in _extensions:
            name = '{}_{}_{}_VS_{}.{}'.format(os.path.basename(__file__),
                                              chn, v[0], v[1]. ext)
            if ext == 'root':
                outputs.append( os.path.join(args.outdir_root, name) )
            else:
                outputs.append( os.path.join(args.outdir_plots, chn, name) )
            
    return outputs

def runUnionWeightsCalculator(args, chn):
    outputs = runUnionWeightsCalculator_outputs(args, chn)
    unite_vars = lambda x,y: x+y
    binedges, nbins = loadBinning(afile=args.binedges_fname, key=args.subtag,
                                  variables=args.variables, channels=[chn])
    nbins_union = (20, 20)

    # initialize histograms
    hUnionWeights = {}
    for var1,var2 in _variables_unionweights:
        var1_low, var1_high = binedges[var1][chn][0], binedges[var1][chn][-1]
        var2_low, var2_high = binedges[var2][chn][0], binedges[var2][chn][-1]
        hUnionWeights[unite_vars(var1,var2)] = ROOT.TH1D( unite_vars(var1,var2),
                                                          unite_vars(var1,var2),
                                                          nbins_union[0], var1_low, var1_low,
                                                          nbins_union[1], var2_low, var2_low )

    efficiencies = effExtractor(args, chn, dvar, nbins)

    json_fname = os.path.join( args.indir, 'runVariableImportanceDiscriminator_{}.json'.format(chn) )
    with open(json_fname, 'r') as f:
        dvar = json.load(f)
    
    lf = LeafManager( fname, t_in )
    for entry in range(0,t_in.GetEntries()):
        t_in.GetEntry(entry)

        mhh = lf.getLeaf( 'HHKin_mass' )
        if mhh<1:
            continue

        pairtype = lf.getLeaf( 'pairType' )
        dau1_eleiso = lf.getLeaf( 'dau1_eleMVAiso'    )
        dau1_muiso  = lf.getLeaf( 'dau1_iso'          )
        dau1_tauiso = lf.getLeaf( 'dau1_deepTauVsJet' )
        dau2_tauiso = lf.getLeaf( 'dau2_deepTauVsJet' )
        
        if pairtype==1 and (dau1_eleiso!=1 or dau2_tauiso<5):
            continue
        if pairtype==0 and (dau1_muiso>=0.15 or dau2_tauiso<5):
            continue
        if pairtype==2 and (dau1_tauiso<5 or dau2_tauiso<5): # Loose / Medium / Tight
            continue

        #((tauH_SVFIT_mass-116.)*(tauH_SVFIT_mass-116.))/(35.*35.) + ((bH_mass_raw-111.)*(bH_mass_raw-111.))/(45.*45.) <  1.0
        svfit_mass = lf.getLeaf('tauH_SVFIT_mass')
        bH_mass    = lf.getLeaf('bH_mass_raw')

        mcut = ((svfit_mass-129.)*(svfit_mass-129.))/(53.*53.) + ((bH_mass-169.)*(bH_mass-169.))/(145.*145.) <  1.0
        if mcut: # inverted elliptical mass cut (-> ttCR)
            continue

        passLEP = lf.getLeaf('isLeptrigger')
        if isChannelConsistent(i, pairtype) and passLEP:

            for var1,var2 in _variables_unionweights:
                hUnionWeights[unite_vars(var1,var2)].Fill( ... )

            vars1 = meanbins(var1_low, var1_high, nbins_union+1)
            vars2 = meanbins(var2_low, var2_high, nbins_union+1)
            for iv1 in vars1:
                for iv2 in vars2:
                    eventvars = (iv1, iv2)
                    effData, effMC = effCalculator(args, efficiencies, eventvars,
                                                   chn, dvar, binedges)



    print('Weights calculated for channel {}.'.format(chn))    
    # with open(outputs[i], 'w') as f:
    #     json.dump(orderedVars, f)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Choose the most significant variables to draw the efficiencies.')

    parser.add_argument('--binedges_fname', dest='binedges_fname', required=True, help='where the bin edges are stored')
    parser.add_argument('--indir',  help='Inputs directory',  required=True)
    parser.add_argument('--outdir_plots', help='Output plots directory', required=True)
    parser.add_argument('--outdir_root', help='Output directory for ROOT files', required=True)
    parser.add_argument('--data_name', dest='data_name', required=True, help='Data sample name')
    parser.add_argument('--mc_name', dest='mc_name', required=True, help='MC sample name')
    parser.add_argument('--triggers', dest='triggers', nargs='+', type=str,
                        required=True, help='Triggers included in the workfow.')
    parser.add_argument('--channel', dest='channel', required=True,
                        help='Select the channels over which the workflow will be run.' )
    parser.add_argument('--variables',        dest='variables',        required=True,
                        nargs='+', type=str,
                        help='Workflow variables considered.')
    parser.add_argument('-t', '--tag', help='string to differentiate between different workflow runs', required=True)
    parser.add_argument('--subtag', dest='subtag', required=True, help='subtag')
    parser.add_argument('--debug', action='store_true', help='debug verbosity')
    args = parser.parse_args()

    runUnionWeightsCalculator(args, args.channel)

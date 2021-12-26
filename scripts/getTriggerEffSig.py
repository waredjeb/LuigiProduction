
"""
Script which calculates the trigger scale factors.
On production mode should run in the grid via scripts/submitTriggerEff.py. 
Local run example:
python3 -m scripts.getTriggerEffSig
--indir /data_CMS/cms/portales/HHresonant_SKIMS/SKIMS_Radion_2018_fixedMETtriggers_mht_16Jun2021/
--outdir .
--sample MET2018A
--isData 1
--triggers METNoMu120 METNoMu120_HT60 HT500
--variables met_et HT20 mht_et metnomu_et mhtnomu_et
--channels mutau
--subtag _default
--tprefix hist_eff_
--file output_0.root
--binedges /data_CMS/cms/alves/TriggerScaleFactors/<your tag>/binedges.hdf5
--debug

TODO: - Rewrite the nested loops so that the trigger loop
        comes before the variable loop
"""

import re
import os
import sys
import functools
import argparse
import fnmatch
import math
from array import array
import numpy as np
import ROOT
import h5py

import sys
sys.path.append(os.path.join(os.environ['CMSSW_BASE'], 'src', 'METTriggerStudies'))
print(sys.path[-1])
from utils.utils import getTriggerBit, LeafManager

from luigi_conf import _cuts, _cuts_ignored, _2Dpairs, _sel

def checkBit(number, bitpos):
    bitdigit = 1
    res = bool(number&(bitdigit<<bitpos))
    return res

def isChannelConsistent(chn, passMu, pairtype):
    return ( ( chn=='all'    and pairtype<_sel['all']['pairType'][1]  ) or
             ( chn=='mutau'  and pairtype==_sel['mutau']['pairType'][1] ) or
             ( chn=='etau'   and pairtype==_sel['etau']['pairType'][1] )  or
             ( chn=='tautau' and pairtype==_sel['tautau']['pairType'][1] ) or
             ( chn=='mumu'   and pairtype==_sel['mumu']['pairType'][1] ) or
             ( chn=='ee'     and pairtype==_sel['ee']['pairType'][1] ) )

def passesCut(trig, variables, leavesmanager, debug):
    """
    Handles cuts on trigger variables.
    Works for both 1D and 2D efficiencies.
    """
    if debug:
        print('Trigger={}; Variables={}'.format(trig, variables))
    flag = True
    try:
        trig_cuts = _cuts[trig]
        for avar,acut in trig_cuts.items():

            # ignore cuts according to the user's definition in '_cuts_ignored'
            # example: do not cut on 'met_et' when displaying 'metnomu_et'
            ignore = functools.reduce( lambda x, y: x or y,
                                       [ avar in _cuts_ignored[k] for k in variables ]
                                      )

            # additionally, by default do not cut on the variable(s) being plotted
            if avar not in variables and not ignore:
                value = leavesmanager.getLeaf(avar) 
                if acut[0]=='>':
                    flag = flag and value > acut[1]
                    if debug:
                        print("Cut: {} > {}".format(avar, acut[1]))
                elif acut[0]=='<':
                    flag = flag and value < acut[1]
                    if debug:
                        print("Cut: {} < {}".format(avar, acut[1]))
                else:
                    raise ValueError("The operator for the cut is currently not supported: Use '>' or '<'.")

    # the trigger has no cut associated
    except KeyError: 
        if debug:
            print('KeyError')            
        flag = True

    return flag
    
def getTriggerEffSig(indir, outdir, sample, fileName,
                     channels, variables, triggers,
                     subtag, tprefix, isData, binedges_fname):
    # -- Check if outdir exists, if not create it
    if not os.path.exists(outdir):
        os.makedirs(outdir)
    if not os.path.exists( os.path.join(outdir, sample) ):
        os.makedirs( os.path.join(outdir, sample) )
    outdir = os.path.join(outdir, sample)

    fname = os.path.join(indir, 'SKIM_'+sample, fileName)
    if not os.path.exists(fname):
        raise ValueError('[' + os.path.basename(__file__) + '] The input files does not exist.')

    f_in = ROOT.TFile( fname )
    t_in = f_in.Get('HTauTauTree')

    fillVar = {}
    lf = LeafManager( fname, t_in )

    # Recover binning
    binedges, nbins = ({} for _ in range(2))
    with h5py.File(binedges_fname, 'r') as f:
        group = f[subtag]
        for var in variables:
            subgroup = group[var]
            binedges[var], nbins[var] = ({} for _ in range(2))
            for chn in args.channels:
                binedges[var][chn] = np.array(subgroup[chn][:])
                nbins[var][chn] = len(binedges[var][chn]) - 1

    # Define 1D histograms:
    #  hRef: pass the reference trigger
    #  hTrig: pass the reference trigger + trigger under study
    #  hNoRef: does not pass the reference trigger BUT passes the trigger under study
    hRef, hNoRef, hTrig= ({} for _ in range(3))

    for i in channels:
        hRef[i], hTrig[i], hNoRef[i] = ({} for _ in range(3))
        
        for j in variables:
            href_name = 'Ref_{}_{}'.format(i,j)
            hTrig[i][j]={}
            hNoRef[i][j] = {}

            binning = (nbins[j][i], binedges[j][i])
            hRef[i][j] = ROOT.TH1D(href_name,'', *binning)
            for k in triggers:
                htrig_name = 'Trig_{}_{}_{}'.format(i,j,k)
                hTrig[i][j][k] = ROOT.TH1D(htrig_name, '', *binning)
                hnoref_name = 'NoRef_{}_{}_{}'.format(i,j,k)
                hNoRef[i][j][k] = ROOT.TH1D(hnoref_name, '', *binning)

    # Define 2D efficiencies:
    #  effRefVsTrig: efficiency for passing the reference trigger
    effRefVsTrig, = ({} for _ in range(1))
    addVarNames = lambda var1,var2 : var1 + '_VERSUS_' + var2
    
    for i in channels:
        effRefVsTrig[i], = ({} for _ in range(1))                        

        for k in triggers:
            if k in _2Dpairs.keys():
                for j in _2Dpairs[k]:
                    vname = addVarNames(j[0],j[1])
                    if vname not in effRefVsTrig[i]: #creates subdictionary if it does not exist
                        effRefVsTrig[i][vname] = {}
                    
                    effRefVsTrig_name = 'effRefVsTrig_{}_{}_{}'.format(i,k,vname)
                    effRefVsTrig[i][vname][k] = ROOT.TEfficiency( effRefVsTrig_name, '',
                                                                  nbins[j[0]][i], binedges[j[0]][i],
                                                                  nbins[j[1]][i], binedges[j[1]][i] )
    
    
    for entry in range(0,t_in.GetEntries()):
        t_in.GetEntry(entry)

        pairtype = lf.getLeaf( 'pairType' )
        mhh = lf.getLeaf( 'HHKin_mass' )
        if mhh<1:
            continue
        #        print('mass ok')
        nleps      = lf.getLeaf( 'nleps'      )
        nbjetscand = lf.getLeaf( 'nbjetscand' )
        isOS       = lf.getLeaf( 'isOS'       )

        dau1_eleiso = lf.getLeaf( 'dau1_eleMVAiso'    )
        dau1_muiso  = lf.getLeaf( 'dau1_iso'          )
        dau1_tauiso = lf.getLeaf( 'dau1_deepTauVsJet' )
        dau2_tauiso = lf.getLeaf( 'dau2_deepTauVsJet' )
        
        if pairtype==1 and (dau1_eleiso!=1 or dau2_tauiso<5):
            continue
        if pairtype==0 and (dau1_muiso>=0.15 or dau2_tauiso<5):
            continue
        if pairtype==2 and (dau1_tauiso<5 or dau2_tauiso<5):
            continue

        #((tauH_SVFIT_mass-116.)*(tauH_SVFIT_mass-116.))/(35.*35.) + ((bH_mass_raw-111.)*(bH_mass_raw-111.))/(45.*45.) <  1.0
        svfit_mass = lf.getLeaf('tauH_SVFIT_mass')
        bH_mass    = lf.getLeaf('bH_mass_raw')

        #mcut = ((svfit_mass-129.)*(svfit_mass-129.))/(53.*53.) + ((bH_mass-169.)*(bH_mass-169.))/(145.*145.) <  1.0
        #if mcut: # inverted elliptical mass cut (-> ttCR)
        #    continue
        
        #        print('passed selection')
        mcweight   = lf.getLeaf( "MC_weight" )
        pureweight = lf.getLeaf( "PUReweight" )
        trigsf     = lf.getLeaf( "trigSF" )
        lumi       = lf.getLeaf( "lumi" )
        idandiso   = lf.getLeaf( "IdAndIsoSF_deep_pt")
        
        if np.isnan(mcweight): mcweight=1
        if np.isnan(pureweight): pureweight=1
        if np.isnan(trigsf): trigsf=1
        if np.isnan(lumi): lumi=1
        if np.isnan(idandiso): idandiso=1

        evtW = pureweight*trigsf*lumi*idandiso
        if np.isnan(evtW):
            evtW = 1
        if isData:
            evtW = 1

        MET    = lf.getLeaf('met_et')
        HTfull = lf.getLeaf('HT20')

        for v in variables:
            fillVar[v] = {}
            for chn in channels:
                fillVar[v].update({chn: lf.getLeaf(v)})
                if fillVar[v][chn]>binedges[v][chn][-1]:
                    fillVar[v][chn]=binedges[v][chn][-1] # include overflow

        passMET = lf.getLeaf('isMETtrigger')
        passLEP = lf.getLeaf('isLeptrigger')
        passTAU = lf.getLeaf('isSingleTautrigger')
        passTAUMET = lf.getLeaf('isTauMETtrigger')

        trigBit = lf.getLeaf('pass_triggerbit')

        passTriggerBits, passRequirements = ({} for _ in range(2))
        for trig in triggers:
            passRequirements[trig] = {}
            for var in variables:
                if trig == 'nonStandard':
                    if trig not in passTriggerBits:
                        passTriggerBits[trig] = functools.reduce(
                            lambda x,y: x or y, #logic OR to join all triggers in this option
                            [ checkBit(trigBit, getTriggerBit(x, isData)) for x in getTriggerBit(trig, isData) ]
                        )
                    #AT SOME POINT I SHOULD ADD THE CUTS LIKE IN THE 'ELSE' CLAUSE
                    passRequirements[trig][var] = passTriggerBits[trig]
                else:
                    if trig not in passTriggerBits:
                        passTriggerBits[trig] = checkBit(trigBit, getTriggerBit(trig, isData))
                    passRequirements[trig][var] =  ( passTriggerBits[trig] and
                                                     passesCut(trig, [var], lf, args.debug) )

        passMu = passLEP and (checkBit(trigBit,0) or checkBit(trigBit,1))

        for i in channels:
            if isChannelConsistent(i, passMu, pairtype):

                # fill histograms for 1D efficiencies
                for j in variables:
                    
                    if passLEP:
                        hRef[i][j].Fill(fillVar[j][i], evtW)
                        for k in triggers:
                            if passRequirements[k][j]:
                                hTrig[i][j][k].Fill(fillVar[j][i], evtW)
                    else:
                        for k in triggers:
                            if passRequirements[k][j]:
                                hNoRef[i][j][k].Fill(fillVar[j][i], evtW)

                # fill 2D efficiencies (currently only reference vs trigger, i.e.,
                # all events pass the reference cut)
                for k in triggers:
                    if k in _2Dpairs.keys():
                        for j in _2Dpairs[k]:
                            vname = addVarNames(j[0],j[1])

                            if passLEP:
                                trigger_flag = ( passTriggerBits[k] and
                                                 passesCut(k, [j[0], j[1]], lf, args.debug) )
                                effRefVsTrig[i][vname][k].Fill( trigger_flag,
                                                                fillVar[j[0]][i], fillVar[j[1]][i] )


    file_id = ''.join( c for c in fileName[-10:] if c.isdigit() ) 
    outName = os.path.join(outdir, tprefix + sample + '_' + file_id + subtag + '.root')
    print('Saving file {} at {} '.format(file_id, outName) )
    f_out = ROOT.TFile(outName, 'RECREATE')
    f_out.cd()

    # Writing histograms to the current file
    for i in channels:
        for j in variables:
            hRef[i][j].Write('Ref_{}_{}'.format(i,j))
            for k in triggers:
                hTrig[i][j][k].Write('Trig_{}_{}_{}'.format(i,j,k))
                hNoRef[i][j][k].Write('NoRef_{}_{}_{}'.format(i,j,k))

    # Writing 2D efficiencies to the current file
    for i in channels:
        for _,j in effRefVsTrig[i].items():
            for _,k in j.items():
                k.Write()

    f_out.Close()
    f_in.Close()

#Run example:
#python3 /home/llr/cms/alves/METTriggerStudies/scripts/getTriggerEffSig.py --indir /data_CMS/cms/portales/HHresonant_SKIMS/SKIMS_Radion_2018_fixedMETtriggers_mht_16Jun2021/ --outdir /data_CMS/cms/alves/FRAMEWORKTEST/ --sample MET2018A --file output_0.root --channels all etau mutau tautau mumu --subtag metnomu200cut

# -- Parse input arguments
parser = argparse.ArgumentParser(description='Command line parser')

parser.add_argument('--binedges_fname', dest='binedges_fname', required=True, help='where the bin edges are stored')
parser.add_argument('--indir',       dest='indir',             required=True, help='SKIM directory')
parser.add_argument('--outdir',      dest='outdir',            required=True, help='output directory')
parser.add_argument('--sample',      dest='sample',            required=True, help='Process name as in SKIM directory')
parser.add_argument('--isData',      dest='isData',            required=True, help='Whether it is data or MC', type=int)
parser.add_argument('--file',        dest='fileName',          required=True, help='ID of input root file')
parser.add_argument('--subtag',      dest='subtag',            required=True,
                    help='Additional (sub)tag to differ        entiate similar runs within the same tag.')
parser.add_argument('--tprefix',     dest='tprefix',           required=True, help='Targets name prefix.')
parser.add_argument('--channels',    dest='channels',          required=True, nargs='+', type=str,  
                    help='Select t   he channels over w        hich the workflow will be run.' )
parser.add_argument('--triggers',    dest='triggers',          required=True, nargs='+', type=str,
                    help='Select t   he triggers over w        hich the workflow will be run.' )
parser.add_argument('--variables',   dest='variables',         required=True, nargs='+', type=str,
                    help='Select the variables over which the workflow will be run.' )
parser.add_argument('--debug', action='store_true', help='debug verbosity')

args = parser.parse_args()

getTriggerEffSig(args.indir, args.outdir, args.sample, args.fileName,
                 args.channels, args.variables, args.triggers,
                 args.subtag, args.tprefix, args.isData, args.binedges_fname)

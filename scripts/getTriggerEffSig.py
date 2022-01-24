
"""
Script which calculates the trigger scale factors.
On production mode should run in the grid via scripts/submitTriggerEff.py. 
Local run example:

python3 /home/llr/cms/alves/CMSSW_12_2_0_pre1/src/METTriggerStudies/scripts/getTriggerEffSig.py --indir /data_CMS/cms/portales/HHresonant_SKIMS/SKIMS_2018_UL_data_test11Jan22/ --outdir /data_CMS/cms/alves/UL_v1/ --sample SKIM_MET2018 --file output_0.root --channels all etau mutau tautau mumu --subtag _default --binedges /data_CMS/cms/alves/TriggerScaleFactors/UL_v1/binedges.hdf5 --isData 1 --tprefix hist_eff_ --triggers METNoMu120 IsoTau50 --variables HT20 met_et mht_et metnomu_et mhtnomu_et dau1_pt dau2_pt dau1_eta dau2_eta --nocut_dummy_str NoCut --debug
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

from collections import defaultdict
import itertools as it

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

def passesCuts(trig, variables, leavesmanager, debug):
    """
    Handles cuts on trigger variables that enter the histograms. 
    Variables being displayed are not cut (i.e., they pass the cut).
    Checks all combinations of cuts specified in '_cuts':
        example: _cuts = {'A': ('>', [10,20]), 'B': ('<', [50,40]))}
        passesCuts will check 4 combinations and return a dict of length 4
        (unless some cuts are ignored according to '_cuts_ignored') 
    Works for both 1D and 2D efficiencies.
    """
    if debug:
        print('Trigger={}; Variables={}'.format(trig, variables))

    flagnameJoin = lambda var,sign,val: '_'.join([str(x) for x in [var,sign,val]])
    
    dflags = defaultdict(lambda: [])
    
    try:
        trig_cuts = _cuts[trig]
    except KeyError: # the trigger has no cut associated
        if debug:
            print('KeyError')            
        return {args.nocut_dummy_str: True}

    for avar,acut in trig_cuts.items():
        # ignore cuts according to the user's definition in '_cuts_ignored'
        # example: do not cut on 'met_et' when displaying 'metnomu_et'
        ignore = functools.reduce( lambda x, y: x or y,
                                   [ avar in _cuts_ignored[k] for k in variables
                                     if k in _cuts_ignored ],
                                   False
                                  )

        # additionally, by default do not cut on the variable(s) being plotted
        if avar not in variables and not ignore:
            value = leavesmanager.getLeaf(avar) 

            for c in acut[1]:
                flagname = flagnameJoin(avar, acut[0], c)

                if debug:
                    print('Cut: {} {} {}'.format(avar, acut[0], c))

                if acut[0]=='>':
                    dflags[avar].append( (flagname, value > c) )
                elif acut[0]=='<':
                    dflags[avar].append( (flagname, value < c) )
                else:
                    raise ValueError("The operator for the cut is currently not supported: Use '>' or '<'.")

    def applyCutsCombinations(dflags):
        tmp = {}
        allNames = sorted(dflags)
        combinations = it.product(*(dflags[name] for name in allNames))
        combinations = list(combinations)

        if debug:
            print('--------- [applyCutsCombinations] ------------------')
            print('Variables being cut: {}'.format(allNames))
            print('All cut combinations: {}'.format(combinations))
            print('----------------------------------------------------')
            
        for comb in combinations:
            joinFlag = functools.reduce( lambda x,y: x and y, [k[1] for k in comb] )
            tmp[ '_AND_'.join([k[0] for k in comb]) ] = joinFlag

        return tmp

    if dflags:
        res = applyCutsCombinations(dflags)
    else:
        res = {args.nocut_dummy_str: True}
    return res
    
def getTriggerEffSig(indir, outdir, sample, fileName,
                     channels, variables, triggers,
                     subtag, tprefix, isData, binedges_fname):
    # -- Check if outdir exists, if not create it
    if not os.path.exists(outdir):
        os.makedirs(outdir)
    if not os.path.exists( os.path.join(outdir, sample) ):
        os.makedirs( os.path.join(outdir, sample) )
    outdir = os.path.join(outdir, sample)

    fname = os.path.join(indir, sample, fileName)
    if not os.path.exists(fname):
        raise ValueError('[' + os.path.basename(__file__) + '] {} does not exist.'.format(fname))

    f_in = ROOT.TFile( fname )
    t_in = f_in.Get('HTauTauTree')

    fillVar = {}
    lf = LeafManager( fname, t_in )

    # Recover binning
    binedges, nbins = ({} for _ in range(2))
    with h5py.File(binedges_fname, 'r') as f:
        try:
            group = f[subtag]
        except KeyError:
            print('{} does not have key {}.'.format(binedges_filename, subtag))
            print('Available keys: {}'.format(f.keys()))
            raise
            
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
    hRef, hNoRef, hTrig = ({} for _ in range(3))

    for i in channels:
        hRef[i], hTrig[i], hNoRef[i] = ({} for _ in range(3))
        for j in variables:
            binning = (nbins[j][i], binedges[j][i])
            href_name = 'Ref_{}_{}'.format(i,j)
            hTrig[i][j]={}
            hNoRef[i][j] = {}
            hRef[i][j] = ROOT.TH1D(href_name,'', *binning)
            for k in triggers:
                hTrig[i][j][k]={}
                hNoRef[i][j][k] = {}

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
                    effRefVsTrig[i][vname][k] = {}
    
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
                passRequirements[trig][var] = {}
                if trig == 'nonStandard':
                    raise NotImplementedError
                    # if trig not in passTriggerBits:
                    #     passTriggerBits[trig] = functools.reduce(
                    #         lambda x,y: x or y, #logic OR to join all triggers in this option
                    #         [ checkBit(trigBit, getTriggerBit(x, isData)) for x in getTriggerBit(trig, isData) ]
                    #     )
                    # #AT SOME POINT I SHOULD ADD THE CUTS LIKE IN THE 'ELSE' CLAUSE
                    # passRequirements[trig][var] = passTriggerBits[trig]
                else:
                    if trig not in passTriggerBits:
                        passTriggerBits[trig] = checkBit(trigBit, getTriggerBit(trig, isData))

                    pCuts = passesCuts(trig, [var], lf, args.debug)
                    for pckey,pcval in pCuts.items():
                        passRequirements[trig][var][pckey] = ( passTriggerBits[trig] and pcval )

        passMu = passLEP and (checkBit(trigBit,0) or checkBit(trigBit,1))

        for i in channels:
            if isChannelConsistent(i, passMu, pairtype):

                # fill histograms for 1D efficiencies
                for j in variables:
                    binning = (nbins[j][i], binedges[j][i])
                    
                    if passLEP:
                        hRef[i][j].Fill(fillVar[j][i], evtW)
                        for k in triggers:
                            for kreq,vreq in passRequirements[k][j].items():
                                if kreq not in hTrig[i][j][k]:
                                    htrig_name = 'Trig_{}_{}_{}_CUTS_{}'.format(i,j,k,kreq)
                                    hTrig[i][j][k][kreq] = ROOT.TH1D(htrig_name, '', *binning)

                                if vreq:
                                    hTrig[i][j][k][kreq].Fill(fillVar[j][i], evtW)
                    else:
                        for k in triggers:
                            for kreq,vreq in passRequirements[k][j].items():
                                if kreq not in hNoRef[i][j][k]:
                                    hnoref_name = 'NoRef_{}_{}_{}_CUTS_{}'.format(i,j,k,kreq)
                                    hNoRef[i][j][k][kreq] = ROOT.TH1D(hnoref_name, '', *binning)

                                if vreq:
                                    hNoRef[i][j][k][kreq].Fill(fillVar[j][i], evtW)

                # fill 2D efficiencies (currently only reference vs trigger, i.e.,
                # all events pass the reference cut)
                for k in triggers:
                    if k in _2Dpairs.keys():
                        for j in _2Dpairs[k]:
                            vname = addVarNames(j[0],j[1])

                            if passLEP:
                                pCuts = passesCuts(k, [j[0], j[1]], lf, args.debug)
                                for pckey,pcval in pCuts.items():
                                    if pckey not in effRefVsTrig[i][vname][k]:
                                        effRefVsTrig_name = 'effRefVsTrig_{}_{}_{}'.format(i,k,vname)
                                        effRefVsTrig_name += '_CUTS_' + pckey
                                        effRefVsTrig[i][vname][k][pckey] = ROOT.TEfficiency( effRefVsTrig_name,
                                                                                             '',
                                                                                             nbins[j[0]][i],
                                                                                             binedges[j[0]][i],
                                                                                             nbins[j[1]][i],
                                                                                             binedges[j[1]][i] )
                                        effRefVsTrig[i][vname][k][pckey].SetConfidenceLevel(0.683)
                                        #Clopper-Pearson (default)
                                        effRefVsTrig[i][vname][k][pckey].SetStatisticOption(0)
                    
                                    trigger_flag = ( passTriggerBits[k] and pcval )
                                    effRefVsTrig[i][vname][k][pckey].Fill( trigger_flag,
                                                                           fillVar[j[0]][i],
                                                                           fillVar[j[1]][i] )
                                
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
                for khist,vhist in hTrig[i][j][k].items():
                    vhist.Write('Trig_{}_{}_{}_CUTS_{}'.format(i,j,k,khist))
                for khist,vhist in hNoRef[i][j][k].items():
                    vhist.Write('NoRef_{}_{}_{}_CUTS_{}'.format(i,j,k,khist))

    # Writing 2D efficiencies to the current file
    for i in channels:
        for _,j in effRefVsTrig[i].items():
            for _,k in j.items():
                for _,q in k.items():
                    q.Write()

    f_out.Close()
    f_in.Close()

# -- Parse input arguments
parser = argparse.ArgumentParser(description='Command line parser')

parser.add_argument('--binedges_fname', dest='binedges_fname', required=True, help='where the bin edges are stored')
parser.add_argument('--indir',       dest='indir',       required=True, help='SKIM directory')
parser.add_argument('--outdir',      dest='outdir',      required=True, help='output directory')
parser.add_argument('--sample',      dest='sample',      required=True, help='Process name as in SKIM directory')
parser.add_argument('--isData',      dest='isData',      required=True, help='Whether it is data or MC', type=int)
parser.add_argument('--file',        dest='fileName',    required=True, help='ID of input root file')
parser.add_argument('--subtag',      dest='subtag',      required=True,
                    help='Additional (sub)tag to differ  entiate similar runs within the same tag.')
parser.add_argument('--tprefix',     dest='tprefix',     required=True, help='Targets name prefix.')
parser.add_argument('--channels',    dest='channels',    required=True, nargs='+', type=str,  
                    help='Select t   he channels over w  hich the workflow will be run.' )
parser.add_argument('--triggers',    dest='triggers',    required=True, nargs='+', type=str,
                    help='Select t   he triggers over w  hich the workflow will be run.' )
parser.add_argument('--variables',   dest='variables',   required=True, nargs='+', type=str,
                    help='Select the variables over which the workflow will be run.' )
parser.add_argument('--nocut_dummy_str', dest='nocut_dummy_str', required=True,
                    help='Dummy string associated to trigger histograms were no cuts are applied.')
parser.add_argument('--debug', action='store_true', help='debug verbosity')

args = parser.parse_args()

getTriggerEffSig(args.indir, args.outdir, args.sample, args.fileName,
                 args.channels, args.variables, args.triggers,
                 args.subtag, args.tprefix, args.isData, args.binedges_fname)

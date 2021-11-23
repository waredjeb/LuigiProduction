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

def CheckBit(number,bitpos):
    bitdigit = 1
    res = bool(number&(bitdigit<<bitpos))
    return res

class LeafManager():
    """
    Class to manage TTree branch leafs, making sure they exist.
    """
    def __init__(self, fname, t_in):
        self.fname = fname
        self.tree = t_in
        self.absent_leaves = set()
        self.error_prefix = '[LeafManager]: '
        
    def getLeaf(self, leaf):
        if not isinstance(leaf, str):
            m = 'The leaf must be a string.'
            raise TypeError(self.error_prefix + m)
        try:
            obj = self.tree.GetListOfBranches().FindObject(leaf)
            name = obj.GetName()
            getAttr = lambda x : getattr(self.tree, x)
            return getAttr(leaf)
        except ReferenceError:
            if leaf not in self.absent_leaves:
                m = 'WARNING: leaf ' + leaf + ' does not exist in file ' + self.fname + '.'
                print(self.error_prefix + m)
                self.absent_leaves.add(leaf)
            return 0.

def getTriggerEffSig(indir, outdir, sample, fileName,
                     channels, variables, triggers,
                     subtag, tprefix, isData):
    # -- Check if outdir exists, if not create it
    if not os.path.exists(outdir):
        os.makedirs(outdir)
    if not os.path.exists( os.path.join(outdir, sample) ):
        os.makedirs( os.path.join(outdir, sample) )
    outdir = os.path.join(outdir, sample)

    # -- Shift triggers for the data (MC and data triggers do not match)
    if isData:
        shiftedTriggers = []
        for t in triggers:
            try:
                shiftedTriggers.append(int(t)+5) #shift based on HLT trigger bits
            except ValueError:
                if t == 'nonStandard':
                    shiftedTriggers.append('nonStandard')
                else:
                    raise
    else:
        shiftedTriggers = triggers
        
    # -- Define histograms
    h_MET, h_METonly, h_ALL= {},{},{}

    for i in channels:

        h_MET[i] = {}
        h_METonly[i] = {}
        h_ALL[i] = {}
        for j in variables:
            hall_name = 'passALL_{}_{}'.format(i,j)
            h_MET[i][j]={}
            h_METonly[i][j] = {}
                
            h_ALL[i][j] = ROOT.TH1D(hall_name,'', 6, 0., 600.)
            for k in triggers:
                hmet_name = 'passMET_{}_{}_{}'.format(i,j,k)
                h_MET[i][j][k] = ROOT.TH1D(hmet_name, '', 6, 0., 600.)
                hmetonly_name = 'passMETOnly_{}_{}_{}'.format(i,j,k)
                h_METonly[i][j][k] = ROOT.TH1D(hmetonly_name, '', 6, 0., 600.)

    fname = os.path.join(indir, 'SKIM_'+sample, fileName)
    f_in = ROOT.TFile( fname )
    t_in = f_in.Get('HTauTauTree')

    sumweights=0
    fillVar = {}
    lf = LeafManager( fname, t_in )

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
        if np.isnan(evtW): evtW=1
        if isData:
            evtW=1.
        sumweights+=evtW

        MET    = lf.getLeaf('met_et')
        HTfull = lf.getLeaf('HT20')

        for v in variables:
            fillVar[v] = lf.getLeaf(v)
        for j in variables:
            if fillVar[j]>600: fillVar[j]=599. # include overflow

        passMET = lf.getLeaf(   'isMETtrigger')
        passLEP = lf.getLeaf(   'isLeptrigger')
        passTAU = lf.getLeaf(   'isSingleTautrigger')
        passTAUMET = lf.getLeaf('isTauMETtrigger')

        trigBit = lf.getLeaf('pass_triggerbit')
        if isData:
            nonStandTrigBits = [CheckBit(trigBit, i) for i in range(14,20)]
        else:
            nonStandTrigBits = [CheckBit(trigBit, i) for i in range(9,15)]
        
        passReq = {}
        for trig in triggers:
            try:
                passReq[trig] = CheckBit(trigBit, int(trig))
            except ValueError:
                if trig == 'nonStandard':
                    passReq[trig] = functools.reduce(lambda x,y: x or y,
                                                     nonStandTrigBits)
                else:
                    raise

        passReq['MET'] = passMET
        passReq['Tau'] = passTAU
        passReq['TauMET'] = passTAUMET
        
        passMu = passLEP and (CheckBit(trigBit,0) or CheckBit(trigBit,1))

        if passLEP:
            for i in channels:
                cond = (   ( i=='all'    and pairtype<3  ) 
                        or ( i=='mutau'  and pairtype==0 ) 
                        or ( i=='etau'   and pairtype==1 )  
                        or ( i=='tautau' and pairtype==2 )
                        or ( i=='mumu'   and pairtype==3 and passMu) 
                        or ( i=='ee'     and pairtype==4 ))
                if cond: 
                    for j in variables:
                        h_ALL[i][j].Fill(fillVar[j],evtW)
                for j in variables:
                    for k in triggers:
                        if cond and passReq[k]:
                            h_MET[i][j][k].Fill(fillVar[j],evtW)
        else:
            for i in channels:
                cond = (   ( i=='all'    and pairtype<3  ) 
                        or ( i=='mutau'  and pairtype==0 ) 
                        or ( i=='etau'   and pairtype==1 )  
                        or ( i=='tautau' and pairtype==2 )
                        or ( i=='mumu'   and pairtype==3 and passMu ) 
                        or ( i=='ee'     and pairtype==4 ))
                for j in variables:
                    for k in triggers:
                        if cond and passReq[k]:
                            h_METonly[i][j][k].Fill(fillVar[j],evtW)


    file_id = ''.join( c for c in fileName[-10:] if c.isdigit() ) 
    outName = os.path.join(outdir, tprefix + sample + '_' + file_id + '.' + subtag + '.root')
    print('Saving file {} at {} '.format(file_id, outName) )
    f_out = ROOT.TFile(outName, 'RECREATE')
    f_out.cd()

    for i in channels:
        for j in variables:
            h_ALL[i][j].Write('passALL_{}_{}'.format(i,j))
            for k in triggers:
                h_MET[i][j][k].Write('passMET_{}_{}_{}'.format(i,j,k))
                h_METonly[i][j][k].Write('passMETOnly_{}_{}_{}'.format(i,j,k))

    f_out.Close()
    f_in.Close()

#Run example:
#python3 /home/llr/cms/alves/METTriggerStudies/scripts/getTriggerEffSig.py --indir /data_CMS/cms/portales/HHresonant_SKIMS/SKIMS_Radion_2018_fixedMETtriggers_mht_16Jun2021/ --outdir /data_CMS/cms/alves/FRAMEWORKTEST/ --sample MET2018A --file output_0.root --channels all etau mutau tautau mumu --subtag metnomu200cut

# -- Parse input arguments
parser = argparse.ArgumentParser(description='Command line parser')

parser.add_argument('--indir',    dest='indir',     required=True, help='SKIM directory')
parser.add_argument('--outdir',   dest='outdir',    required=True, help='output directory')
parser.add_argument('--sample',   dest='sample',    required=True, help='Process name as in SKIM directory')
parser.add_argument('--isData',   dest='isData',    required=True, help='Whether it is data or MC', type=int)
parser.add_argument('--file',     dest='fileName',  required=True, help='ID of input root file')
parser.add_argument('--subtag',   dest='subtag',    required=True,
                    help='Additional (sub)tag to differentiate similar runs within the same tag.')
parser.add_argument('--tprefix',  dest='tprefix',   required=True, help='Targets name prefix.')
parser.add_argument('--channels', dest='channels',  required=True, nargs='+', type=str,
                    help='Select the channels over which the workflow will be run.' )
parser.add_argument('--triggers', dest='triggers',  required=True, nargs='+', type=str,
                    help='Select the triggers over which the workflow will be run.' )
parser.add_argument('--variables', dest='variables', required=True, nargs='+', type=str,
                    help='Select the variables over which the workflow will be run.' )

args = parser.parse_args()

getTriggerEffSig(args.indir, args.outdir, args.sample, args.fileName,
                 args.channels, args.variables, args.triggers,
                 args.subtag, args.tprefix, args.isData)

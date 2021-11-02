from ROOT import *
import re
import os
import sys
import argparse
import fnmatch
import math
from array import array
import numpy as np

def CheckBit(number,bitpos):
    bitdigit = 1
    res = bool(number&(bitdigit<<bitpos))
    return res

if __name__ == "__main__":

    # -- Parse input arguments
    parser = argparse.ArgumentParser(description='Command line parser')

    parser.add_argument('--indir',   dest='indir',   help='SKIM directory')
    parser.add_argument('--outdir',  dest='outdir',  help='output directory')
    parser.add_argument('--proc',    dest='proc',    help='Process name as in SKIM directory')
    parser.add_argument('--file',    dest='fileName', help='ID of input root file')

    args = parser.parse_args()

    getTriggerEffSig(args.indir, args/outdir, args.proc, args.file)
    

def getTriggerEffSig(indir, outdir, proc, file):
    
    # -- Check if outdir exists, if not create it
    if not os.path.exists(args.outdir):
        os.makedirs(args.outdir)
    if not os.path.exists(args.outdir+'/'+args.proc):
        os.makedirs(args.outdir+'/'+args.proc)
    OUT_DIR= args.outdir+'/'+args.proc

    
    # -- Define histograms
    h_MET, h_METonly, h_ALL= {},{},{}
    cat = ['etau','mutau','tautau','mumu','ee','emu','all']
    #trg = ['9','10','11']#,'12','13','14', 'all']
    trg = ['MET','Tau','TauMET']
    var = ['met_et','HT20','mht_et','metnomu_et','mhtnomu_et','dau1_pt','dau2_pt']
    for i in cat:
        h_MET[i] = {}
        h_METonly[i] = {}
        h_ALL[i] = {}#TH1D('passALL_{}'.format(i),     '',6,0.,600.)
        for j in var:
            h_MET[i][j]={}
            h_METonly[i][j] = {}
            h_ALL[i][j] = TH1D('passALL_{}_{}'.format(i,j),     '',6,0.,600.)
            for k in trg:
                h_MET[i][j][k] = TH1D('passMET_{}_{}_{}'.format(i,j,k),     '',6,0.,600.)
                h_METonly[i][j][k] = TH1D('passMETonly_{}_{}_{}'.format(i,j,k),     '',6,0.,600.)


    isData = ('2018' in args.proc) # TODO

    f_in = TFile(args.indir+"/SKIM_"+args.proc+"/"+args.fileName)
    t_in = f_in.Get('HTauTauTree')

    sumweights=0
    fillVar = {}
    for entry in range(0,t_in.GetEntries()):
        t_in.GetEntry(entry)
        
        pairtype = getattr(t_in, 'pairType' )
        mhh = getattr(t_in, 'HHKin_mass' )
        if mhh<1:
            continue
#        print('mass ok')
        nleps      = getattr(t_in, 'nleps'      )
        nbjetscand = getattr(t_in, 'nbjetscand' )
        isOS       = getattr(t_in, 'isOS'       )

        dau1_eleiso = getattr(t_in, 'dau1_eleMVAiso'    )
        dau1_muiso  = getattr(t_in, 'dau1_iso'          )
        dau1_tauiso = getattr(t_in, 'dau1_deepTauVsJet' )
        dau2_tauiso = getattr(t_in, 'dau2_deepTauVsJet' )
        
        if pairtype==1 and (dau1_eleiso!=1 or dau2_tauiso<5):
            continue
        if pairtype==0 and (dau1_muiso>=0.15 or dau2_tauiso<5):
            continue
        if pairtype==2 and (dau1_tauiso<5 or dau2_tauiso<5):
            continue

        #((tauH_SVFIT_mass-116.)*(tauH_SVFIT_mass-116.))/(35.*35.) + ((bH_mass_raw-111.)*(bH_mass_raw-111.))/(45.*45.) <  1.0
        svfit_mass = getattr(t_in,'tauH_SVFIT_mass')
        bH_mass    = getattr(t_in,'bH_mass_raw')

       #mcut = ((svfit_mass-129.)*(svfit_mass-129.))/(53.*53.) + ((bH_mass-169.)*(bH_mass-169.))/(145.*145.) <  1.0
       #if mcut: # inverted elliptical mass cut (-> ttCR)
       #    continue

#        print('passed selection')
        mcweight   = getattr(t_in, "MC_weight"          )
        pureweight = getattr(t_in, "PUReweight"         )
        trigsf     = getattr(t_in, "trigSF"             )
        lumi       = getattr(t_in, "lumi"               )
        idandiso   = getattr(t_in, "IdAndIsoSF_deep_pt" )

        if np.isnan(mcweight): mcweight=1
        if np.isnan(pureweight): pureweight=1
        if np.isnan(trigsf): trigsf=1
        if np.isnan(lumi): lumi=1
        if np.isnan(idandiso): idandiso=1

        evtW = pureweight*trigsf*lumi*idandiso
        if np.isnan(evtW): evtW=1
        if isData: evtW=1.
        sumweights+=evtW

        MET    = getattr(t_in,'met_et')
        HTfull = getattr(t_in,'HT20')

        #var = ['met_et','HT20','mht_et','metnomu_et','mhtnomu_et']
        fillVar['met_et']     = getattr(t_in,'met_et')   #MET
        fillVar['HT20']       = getattr(t_in,'HT20')     #HTfull
        fillVar['mht_et']     = getattr(t_in,'mht_et')
        fillVar['metnomu_et'] = getattr(t_in,'metnomu_et')
        fillVar['mhtnomu_et'] = getattr(t_in,'mhtnomu_et')
        fillVar['dau1_pt']       = getattr(t_in,'dau1_pt')#HTfull
        fillVar['dau2_pt']       = getattr(t_in,'dau2_pt')#HTfull


        #fillVar=HTfull
        #if fillVar['metnomu_et']<200: continue
        for j in var:
            if fillVar[j]>600: fillVar[j]=599. # include overflow

        passMET = getattr(t_in,'isMETtrigger')
        passLEP = getattr(t_in,'isLeptrigger')
        passTAU = getattr(t_in,'isSingleTautrigger')
        passTAUMET = getattr(t_in,'isTauMETtrigger')

        trigBit = getattr(t_in,'pass_triggerbit')
        
        passReq = {}
        #req=[9,10,11]#,12,13,14]
        #if isData:
        #    req=[14,15,16]#,17,18,19]
        #
        #passReq['9']  = CheckBit(trigBit,req[0])
        #passReq['10'] = CheckBit(trigBit,req[1])
        #passReq['11'] = CheckBit(trigBit,req[2])
        ##passReq['12'] = CheckBit(trigBit,req[3])
        ##passReq['13'] = CheckBit(trigBit,req[4])
        ##passReq['14'] = CheckBit(trigBit,req[5])
        passReq['MET'] = passMET
        passReq['Tau'] = passTAU
        passReq['TauMET'] = passTAUMET
        #
        passMu   = passLEP and (CheckBit(trigBit,0) or CheckBit(trigBit,1))

        if passLEP:
            for i in cat:
                cond = (   ( i=='all'    and pairtype<3  ) 
                        or ( i=='mutau'  and pairtype==0 ) 
                        or ( i=='etau'   and pairtype==1 )  
                        or ( i=='tautau' and pairtype==2 )
                        or ( i=='mumu'   and pairtype==3 and passMu) 
                        or ( i=='ee'     and pairtype==4 ))
                if cond: 
                    for j in var:
                        h_ALL[i][j].Fill(fillVar[j],evtW)
                for j in var:
                    for k in trg:
                        if cond and passReq[k]:
                            h_MET[i][j][k].Fill(fillVar[j],evtW)
        else:
            for i in cat:
                cond = (   ( i=='all'    and pairtype<3  ) 
                        or ( i=='mutau'  and pairtype==0 ) 
                        or ( i=='etau'   and pairtype==1 )  
                        or ( i=='tautau' and pairtype==2 )
                        or ( i=='mumu'   and pairtype==3 and passMu ) 
                        or ( i=='ee'     and pairtype==4 ))
                for j in var:
                    for k in trg:
                        if cond and passReq[k]:
                            h_METonly[i][j][k].Fill(fillVar[j],evtW)


    file_id = ''.join( c for c in args.fileName[-10:] if c.isdigit() ) 
    outName = OUT_DIR+"/hist_eff_"+args.proc+"_"+file_id+".metnomu200cut.root"
    print('saving file: ', file_id, 'at ', outName)
    f_out = TFile(outName,'recreate')
    f_out.cd()

    for i in cat:
        for j in var:
            h_ALL[i][j].Write('passALL_{}_{}'.format(i,j))
            for k in trg:
                h_MET[i][j][k].Write('pass{}_{}_{}'.format(k,i,j))
                h_METonly[i][j][k].Write('pass{}only_{}_{}'.format(k,i,j))

    f_out.Close()
    f_in.Close()

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

    parser = argparse.ArgumentParser(description='Command line parser')

    parser.add_argument('--indir',   dest='indir',   help='SKIM directory')
    parser.add_argument('--outdir',  dest='outdir',  help='output directory')
    parser.add_argument('--proc',    dest='proc',    help='Process name as in SKIM directory')
    parser.add_argument('--file',    dest='fileName', help='ID of input root file')

    args = parser.parse_args()

    if not os.path.exists(args.outdir+'/'+args.proc):
        os.makedirs(args.outdir+'/'+args.proc)
    OUT_DIR= args.outdir+'/'+args.proc

    h_MET, h_METonly, h_ALL= {},{},{}
    cat = ['etau','mutau','tautau','all']
    trg = ['9','10','11','12','13','14', 'all']
    for i in cat:
        h_MET[i] = {}
        h_METonly[i] = {}
        h_ALL[i] = TH1D('passALL_{}'.format(i),     '',30,0,600)
        for j in trg:
            h_MET[i][j] = TH1D('passMET_{}_{}'.format(i,j),     '',30,0,600)
            h_METonly[i][j] = TH1D('passMETonly_{}_{}'.format(i,j),     '',30,0,600)



    isData = ('2018' in args.proc) # TODO

    f_in = TFile(args.indir+"SKIM_"+args.proc+"/"+args.fileName)
    t_in = f_in.Get('HTauTauTree')

    sumweights=0
    for entry in range(0,t_in.GetEntries()):
        t_in.GetEntry(entry)
        
        pairtype = getattr(t_in, 'pairType' )
        mhh = getattr(t_in, 'HHKin_mass' )
        if mhh<1:
            continue
        print('mass ok')
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
        print('passed selection')
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
        
        sumweights+=evtW

        MET    = getattr(t_in,'met_et')
        HTfull = getattr(t_in,'HT20Full')
        #HT     = getattr(t_in,'HT')

        passMET = getattr(t_in,'isMETtrigger')
        passLEP = getattr(t_in,'isLeptrigger')

        trigBit = getattr(t_in,'pass_triggerbit')
        
        passReq = {}
        req=[9,10,11,12,13,14]
        if isData:
            req=[14,15,16,17,18,19]

        passReq['9']  = CheckBit(trigBit,req[0])
        passReq['10'] = CheckBit(trigBit,req[1])
        passReq['11'] = CheckBit(trigBit,req[2])
        passReq['12'] = CheckBit(trigBit,req[3])
        passReq['13'] = CheckBit(trigBit,req[4])
        passReq['14'] = CheckBit(trigBit,req[5])
        passReq['all'] = passMET
    
        if passLEP:
            for i in cat:
                cond = (   ( i=='all'                    ) 
                        or ( i=='mutau'  and pairtype==0 ) 
                        or ( i=='etau'   and pairtype==1 )  
                        or ( i=='tautau' and pairtype==2 ))
                if cond: 
                    h_ALL[i].Fill(MET,evtW)
                for j in trg:
                    if cond:
                        if passReq[j]:
                            h_MET[i][j].Fill(MET,evtW)
        else:
            for i in cat:
                cond = (   ( i=='all'                    ) 
                        or ( i=='mutau'  and pairtype==0 ) 
                        or ( i=='etau'   and pairtype==1 )  
                        or ( i=='tautau' and pairtype==2 ))
                for j in trg:
                    if cond:
                        if passReq[j]:
                            h_METonly[i][j].Fill(MET,evtW)


    file_id = ''.join( c for c in args.fileName[-10:] if c.isdigit() ) 
    outName = OUT_DIR+"/hist_eff_"+args.proc+"_"+file_id+".root"
    print('saving file: ', file_id, 'at ', outName)
    f_out = TFile(outName,'recreate')
    f_out.cd()

    for i in cat:
        h_ALL[i].Write('passALL_{}'.format(i))
        for j in trg:
            h_MET[i][j].Write('passMET_{}_{}'.format(i,j))
            h_METonly[i][j].Write('passMETonly_{}_{}'.format(i,j))

    f_out.Close()
    f_in.Close()
            
        
        
        

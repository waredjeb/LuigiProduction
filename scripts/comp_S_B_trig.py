from ROOT import *
import re
import os
import sys
import argparse
import fnmatch
import math
from array import array
import numpy as np

# ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

def passBaseline(theTree, ievt):

    theTree.GetEntry(ievt)

    pairType    = getattr( theTree , 'pairType'          )
    nleps       = getattr( theTree , 'nleps'             )
    isOS        = getattr( theTree , 'isOS'              )
    nbjetscand  = getattr( theTree , 'nbjetscand'        )
    dau1_eleiso = getattr( theTree , 'dau1_eleMVAiso'    )
    dau1_iso    = getattr( theTree , 'dau1_iso'          )
    dau1_tauiso = getattr( theTree , 'dau1_deepTauVsJet' )
    dau2_tauiso = getattr( theTree , 'dau2_deepTauVsJet' )

    evtPass = True
    if nleps!=0 or nbjetscand<2 or isOS==0:
        evtPass = False
        
    if pairType>2:
        evtPass = False
    if dau2_tauiso<5:
        evtPass = False
       
    if pairType==0 and dau1_iso>=0.15:
        evtPass = False
    if pairType==1 and dau1_eleiso!=1.:
        evtPass = False
    if pairType==2 and dau1_tauiso<5:
        evtPass = False
        
    return evtPass

# ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

def passTriggerCut(is_VBF, is_LEP, is_MET, is_SingleTau, is_TauMET):

    passTrg = False
    
    if is_Lep or is_VBF or is_SingleTau:
        passTrg = True
    else:
        if is_TauMET and MET>100.:
            passTrg = True
        if is_MET and MET>200.:
            passTrg = True

    return passTrg

# ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

if __name__ == "__main__":


    parser = argparse.ArgumentParser(description='Command line parser')

    parser.add_argument('--indir',   dest='dir_in',   help='SKIM directory')
    parser.add_argument('--outdir',  dest='dir_out',  help='output directory')
    parser.add_argument('--file',    dest='file_in',  help='ID of input root file')

    args = parser.parse_args()
    print(args)
    # -- Check if outdir exists, if not create it
    if not os.path.exists(args.dir_out):
        os.makedirs(args.dir_out)
    if not os.path.exists(args.dir_out+'/'+args.proc):
        os.makedirs(args.dir_out+'/'+args.proc)

    f = TFile(args.dir_in+args.file_in)
    t = f.Get('HTauTauTree')

    ### histograms to be saved:

    categories = ['etau','mutau','tautau','all']
    triggers   = ['LEP','VBF','MET','TAU','TAUMET','ANY']
    variables = ['met_et','dau1_pt','dau2_pt','HHKin_mass']

    binning = {
        'met_et':  [20,0,600],
        'dau1_pt': [20,0,600],
        'dau2_pt': [20,0,600],
        'HHKin_mass': [20,0,1000],
    }
    
    # trigger overlap per category
    h_overlaps = {}
    for cat in categories:
        h_overlaps[cat] = TH2D('h_overlaps_%s' % cat,'trigger overlaps',7,0.5,7.5,5,0.5,5.5)

    # define the kin. variables histograms
    h_trg = {}
    h_only = {}
    for cat in categories:
        h_trg[cat] = {}
        h_only[cat] = {}
        for trg in triggers:
            h_trg[cat][trg] = {}
            h_only[cat][trg] = {}
            for var in variables:
                h_trg[cat][trg][var]  = TH1D('h_trg_%s_%s_%s' % (cat,trg,var),'',binning[var][0],binning[var][1],binning[var][2])
                h_only[cat][trg][var] = TH1D('h_only_%s_%s_%s' % (cat,trg,var),'',binning[var][0],binning[var][1],binning[var][2])

    # loop on tree entries
    for entry in range(0,t.GetEntries()):
        t.GetEntry(entry)

        # check baseline selection
        if not passBaseline(t,entry):
            continue
            
        # get trigger decisions

        isTrg = {}
        isTrg['VBF']    = getattr( t , 'isVBFtrigger'       )
        isTrg['LEP']    = getattr( t , 'isLeptrigger'       )
        isTrg['MET']    = getattr( t , 'isMETtrigger'       )
        isTrg['TAU']    = getattr( t , 'isSingleTautrigger' )
        isTrg['TAUMET'] = getattr( t , 'isTauMETtrigger'    )

        # security: check that AT LEAST one of the trigger category is passed
        if not (isTrg['VBF'] or isTrg['LEP'] or isTrg['MET'] or isTrg['TAU'] or isTrg['TAUMET']):
            continue
        isTrg['ANY'] = True #placeholder
            
        isOnly = {}
        isOnly['VBF']    = isTrg['VBF']    and not ( isTrg['LEP'] or isTrg['MET'] or isTrg['TAU'] or isTrg['TAUMET'] )
        isOnly['LEP']    = isTrg['LEP']    and not ( isTrg['VBF'] or isTrg['MET'] or isTrg['TAU'] or isTrg['TAUMET'] )
        isOnly['MET']    = isTrg['MET']    and not ( isTrg['LEP'] or isTrg['VBF'] or isTrg['TAU'] or isTrg['TAUMET'] )
        isOnly['TAU']    = isTrg['TAU']    and not ( isTrg['LEP'] or isTrg['MET'] or isTrg['VBF'] or isTrg['TAUMET'] )
        isOnly['TAUMET'] = isTrg['TAUMET'] and not ( isTrg['LEP'] or isTrg['MET'] or isTrg['TAU'] or isTrg['VBF']    )
        
        isOnly['ANY'] = True # placeholder

        is_LEPorMET = isTrg['LEP'] or isTrg['MET'] or isTrg['VBF']

        for i,cat in enumerate(categories):
            if i==3 or getattr( t , 'pairType')==i:
                # Pass only "that" trigger
                h_overlaps[cat].Fill(1,1,isOnly['LEP'])
                h_overlaps[cat].Fill(1,2,isOnly['VBF'])
                h_overlaps[cat].Fill(1,3,isOnly['MET'])
                h_overlaps[cat].Fill(1,4,isOnly['TAUMET'])
                h_overlaps[cat].Fill(1,5,isOnly['TAU'])
                # pass LEP & MET & trigger
                h_overlaps[cat].Fill(2,1, not is_LEPorMET or isTrg['LEP'])
                h_overlaps[cat].Fill(2,2, not is_LEPorMET or isTrg['VBF'])
                h_overlaps[cat].Fill(2,3, not is_LEPorMET or isTrg['MET'])
                h_overlaps[cat].Fill(2,4, not is_LEPorMET or isTrg['TAUMET'])
                h_overlaps[cat].Fill(2,5, not is_LEPorMET or isTrg['TAU'])
                # pass LEP & trigger
                h_overlaps[cat].Fill(3,1,isTrg['LEP'] or isTrg['LEP'])
                h_overlaps[cat].Fill(3,2,isTrg['LEP'] or isTrg['VBF'])
                h_overlaps[cat].Fill(3,3,isTrg['LEP'] or isTrg['MET'])
                h_overlaps[cat].Fill(3,4,isTrg['LEP'] or isTrg['TAUMET'])
                h_overlaps[cat].Fill(3,5,isTrg['LEP'] or isTrg['TAU'])
                # pass VBF & trigger
                h_overlaps[cat].Fill(4,1,isTrg['VBF'] or isTrg['LEP'])
                h_overlaps[cat].Fill(4,2,isTrg['VBF'] or isTrg['VBF'])
                h_overlaps[cat].Fill(4,3,isTrg['VBF'] or isTrg['MET'])
                h_overlaps[cat].Fill(4,4,isTrg['VBF'] or isTrg['TAUMET'])
                h_overlaps[cat].Fill(4,5,isTrg['VBF'] or isTrg['TAU'])
                # pass MET & trigger
                h_overlaps[cat].Fill(5,1,isTrg['MET'] or isTrg['LEP'])
                h_overlaps[cat].Fill(5,2,isTrg['MET'] or isTrg['VBF'])
                h_overlaps[cat].Fill(5,3,isTrg['MET'] or isTrg['MET'])
                h_overlaps[cat].Fill(5,4,isTrg['MET'] or isTrg['TAUMET'])
                h_overlaps[cat].Fill(5,5,isTrg['MET'] or isTrg['TAU'])
                # pass TAUMET & trigger
                h_overlaps[cat].Fill(6,1,isTrg['TAUMET'] or isTrg['LEP'])
                h_overlaps[cat].Fill(6,2,isTrg['TAUMET'] or isTrg['VBF'])
                h_overlaps[cat].Fill(6,3,isTrg['TAUMET'] or isTrg['MET'])
                h_overlaps[cat].Fill(6,4,isTrg['TAUMET'] or isTrg['TAUMET'])
                h_overlaps[cat].Fill(6,5,isTrg['TAUMET'] or isTrg['TAU'])
                # pass TAU & trigger
                h_overlaps[cat].Fill(7,1,isTrg['TAU'] or isTrg['LEP'])
                h_overlaps[cat].Fill(7,2,isTrg['TAU'] or isTrg['VBF'])
                h_overlaps[cat].Fill(7,3,isTrg['TAU'] or isTrg['MET'])
                h_overlaps[cat].Fill(7,4,isTrg['TAU'] or isTrg['TAUMET'])
                h_overlaps[cat].Fill(7,5,isTrg['TAU'] or isTrg['TAU'])
            
        kinvar = {}
        weight = getattr( t , 'totalWeight' )
        for var in variables:
            kinvar[var] = getattr( t , var )
            
        for i,cat in enumerate(categories):
            if not (i==3 or getattr( t , 'pairType')==i):
                continue
            for trg in triggers:
                if isTrg[trg]:
                    for var in variables:
                        h_trg[cat][trg][var].Fill(kinvar[var], weight)
                if isOnly[trg]:
                    for var in variables:
                        h_only[cat][trg][var].Fill(kinvar[var], weight)
                    
        
                        

    # create output file
    f_out = TFile(args.dir_out+args.file_in,'recreate')
    f_out.cd()

    # save histograms
    for cat in categories:
        h_overlaps[cat].GetXaxis().SetBinLabel(1,"none");
        h_overlaps[cat].GetXaxis().SetBinLabel(2,"notLEPMETVBF");
        h_overlaps[cat].GetXaxis().SetBinLabel(3,"isLEP");
        h_overlaps[cat].GetXaxis().SetBinLabel(4,"isVBF");
        h_overlaps[cat].GetXaxis().SetBinLabel(5,"isMET");
        h_overlaps[cat].GetXaxis().SetBinLabel(6,"isTauMET");
        h_overlaps[cat].GetXaxis().SetBinLabel(7,"isSingleTau");
        h_overlaps[cat].GetYaxis().SetBinLabel(1,"isLEP");
        h_overlaps[cat].GetYaxis().SetBinLabel(2,"isVBF");
        h_overlaps[cat].GetYaxis().SetBinLabel(3,"isMET");
        h_overlaps[cat].GetYaxis().SetBinLabel(4,"isTauMET");
        h_overlaps[cat].GetYaxis().SetBinLabel(5,"isSingleTau");

        h_overlaps[cat].Write()
        for trg in triggers:
            for var in variables:
                h_trg[cat][trg][var].GetXaxis().SetTitle(var)
                h_only[cat][trg][var].GetXaxis().SetTitle(var)
                h_trg[cat][trg][var].Write()
                h_only[cat][trg][var].Write()

    f_out.Close()

# ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

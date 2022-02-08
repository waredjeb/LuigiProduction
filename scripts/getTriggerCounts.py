
"""
Script which calculates the trigger scale factors.
On production mode should run in the grid via scripts/submitTriggerEff.py. 
Local run example:

python3 /home/llr/cms/alves/CMSSW_12_2_0_pre1/src/METTriggerStudies/scripts/getTriggerCounts.py --indir /data_CMS/cms/portales/HHresonant_SKIMS/SKIMS_2018_UL_data_test11Jan22/ --outdir /data_CMS/cms/alves/CountsTest/ --sample SKIM_MET2018 --file output_0.root --channels etau mutau tautau --subtag _default --isData 1 --tprefix count_ --triggers IsoMuIsoTau EleIsoTau VBFTau VBFTauHPS METNoMu120 IsoTau50 IsoTau180 --debug
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
import itertools as it

import sys
sys.path.append(os.path.join(os.environ['CMSSW_BASE'], 'src', 'METTriggerStudies'))

from utils.utils import (
    checkBit,
    getTriggerBit,
    joinNameTriggerIntersection as joinNTC,
    LeafManager
)

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
    
def getTriggerCounts(indir, outdir, sample, fileName,
                     channels, triggers,
                     subtag, tprefix, isData ):
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

    triggercomb = list( it.chain.from_iterable(it.combinations(triggers, x)
                                               for x in range(1,len(triggers)+1)) )
    counter, counterRef = ({} for _ in range(2))
    for tcomb in triggercomb:
        tcomb_str = joinNTC(tcomb)
        counter[tcomb_str] = {}
        for chn in channels:
            counter[tcomb_str][chn] = 0
            counterRef.setdefault(chn, 0.)

    lf = LeafManager( fname, t_in )
    
    for entry in range(0,t_in.GetEntries()):
        t_in.GetEntry(entry)
        
        pairtype = lf.getLeaf( 'pairType' )
        mhh = lf.getLeaf( 'HHKin_mass' )
        if mhh<1:
            continue

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

        mcut = ((svfit_mass-129.)*(svfit_mass-129.))/(53.*53.) + ((bH_mass-169.)*(bH_mass-169.))/(145.*145.) <  1.0
        if mcut: # inverted elliptical mass cut (-> ttCR)
            continue

        trigBit = lf.getLeaf('pass_triggerbit')
        passLEP = lf.getLeaf('isLeptrigger')
        passMu = passLEP and (checkBit(trigBit,0) or checkBit(trigBit,1))

        for tcomb in triggercomb:
            for chn in channels:
                if isChannelConsistent(chn, passMu, pairtype) and passLEP:

                    passAllTriggerBits = functools.reduce(
                        lambda x,y: x and y, #logic AND to join all triggers in this option
                        [ checkBit(trigBit, getTriggerBit(x, isData)) for x in tcomb ]
                    )

                    counterRef[chn] += 1
                    if passAllTriggerBits:
                        tcomb_str = joinNTC(tcomb)
                        counter[tcomb_str][chn] += 1
                                            
    file_id = ''.join( c for c in fileName[-10:] if c.isdigit() ) 
    outName = os.path.join(outdir, tprefix + sample + '_' + file_id + subtag + '.txt')
    print('Saving file {} at {} '.format(file_id, outName) )

    sep = '\t'
    with open(outName, 'w') as f:
        for chn in channels:
            f.write( 'Total' + sep + chn + sep + str(int(counterRef[chn])) + '\n' )
        for tcomb in triggercomb:
            for chn in channels:
                tcomb_str = joinNTC(tcomb)
                f.write( tcomb_str + sep + chn + sep + str(int(counter[tcomb_str][chn])) + '\n' )
    
# -- Parse input arguments
parser = argparse.ArgumentParser(description='Command line parser')

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
parser.add_argument('--debug', action='store_true', help='debug verbosity')

args = parser.parse_args()

getTriggerCounts( args.indir, args.outdir, args.sample, args.fileName,
                  args.channels, args.triggers,
                  args.subtag, args.tprefix, args.isData )

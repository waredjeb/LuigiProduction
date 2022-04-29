
"""
Script which calculates the trigger scale factors.
On production mode should run in the grid via scripts/submitTriggerEff.py. 
Local run example:

python3 /home/llr/cms/alves/CMSSW_12_2_0_pre1/src/METTriggerStudies/scripts/getTriggerCounts.py --indir /data_CMS/cms/portales/HHresonant_SKIMS/SKIMS_2018_UL_data_test11Jan22/ --outdir /data_CMS/cms/alves/CountsTest/ --sample SKIM_MET2018 --file output_0.root --channels etau mutau tautau --subtag _default --isdata 1 --tprefix count_ --triggers IsoMuIsoTau EleIsoTau VBFTau VBFTauHPS METNoMu120 IsoTau50 IsoTau180 --debug
"""
import re
import os
import sys
import functools
import argparse
import fnmatch
from array import array
from ROOT import TFile

import sys
sys.path.append( os.environ['PWD'] ) 

from writeHTCondorProcessingFiles import runTrigger_outputs_sample

from utils.utils import (
    generate_trigger_combinations,
    get_histo_names,
    get_trigger_bit,
    is_channel_consistent,
    join_name_trigger_intersection as joinNTC,
    LeafManager,
    load_binning,
    pass_any_trigger,
    pass_selection_cuts,
    rewriteCutString,
    pass_trigger_bits,
    print_configuration,
)

from luigi_conf import _triggers_custom

def getTriggerCounts(indir, outdir, sample, fileName,
                     channels, triggers,
                     subtag, tprefix, isdata ):
    # -- Check if outdir exists, if not create it
    if not os.path.exists(outdir):
        os.makedirs(outdir)
    if not os.path.exists( os.path.join(outdir, sample) ):
        os.makedirs( os.path.join(outdir, sample) )
    outdir = os.path.join(outdir, sample)

    if not os.path.exists(fileName):
        raise ValueError('[' + os.path.basename(__file__) + '] {} does not exist.'.format(fileName))

    f_in = TFile(fileName)
    t_in = f_in.Get('HTauTauTree')

    triggercomb = generate_trigger_combinations(triggers)

    counter, counterRef = ({} for _ in range(2))
    for tcomb in triggercomb:
        tcomb_str = joinNTC(tcomb)
        counter[tcomb_str] = {}
        for chn in channels:
            counter[tcomb_str][chn] = 0
            counterRef.setdefault(chn, 0.)

    lf = LeafManager(fileName, t_in)
    
    for entry in range(0,t_in.GetEntries()):
        t_in.GetEntry(entry)

        if not pass_selection_cuts(lf):
            continue

        trig_bit = lf.getLeaf('pass_triggerbit')
        run = lf.getLeaf('RunNumber')
        if not pass_any_trigger(triggers, trig_bit, run, isdata=isdata):
            continue

        pass_trigger = {}
        for trig in triggers:
            pass_trigger[trig] = pass_trigger_bits(trig, trig_bit, run, isdata)

        for tcomb in triggercomb:
            for chn in channels:
                if is_channel_consistent(chn, lf.getLeaf('pairType')):

                    pass_trigger_intersection = functools.reduce(
                        lambda x,y: x and y, #logic AND to join all triggers in this option
                        [ pass_trigger[x] for x in tcomb ]
                    )

                    counterRef[chn] += 1
                    if pass_trigger_intersection:
                        tcomb_str = joinNTC(tcomb)
                        counter[tcomb_str][chn] += 1
                                            
    file_id = ''.join( c for c in fileName[-10:] if c.isdigit() )

    #all_sample_files = runTrigger_outputs_sample(args, sample, 'root')

    proc_folder = os.path.dirname(fileName).split('/')[-1]
    outName = os.path.join(outdir, tprefix + proc_folder + '_' + file_id + subtag + '.txt')
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
parser.add_argument('--isdata',      dest='isdata',      required=True, help='Whether it is data or MC', type=int)
parser.add_argument('--file',        dest='fileName',    required=True, help='ID of input root file')
parser.add_argument('--subtag',      dest='subtag',      required=True,
                    help='Additional (sub)tag to differ  entiate similar runs within the same tag.')
parser.add_argument('--tprefix',     dest='tprefix',     required=True, help='Targets name prefix.')
parser.add_argument('--channels',    dest='channels',    required=True, nargs='+', type=str,  
                    help='Select the channels over which the workflow will be run.' )
parser.add_argument('--triggers',    dest='triggers',    required=True, nargs='+', type=str,
                    help='Select the triggers over which the workflow will be run.' )
parser.add_argument('--debug', action='store_true', help='debug verbosity')

args = parser.parse_args()
print_configuration(args)

getTriggerCounts( args.indir, args.outdir, args.sample, args.fileName,
                  args.channels, args.triggers,
                  args.subtag, args.tprefix, args.isdata )

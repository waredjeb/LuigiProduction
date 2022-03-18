import os
import h5py
import re
import glob
import json
import argparse
import sys
sys.path.append( os.environ['PWD'] )
import numpy as np

from ROOT import (
    gROOT,
    TFile,
    TH2D,
    TIter
)
gROOT.SetBatch(True)

from utils.utils import (
    find_bin,
    generateTriggerCombinations,
    get_histo_names,
    get_root_input_files,
    is_channel_consistent,
    joinNameTriggerIntersection as joinNTC,
    LeafManager,
    load_binning,
    pass_any_trigger,
    pass_selection_cuts,
    pass_trigger_bits
)

from luigi_conf import (
    _extensions,
    _variables_unionweights,
)

def eff_extractor(args, chn, effvars, nbins):
    """
    Extracts the efficiencies for data and MC to be used as scale factors
    Returns a dictionary with al efficiencies.
    """
    efficiencies_data, efficiencies_data_ehigh, efficiencies_data_elow = ({} for _ in range(3))
    efficiencies_mc, efficiencies_mc_ehigh, efficiencies_mc_elow = ({} for _ in range(3)) 
    
    triggercomb = generateTriggerCombinations(args.triggers)
    for tcomb in triggercomb:
        tcstr = joinNTC(tcomb)
        comb_vars = effvars[tcstr][0]
        assert len(comb_vars)==4

        for var in comb_vars:        
            in_base_name = ( args.inprefix + args.data_name + '_' + args.mc_name + '_' +
                             chn + '_' + var + '_' + tcstr + args.subtag + '_CUTS*.root' )
            in_name = os.path.join(args.indir_eff, chn, var, in_base_name)
            glob_name = glob.glob(in_name)

            if len(glob_name) != 0: #some triggers do not fire for some channels: Ele32 for mutau (for example)
                efficiencies_data.setdefault(tcstr, {})
                efficiencies_data[tcstr][var] = []
                efficiencies_data_elow.setdefault(tcstr, {})
                efficiencies_data_elow[tcstr][var] = []
                efficiencies_data_ehigh.setdefault(tcstr, {})
                efficiencies_data_ehigh[tcstr][var] = []

                efficiencies_mc.setdefault(tcstr, {})
                efficiencies_mc[tcstr][var] = []
                efficiencies_mc_elow.setdefault(tcstr, {})
                efficiencies_mc_elow[tcstr][var] = []
                efficiencies_mc_ehigh.setdefault(tcstr, {})
                efficiencies_mc_ehigh[tcstr][var] = []
                
                # CHANGE!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                in_file_name = min(glob_name, key=len) #select the shortest string (NoCut)

                in_file = TFile.Open(in_file_name, 'READ')
                key_list = TIter(in_file.GetListOfKeys())
                for key in key_list:
                    obj = key.ReadObj()
                    assert(nbins[var][chn] == obj.GetN())
                    if obj.GetName() == 'Data':
                        for datapoint in range(obj.GetN()):
                            efficiencies_data[tcstr][var].append( obj.GetPointY(datapoint) )
                            efficiencies_data_elow[tcstr][var].append( obj.GetErrorYlow(datapoint) )
                            efficiencies_data_ehigh[tcstr][var].append( obj.GetErrorYhigh(datapoint) )               
                    elif obj.GetName() == 'MC':
                        for datapoint in range(obj.GetN()):
                            efficiencies_mc[tcstr][var].append( obj.GetPointY(datapoint) )
                            efficiencies_mc_elow[tcstr][var].append( obj.GetErrorYlow(datapoint) )
                            efficiencies_mc_ehigh[tcstr][var].append( obj.GetErrorYhigh(datapoint) )
                assert len(efficiencies_data[tcstr][var]) != 0
                assert len(efficiencies_mc[tcstr][var]) != 0

    return ( (efficiencies_data, efficiencies_data_ehigh, efficiencies_data_elow),
             (efficiencies_mc,   efficiencies_mc_ehigh,   efficiencies_mc_elow) )


def prob_calculator(efficiencies, effvars, leaf_manager, channel, triggers, binedges):
    """
    Calculates the probabilities of this event to fire at least one of the triggers under study.

    Section 4.3.4 of the following paper:
    Lendermann V et al. Combining Triggers in HEP data analysis.
    Nucl Instruments Methods Phys Res Sect A Accel Spectrometers, 
    Detect Assoc Equip. 2009;604(3):707-718.
    doi:10.1016/j.nima.2009.03.173
    """
    nweight_vars = 4 #dau1_pt, dau1_eta, dau2_pt, dau2_eta
    prob_data, prob_mc = ([0 for _ in range(nweight_vars)] for _ in range(2))

    triggercomb = generateTriggerCombinations(triggers)
    for tcomb in triggercomb:
        joincomb = joinNTC(tcomb)

        if joincomb in efficiencies[1][0] and joincomb not in efficiencies[0][0]:
            raise ValueError('This should never happen. Cannot be in MC but not in data.')

        #some triggers do not fire for some channels: Ele32 for mutau (for example)
        if joincomb in efficiencies[0][0]:
            variables = effvars[joincomb][0] #constant 1D variables, check [1] and [2] for changing ones
            values = [ leaf_manager.getLeaf(x) for x in variables ]
            assert len(variables) == 4 #Change according to the variable discriminator
            assert len(variables) == nweight_vars

            for iw,weightvar in enumerate(variables):
                # The following is 1D only CHANGE!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                binid = find_bin(binedges[weightvar][channel], values[iw], weightvar)

                term_data = efficiencies[0][0][joinNTC(tcomb)][weightvar][binid-1]
                term_mc   = efficiencies[1][0][joinNTC(tcomb)][weightvar][binid-1]

                if len(tcomb)%2==0:
                    prob_data[iw] -= term_data
                    prob_mc[iw]   -= term_mc
                else:
                    prob_data[iw] += term_data
                    prob_mc[iw]   += term_mc

    return prob_data, prob_mc

def runUnionWeightsCalculator_outputs(args, proc):
    outputs = []

    exp = re.compile('output(_[0-9]{1,5}).root')
    inputs, _ = get_root_input_files(proc, [args.indir_root])
    folder = os.path.join( args.outdir, proc )

    for inp in inputs:
        number = exp.search(inp)
        basename = args.outprefix + '_' + proc + number.group(1) + args.subtag + '.hdf5'
        outputs.append( os.path.join(folder, basename) )
    return outputs

def runUnionWeightsCalculator(args):
    output = runUnionWeightsCalculator_outputs(args, args.sample)
    number = re.search('(_[0-9]{1,5}).root', args.file_name)
    r = re.compile('.*/' + args.outprefix + '_' + args.sample + number.group(1) + args.subtag + '\.hdf5')
    output = list(filter(r.match, output))

    assert len(output)==1
    output = output[0]

    binedges, nbins = load_binning(afile=args.binedges_fname, key=args.subtag,
                                   variables=args.variables, channels=args.channels)
    # open input ROOT file
    fname = os.path.join(args.indir_root, args.sample, args.file_name)
    if not os.path.exists(fname):
        raise ValueError('[' + os.path.basename(__file__) + '] {} does not exist.'.format(fname))
    f_in = TFile( fname )
    t_in = f_in.Get('HTauTauTree')
    lfm = LeafManager(fname, t_in)

    outdata = h5py.File(output, mode='w')
    prob_ratios, ref_prob_ratios = ({} for _ in range(2))
    effvars = {}
    efficiencies = {}
    for chn in args.channels:
        outdata.create_group(chn)
        prob_ratios[chn], ref_prob_ratios[chn] = ({} for _ in range(2))
        
        # load efficiency variables obtained previously
        json_name = os.path.join(args.indir_json,
                                 'runVariableImportanceDiscriminator_{}.json'.format(chn))
        with open(json_name, 'r') as f:
            effvars[chn] = json.load(f)

        # load efficiencies
        efficiencies[chn] = eff_extractor(args, chn, effvars[chn], nbins)
        
        # initialization
        for var in _variables_unionweights:
            outdata[chn].create_group(var)
            prob_ratios[chn][var] = {}
            ref_prob_ratios[chn][var] = {}
            for weightvar in effvars[chn][generateTriggerCombinations(args.triggers)[0][0]][0]: #any trigger works for the constant list
                outdata[chn][var].create_group(weightvar)
                prob_ratios[chn][var][weightvar] = {}
                ref_prob_ratios[chn][var][weightvar] = {}
                for trig in args.triggers:
                    outdata[chn][var][weightvar].create_group(trig)
                    prob_ratios[chn][var][weightvar][trig] = {}
                    for ibin in range(nbins[var][chn]):
                        if trig==args.triggers[0]: #reference
                            outdata[chn][var][weightvar].create_group(str(ibin))
                        outdata[chn][var][weightvar][trig].create_group(str(ibin))
                        prob_ratios[chn][var][weightvar][trig][str(ibin)] = []
                        ref_prob_ratios[chn][var][weightvar].setdefault(str(ibin), [])

    # event loop; building scale factor 2D maps
    for entry in range(0,t_in.GetEntries()):
        t_in.GetEntry(entry)
        if not pass_selection_cuts(lfm):
            continue

        trig_bit = lfm.getLeaf('pass_triggerbit')
        run = lfm.getLeaf('RunNumber')
        if not pass_any_trigger(args.triggers, trig_bit, run, isdata=False):
            continue
        for chn in args.channels:
            if not is_channel_consistent(chn, lfm.getLeaf('pairType')):
                continue
            prob_data, prob_mc = prob_calculator(efficiencies[chn],
                                                 effvars[chn],
                                                 lfm,
                                                 chn,
                                                 args.triggers,
                                                 binedges)

            prob_ratio = []
            for pd,pm in zip(prob_data,prob_mc): #loop over weight variables: dau1_pt, dau1_eta, dau2_pt, dau2_eta
                prob_ratio.append( pd/pm )
            assert len(effvars[chn][generateTriggerCombinations(args.triggers)[0][0]][0]) == len(prob_ratio)
            
            for var in _variables_unionweights:
                val = lfm.getLeaf(var)
                binid = find_bin(binedges[var][chn], val, var)
                for iw,weightvar in enumerate(effvars[chn][generateTriggerCombinations(args.triggers)[0][0]][0]): #any trigger works for the constant list
                    ref_prob_ratios[chn][var][weightvar][str(binid-1)].append(prob_ratio[iw])
                    for trig in args.triggers:
                        ptb = pass_trigger_bits(trig, trig_bit, run, isdata=False)
                        if ptb:
                            prob_ratios[chn][var][weightvar][trig][str(binid-1)].append(prob_ratio[iw])

    for chn in args.channels:
        for var in _variables_unionweights:
            for weightvar in effvars[chn][generateTriggerCombinations(args.triggers)[0][0]][0]: #any trigger works for the constant list
                for trig in args.triggers:
                    for ibin in range(nbins[var][chn]):
                        outdata[chn][var][weightvar][trig][str(ibin)]['prob_ratios'] = prob_ratios[chn][var][weightvar][trig][str(ibin)]
                        if trig==args.triggers[0]:
                            outdata[chn][var][weightvar][str(ibin)]['ref_prob_ratios'] = ref_prob_ratios[chn][var][weightvar][str(ibin)]
    outdata.attrs['doc'] = ( 'Probability ratios (data/MC) and counts per bin for all'
                             ' channels, variables and triggers' )
                
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Choose the most significant variables to draw the efficiencies.')

    parser.add_argument('--binedges_fname', dest='binedges_fname', required=True, help='where the bin edges are stored')
    parser.add_argument('--indir_root', help='Original ROOT files directory',  required=True)
    parser.add_argument('--indir_json'
                        , help='Input directory where discriminator JSON files are stored',
                        required=True)
    parser.add_argument('--indir_eff', help='Input directory where intersection efficiencies are stored',
                        required=True)
    parser.add_argument('--sample', dest='sample', required=True, help='Process name as in SKIM directory')
    parser.add_argument('--file_name', dest='file_name', required=True, help='ID of input root file')
    parser.add_argument('--outdir', help='Output directory for ROOT files', required=True)
    parser.add_argument('--inprefix', dest='inprefix', required=True, help='In histos prefix.')
    parser.add_argument('--outprefix', dest='outprefix', required=True, help='Out histos prefix.')
    parser.add_argument('--data_name', dest='data_name', required=True, help='Data sample name')
    parser.add_argument('--mc_name', dest='mc_name', required=True, help='MC sample name')
    parser.add_argument('--triggers', dest='triggers', nargs='+', type=str,
                        required=True, help='Triggers included in the workfow.')
    parser.add_argument('--channels',    dest='channels',    required=True, nargs='+', type=str,  
                        help='Select the channels over which the workflow will be run.' )
    parser.add_argument('--variables', dest='variables', required=True, nargs='+', type=str,
                        help='Workflow variables considered.')
    parser.add_argument('-t', '--tag', help='string to differentiate between different workflow runs', required=True)
    parser.add_argument('--subtag', dest='subtag', required=True, help='subtag')
    parser.add_argument('--debug', action='store_true', help='debug verbosity')
    args = parser.parse_args()

    runUnionWeightsCalculator(args)

import os
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
    TH1D,
    TIter
)
gROOT.SetBatch(True)

from utils.utils import (
    generateTriggerCombinations,
    get_histo_names,
    get_root_input_files,
    is_channel_consistent,
    joinNameTriggerIntersection as joinNTC,
    LeafManager,
    load_binning,
    pass_any_trigger,
    pass_selection_cuts,
    set_custom_trigger_bit,
)

from luigi_conf import (
    _extensions,
    _variables_unionweights,
)

def effExtractor(args, chn, effvars, nbins):
    """
    Extracts the efficiencies for data and MC to be used as scale factors: e_data / e_MC.
    Returns a dictionary with al efficiencies.
    """
    efficiencies_data, efficiencies_data_ehigh, efficiencies_data_elow = ({} for _ in range(3))
    efficiencies_mc, efficiencies_mc_ehigh, efficiencies_mc_elow = ({} for _ in range(3)) 
    
    triggercomb = generateTriggerCombinations(args.triggers)
    for tcomb in triggercomb:
        tcstr = joinNTC(tcomb)
        comb_vars = effvars[tcstr]
        assert len(comb_vars)==2
        var = comb_vars[0]
        
        inBaseName = ( 'trigSF_' + args.data_name + '_' + args.mc_name + '_' +
                       chn + '_' + var + '_' + tcstr + args.subtag + '_CUTS*.root' )
        inName = os.path.join(args.indir_eff, chn, var, inBaseName)
        globName = glob.glob(inName)

        if len(globName) != 0: #some triggers do not fire for some channels: Ele32 for mutau (for example)
            efficiencies_data[tcstr] = []
            efficiencies_data_elow[tcstr] = []
            efficiencies_data_ehigh[tcstr] = []
            efficiencies_mc[tcstr] = []
            efficiencies_mc_elow[tcstr] = []
            efficiencies_mc_ehigh[tcstr] = []

            in_file_name = min(globName, key=len) #select the shortest string (NoCut)
            in_file = TFile.Open(in_file_name, 'READ')
            key_list = TIter(in_file.GetListOfKeys())
            for key in key_list:
                obj = key.ReadObj()
                assert(nbins[var][chn] == obj.GetN())
                if obj.GetName() == 'Data':
                    for datapoint in range(obj.GetN()):
                        efficiencies_data[tcstr].append( obj.GetPointY(datapoint) )
                        efficiencies_data_elow[tcstr].append( obj.GetErrorYlow(datapoint) )
                        efficiencies_data_ehigh[tcstr].append( obj.GetErrorYhigh(datapoint) )                
                elif obj.GetName() == 'MC':
                    for datapoint in range(obj.GetN()):
                        efficiencies_mc[tcstr].append( obj.GetPointY(datapoint) )
                        efficiencies_mc_elow[tcstr].append( obj.GetErrorYlow(datapoint) )
                        efficiencies_mc_ehigh[tcstr].append( obj.GetErrorYhigh(datapoint) )
            assert len(efficiencies_data[tcstr]) != 0
            assert len(efficiencies_mc[tcstr]) != 0

    return ( (efficiencies_data, efficiencies_data_ehigh, efficiencies_data_elow),
             (efficiencies_mc,   efficiencies_mc_ehigh,   efficiencies_mc_elow) )

def find_bin(edges, value):
    """Find the bin id corresponding to one value, given the bin edges."""
    binid = np.digitize(value, edges)
    if binid == len(edges):
        binid -= 1 # include overflow

    # check for out-of-bounds
    if binid==0 or binid>len(edges):
        print(binid, values[0])
        print(binedges[variables[0]][channel], len(binedges[variables[0]][channel]))
        raise ValueError('Wrong bin')

    return binid

def eff_calculator(efficiencies, effvars, leaf_manager, channel, triggers, binedges):
    eff_data, eff_mc = (0 for _ in range(2))

    triggercomb = generateTriggerCombinations(triggers)
    for tcomb in triggercomb:
        joincomb = joinNTC(tcomb)

        if joincomb in efficiencies[1][0] and joincomb not in efficiencies[0][0]:
            raise ValueError('This should never happen. Cannot be in MC but not in data.')
        
        #some triggers do not fire for some channels: Ele32 for mutau (for example)
        if joincomb in efficiencies[0][0]:
            variables = effvars[joincomb]
            values = [ leaf_manager.getLeaf(x) for x in variables ]
            assert len(variables) == 2 #Change according to the discriminator

            # The following is 1D only CHANGE!!!!
            binid = find_bin(binedges[variables[0]][channel], values[0])

            term_data = efficiencies[0][0][joinNTC(tcomb)][binid-1]
            term_mc   = efficiencies[1][0][joinNTC(tcomb)][binid-1]

            if len(tcomb)%2==0:
                eff_data -= term_data
                eff_mc   -= term_mc
            else:
                eff_data += term_data
                eff_mc   += term_mc

    return eff_data, eff_mc

def runUnionWeightsCalculator_outputs(args, proc):
    outputs = []
    exp = re.compile('output(_[0-9]{1,5}).root')
    inputs, _ = get_root_input_files(proc, [args.indir_root])
    folder = os.path.join( args.outdir, proc )
    for inp in inputs:
        number = exp.search(inp)
        basename = args.outprefix + '_' + proc + number.group(1) + args.subtag + '.root'
        outputs.append( os.path.join(folder, basename) )
    return outputs

def runUnionWeightsCalculator(args):
    output = runUnionWeightsCalculator_outputs(args, args.sample)
    number = re.search('output(_[0-9]{1,5}).root', args.file_name)
    output = [ x for x in output if number.group(1) in x ]
    assert len(output)==1
    output = output[0]
    
    binedges, nbins = load_binning(afile=args.binedges_fname, key=args.subtag,
                                   variables=args.variables, channels=args.channels)

    effvars = {}
    h_union_weights = {}
    efficiencies = {}
    
    for chn in args.channels:
        h_union_weights[chn] = {}

        # load efficiency variables obtained previously
        json_name = os.path.join(args.indir_json,
                                 'runVariableImportanceDiscriminator_{}.json'.format(chn))
        with open(json_name, 'r') as f:
            effvars[chn] = json.load(f)

        # load efficiencies
        efficiencies[chn] = effExtractor(args, chn, effvars[chn], nbins)

        # initialize histograms
        for var in _variables_unionweights:
            var_low, var_high = binedges[var][chn][0], binedges[var][chn][-1]
            h_union_weights[chn][var] = TH1D(var + '_' + chn, var + '_' + chn,
                                             nbins[var][chn], var_low, var_high)

    # open input ROOT file
    fname = os.path.join(args.indir_root, args.sample, args.file_name)
    if not os.path.exists(fname):
        raise ValueError('[' + os.path.basename(__file__) + '] {} does not exist.'.format(fname))
    f_in = TFile( fname )
    t_in = f_in.Get('HTauTauTree')
    lfm = LeafManager(fname, t_in)

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

            eff_data, eff_mc = eff_calculator(efficiencies[chn],
                                              effvars[chn],
                                              lfm,
                                              chn,
                                              args.triggers,
                                              binedges)

            for var in _variables_unionweights:
                val = lfm.getLeaf(var)
                h_union_weights[chn][var].Fill(val, eff_data / eff_mc )

    f_out = TFile(output, 'RECREATE')
    f_out.cd()
    for chn in args.channels:
        for var in _variables_unionweights:
            h_union_weights[chn][var].Write( get_histo_names('Closure')(chn,var) )

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

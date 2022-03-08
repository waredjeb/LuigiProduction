import os
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
    _nbins_unionweights,
)

def meanbins(m1,m2,nelem):
    arr = np.linspace(m1, m2, nelem)
    return (arr[:-1]+arr[1:])/2

def effExtractor(args, chn, dvars, nbins):
    """
    Extracts the efficiencies for data and MC to be used as scale factors: e_data / e_MC.
    Returns a dictionary with al efficiencies.
    """
    efficiencies_data, efficiencies_data_ehigh, efficiencies_data_elow = ({} for _ in range(3))
    efficiencies_MC, efficiencies_MC_ehigh, efficiencies_MC_elow = ({} for _ in range(3)) 
    
    triggercomb = generateTriggerCombinations(args.triggers)
    for tcomb in triggercomb:
        comb_vars = dvars[ joinNTC(tcomb) ]
        assert len(comb_vars)==2
        var = comb_vars[0]
        
        inBaseName = ( 'trigSF_' + args.data_name + '_' + args.mc_name + '_' +
                       chn + '_' + var + '_' + joinNTC(tcomb) + args.subtag + '_CUTS*.root' )
        inName = os.path.join(args.indir_eff, chn, var, inBaseName)
        globName = glob.glob(inName)

        if len(globName) != 0: #some triggers do not fire for some channels: Ele32 for mutau (for example)
            efficiencies_data[joinNTC(tcomb)] = []
            efficiencies_MC[joinNTC(tcomb)] = []

            inFileName = min(globName, key=len) #select the shortest string (NoCut)
            inFile = TFile.Open(inFileName, 'READ')
            keyList = TIter(inFile.GetListOfKeys())

            for key in keyList:
                print(key)
                
                cl = gROOT.GetClass(key.GetClassName())
                if not cl.InheritsFrom("TGraph"):
                    continue
                h = key.ReadObj()

                assert(nbins[var][chn] == h.GetN())
                for datapoint in range(h.GetN()):
                    efficiencies_data[joinNTC(tcomb)].append( h.GetPointY(datapoint) )
                    efficiencies_data_elow[joinNTC(tcomb)].append( h.GetErrorYlow(datapoint) )
                    efficiencies_data_ehigh[joinNTC(tcomb)].append( h.GetErrorYhigh(datapoint) )

    return ( (efficiencies_data, efficiencies_data_ehigh, efficiencies_data_elow),
             (efficiencies_MC,   efficiencies_MC_ehigh,   efficiencies_MC_elow) )

def find_bin(edges, value):
    """Find the bin id corresponding to one value, given the bin edges."""
    return np.digitize(value, edges)

def effCalculator(args, efficiencies, eventvars, channel, dvars, binedges):
    eff_data, eff_mc = (0 for _ in range(2))

    triggercomb = generateTriggerCombinations(args.triggers)
    for tcomb in triggercomb:
        joincomb = joinNTC(tcomb)

        if joincomb in efficiencies[1][0] and joincomb not in efficiencies[0][0]:
            raise ValueError('This should never happen. Cannot be in MC but not in data.')
        
        #some triggers do not fire for some channels: Ele32 for mutau (for example)
        if joincomb in efficiencies[0][0]:

            variables = dvars[joincomb] 
            assert len(variables) == 2 #Change according to the discriminator

            binid = find_bin(binedges[variables[0]][channel], eventvars)
            # check for out-of-bounds
            assert binid!=0 and binid!=len(binedges[variables[0]][channel])

            term_data = efficiencies[0][0][joinNTC(tcomb)][binid]
            term_mc   = efficiencies[1][0][joinNTC(tcomb)][binid]

            if len(tcomb)%2==0:
                eff_data -= term_data
                ef_mc    -= term_mc
            else:
                eff_data += term_data
                eff_mc   += term_mc

    return eff_data, eff_mc

def runUnionWeightsCalculator_outputs(args):
    outputs = []
    outdict = {}
    for chn in args.channels:
        outdict[chn] = {}
        for var in _nbins_unionweights.keys():
            outdict[chn][var] = {}
            for ext in _extensions:
                name = '{}_{}_{}.{}'.format(args.outprefix, chn, var, ext)
                if ext == 'root':
                    name = os.path.join(args.outdir_root, name)
                else:
                    name = os.path.join(args.outdir_plots, chn, name)

                outputs.append(name)
                outdict[chn][var][ext] = name
            
    return outputs, outdict

def runUnionWeightsCalculator(args):
    _, outputs = runUnionWeightsCalculator_outputs(args)
    binedges, nbins = load_binning(afile=args.binedges_fname, key=args.subtag,
                                   variables=args.variables, channels=args.channels)

    h_union_weights = {}
    efficiencies = {}
    
    for chn in args.channels:
        h_union_weights[chn] = {}

        # load efficiencies
        efficiencies[chn] = effExtractor(args, chn, dvar, nbins)

        # initialize histograms
        for var in _nbins_unionweights.keys():
            nb_tmp = _nbins_unionweights[var]
            var_low, var_high = binedges[var][chn][0], binedges[var][chn][-1]
            h_union_weights[chn][var] = TH1D(var, var, nb_tmp, var_low, var_high)

    json_fname = os.path.join( args.indir_json, 'runVariableImportanceDiscriminator_{}.json'.format(chn) )
    with open(json_fname, 'r') as f:
        dvar = json.load(f)

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
            
            eff_data, eff_mc = effCalculator(args, efficiencies[chn], eventvars,
                                             chn, dvar, binedges)

            for var in _nbins_unionweights.keys():
                val = lfm.getLeaf(var)
                h_union_weights[chn][var].Fill(val, eff_data / eff_mc )


    file_id = ''.join( c for c in fileName[-10:] if c.isdigit() )
    for chn in args.channels:
        for var in _nbins_unionweights.keys():
            canvas_name = outputs[chn][var]['png'].split('.')[0]
            canvas = TCanvas(canvas_name, 'canvas', 600, 600 )
            h_union_weights[chn][var].SetLineColor(1)
            h_union_weights[chn][var].SetLineWidth(2)
            h_union_weights[chn][var].SetMarkerColor(1)
            h_union_weights[chn][var].SetMarkerSize(1.3)
            h_union_weights[chn][var].SetMarkerStyle(20)
            h_union_weights[chn][var].Draw()
            for ext in _extensions:
                if ext in ('png', 'pdf', 'C'):
                    canvas.SaveAs( outputs[chn][var][ext] )

            
            f_out = TFile(outputs[chn][var]['root'], 'RECREATE')
            f_out.cd()
            h_union_weights[chn][var].Write( get_histo_names('Closure')(chn,var) )

    print('Weights calculated for channel {}.'.format(chn))    
    # with open(outputs[i], 'w') as f:
    #     json.dump(orderedVars, f)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Choose the most significant variables to draw the efficiencies.')

    parser.add_argument('--binedges_fname', dest='binedges_fname', required=True, help='where the bin edges are stored')
    parser.add_argument('--indir_root', help='Original ROOT files directory',  required=True)
    parser.add_argument('--indir_json', help='Input directory where discriminator JSON files are stored',
                        required=True)
    parser.add_argument('--indir_eff', help='Input directory where intersection efficiencies are stored',
                        required=True)
    parser.add_argument('--sample', dest='sample', required=True, help='Process name as in SKIM directory')
    parser.add_argument('--file', dest='file_name', required=True, help='ID of input root file')
    parser.add_argument('--outdir_plots', help='Output plots directory', required=True)
    parser.add_argument('--outdir_root', help='Output directory for ROOT files', required=True)
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

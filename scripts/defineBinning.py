###### DOCSTRING ####################################################
# Defines the binning to be used for the efficiencies and scale
#   factors, using only the data. It does nothing if the user
#   explicitly specifies the binning.
# Run example:
# python3 -m scripts.defineBinning
# --indir /data_CMS/cms/portales/HHresonant_SKIMS/SKIMS_Radion_2018_fixedMETtriggers_mht_16Jun2021/
# --outdir .
# --tag binning_test
# --data MET2018A
# --variables met_et HT20 mht_et metnomu_et mhtnomu_et
# --subtag SUBTAG
####################################################################

import os
import h5py
import ROOT
import argparse

from utils.utils import getTriggerBit, LeafManager, set_pure_input_namespace
from luigi_conf import _binedges

def skipDataLoop(args):
    for var in args.variables:
        if var not in _binedges.keys():
            return False
    return True

@set_pure_input_namespace
def defineBinning_outputs(args):
    assert( os.path.splitext(args.binedges_filename)[1] == '.hdf5' )
    return os.path.join(args.outdir, args.binedges_filename)

@set_pure_input_namespace
def defineBinning(args):
    """
    Determine histogram quantiles
    """
    quant_down, quant_up = 0.05, 0.95

    # Initialization
    nTotEntries = 0
    _maxedge, _minedge = ({} for _ in range(2))
    for var in args.variables:
        _maxedge[var], _minedge[var] = ({} for _ in range(2))

    ###############################################
    ############## Data Loop: Start ###############
    ###############################################
    if not skipDataLoop(args):
        
        # Loop over all data datasets, calculating the quantiles in each dataset per variable
        for sample in args.data:

            #### Input list
            inputfiles = os.path.join(args.indir, 'SKIM_' + sample + '/goodfiles.txt')

            #### Parse input list
            filelist = []
            with open(inputfiles) as fIn:
                for line in fIn:
                    if '.root' in line:
                        if not os.path.exists(line[:-1]):
                            raise ValueError('[' + os.path.basename(__file__) + '] The input file does not exist: {}'.format(line))
                        filelist.append(line[:-1])

            #create chain of trees
            chain = ROOT.TChain('HTauTauTree')
            for line in filelist[:5]: #only looks at the first five files to speedup (accuracy loss)
                if args.debug:
                    print("Adding file: " + line)
                chain.Add(line)

            lf = LeafManager( 'Chain_HTauTauTree', chain )

            var_vectors = {k: [] for k in args.variables}
            treesize = chain.GetEntries()
            nTotEntries += treesize

            #ROOT.EnableImplicitMT()
            rdf = ROOT.RDataFrame(chain)
            rdf_counts = rdf.Count()
            arr = rdf.AsNumpy('met_et')
            print(arr)
            #rdf = rdf.Display({"met_et", "HT20"}, 30)
            #rdf.Print()
            quit()

            # for entry in range(0,treesize):
            #     chain.GetEntry(entry)
            #     if entry % 50000 == 0:
            #         print('Chain processing entry {}'.format(entry))
            #     for var in args.variables:
            #         var_vectors[var].append( lf.getLeaf(var) )
                    
            for var in args.variables:
                if var not in _binedges:
                    print('VAR::::::::::: {}'.format(var))
                    var_vectors[var] = rdframe.Take('std::vector<float>')(var)
                    print(var_vectors[var])
                    quit()

                    vsize = len(var_vectors[var])
                    assert(vsize == treesize)
                    var_vectors[var].sort()
                    var_vectors[var] = var_vectors[var][int(quant_down*vsize):int(quant_up*vsize)]
                    var_vectors[var] = (var_vectors[var][0], var_vectors[var][-1])
                    # multiply by the size of each sample (averaged at the end)
                    _minedge[var][sample] = treesize*var_vectors[var][0]
                    _maxedge[var][sample] = treesize*var_vectors[var][1]
                    if args.debug:
                        print('Quantiles of {} variable.'.format(var))
                        print('Q({qup})={qupval}; Q({qdown})={qdownval} (array size = {size})'.format(qup=quant_up,
                                                                                                  qupval=int(quant_up*vsize),
                                                                                                  qdown=quant_down,
                                                                                                  qdownval=int(quant_down*vsize),
                                                                                                  size=vsize))
                        print('Maximum={}; Minimum={}'.format(var_vectors[var][1],var_vectors[var][0]))
                else:
                    if args.debug:
                        print('Quantiles were not calculated. The custom bins for variable {} were instead used'
                              .format(var))

        # Do weighted average based on the number of events in each dataset
        maxmin = {}
        for var in args.variables:
            maxedge[var] = sum(_maxedge[var].values()) / nTotEntries
            minedge[var] = sum(_minedge[var].values()) / nTotEntries
            assert(maxedge[var] > minedge[var])
    ###############################################
    ############## Data Loop: End #################
    ###############################################
            
    for _ in (True,): #breakable scope (otherwise 'break' cannot be used)
        with h5py.File( defineBinning_outputs(args), 'a') as f:
            try:
                group = f.create_group(args.subtag)
            except ValueError:
                print('The HD5 group already existed. Skipping binning definition...')
                break
            for v in args.variables:
                dset = group.create_filename(v, dtype=float, shape=(args.nbins+1,))
                if v in _binedges:
                    if args.debug:
                        print( '[' + os.path.basename(__file__) + '] Using custom binning for variable {}: {}'
                               .format(v, _binedges[v]) )
                    dset[:] = _binedges[v]
                else:
                    _binwidth = (maxedge[v]-minedge[v])/args.nbins
                    _data = [minedge[v]+k*_binwidth for k in range(args.nbins+1)]
                    if args.debug:
                        print( '[' + os.path.basename(__file__) + '] Using regular binning for variable {}: {}'
                               .format(v, _data) )
                    dset[:] = _data

# -- Parse options
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Command line parser')

    parser.add_argument('--binedges_filename', dest='binedges_filename', required=True, help='in directory')
    parser.add_argument('--nbins',             dest='nbins',    required=True, help='number of X bins')
    parser.add_argument('-a', '--indir',       dest='indir',              required=True, help='in directory')
    parser.add_argument('-o', '--outdir',      dest='outdir',             required=True, help='out directory')
    parser.add_argument('-t', '--tag',         dest='tag',                required=True, help='tag')
    parser.add_argument('--subtag',            dest='subtag',             required=True, help='subtag')
    parser.add_argument('--data',              dest='data',               required=True, nargs='+', type=str,
                        help='list of datasets')                          
    parser.add_argument('--variables',        dest='variables',          required=True, nargs='+', type=str,
                        help='Select the variables to calculate the binning' )
    parser.add_argument('--debug', action='store_true', help='debug verbosity')
    args = parser.parse_args()

    defineBinning( args )

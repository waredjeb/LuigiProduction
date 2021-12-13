###### DOCSTRING ####################################################
# Defines the binning to be used for the efficiencies and scale
#   factors, using only the data. It does nothing if the user
#   explicitly specifies the binning.
# Run example:
# python3 -m scripts.defineBinning
#
####################################################################

import os
import hdf5

from utils.utils import getTriggerBit, LeafManager

from luigi_conf import _binedges

@utils.set_pure_input_namespace
def defineBinning_outputs(args):
    assert( os.path.splitext(args.binedges_dataset)[0] == 'hdf5' )
    return os.path.join(args.outir, args.binedges_dataset)

@utils.set_pure_input_namespace
def defineBinning(args):
    """
    Determine histogram quantiles
    """
    quant_down, quant_up = 0.05, 0.95
    nbins = 10 if args.nbins is None else args.nbins

    # Initialization
    nTotEntries = 0
    _maxedge, _minedge = ({} for _ in range(2))
    for v in variables:
        _maxedge[var], _minedge[var] = ({} for _ in range(2))

    # Loop over all data datasets, calcuating the quantiles in each dataset per variable
    for sample in args.data:
        fname = os.path.join(indir, 'SKIM_'+sample, fileName)
        if not os.path.exists(fname):
            raise ValueError('[' + os.path.basename(__file__) + '] The input files does not exist.')

        f_in = ROOT.TFile( fname )
        t_in = f_in.Get('HTauTauTree')
        
        var_vectors = {k: [] for k in variables}
        treesize = t_in.GetEntries()
        nTotEntries += treesize
        # This loop could be avoided when all binedges are defined via the configuration
        # but as soon as one variable is not defined there the loop is anyways required.
        # Optimization (removing this loop for a particular situation) is not important.
        for entry in range(0,treesize):
            t_in.GetEntry(entry)

            for var in variables:
                var_vectors[var].append( lf.getLeaf(var) )

        for var in variables:
            if var not in _binedges:
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
    for var in variables:
        maxedge[var] = sum(_maxedge[var].values()) / nTotEntries
        minedge[var] = sum(_minedge[var].values()) / nTotEntries
        assert(maxedge[var] > minedge[var])

    for _ in (True,): #breakable scope (otherwise 'break' cannot be used)
        with h5py.File(args.binedges_dataset, 'a') as f:
            try:
                group = f.create_group(args.subtag)
            except ValueError:
                print('The HD5 group already existed. Skipping binning definition...')
                break
            for v in args.variables:
                dset = group.create_dataset(v, dtype=float, shape=(nbins+1,))
                if v in _binedges:
                    if args.debug:
                        print( '[' + os.path.basename(__file__) + '] Using custom binning for variable {}: {}'
                               .format(v, _binedges[v]) )
                    dset[:] = _binedges[v]
                else:
                    _binwidth = (maxedge[v]-minedge[v])/nbins
                    _data = [minedge[v]+k*_binwidth for k in range(nbins+1)]
                    if args.debug:
                        print( '[' + os.path.basename(__file__) + '] Using regular binning for variable {}: {}'
                               .format(v, _data) )
                    dset[:] = _data

# -- Parse options
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Command line parser')

    parser.add_argument('--binedges_dataset', dest='binedges_dataset', required=True, help='in directory')
    parser.add_argument('--nbins',            dest='nbins',            required=True, help='number of X bins')
    parser.add_argument('-a', '--indir',      dest='indir',            required=True, help='in directory')
    parser.add_argument('-o', '--outdir',     dest='outdir',           required=True, help='out directory')
    parser.add_argument('-t', '--tag',        dest='tag',              required=True, help='tag')
    parser.add_argument('--subtag',           dest='subtag',           required=True, help='subtag')
    parser.add_argument('--data',             dest='data',             required=True, nargs='+', type=str,
                        help='list of datasets')
    parser.add_argument('--variables',        dest='variables',        required=True, nargs='+', type=str,
                        help='Select the variables to calculate the binning' )
    parser.add_argument('--debug', action='store_true', help='debug verbosity')
    args = parser.parse_args()

    defineBinning( args )

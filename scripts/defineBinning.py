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
import uproot as up
import pandas as pd
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
                        filelist.append(line[:-1] + ':HTauTauTree')

            treesize = 0
            quantiles = []
            for ib,batch in enumerate(up.iterate(files=filelist, expressions=args.variables,
                                                 step_size='10 GB', library='pd')):
                print('{}/{} files\r'.format(ib+1,len(filelist)), end="", flush=True)
                treesize += batch.shape[0]
                quantiles.append( batch.quantile([quant_down, quant_up]) )
                if ib>2: break
            
            quantiles = pd.concat(quantiles, axis=0).groupby(level=0).mean()
            nTotEntries += treesize

            for var in args.variables:
                if var not in _binedges:
                    _minedge[var][sample] = treesize*quantiles.loc[quant_down, var]
                    _maxedge[var][sample] = treesize*quantiles.loc[quant_up, var]

                    if args.debug:
                        print( '{} quantiles:  Q({})={}, Q({})={}'
                               .format(var, quant_down, _minedge[var][sample], quant_up, _maxedge[var][sample]) )
                else:
                    if args.debug:
                        print('Quantiles were not calculated. The custom bins for variable {} were instead used'
                              .format(var))

        # Do weighted average based on the number of events in each dataset
        maxedge, minedge = ({} for _ in range(2))
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
                dset = group.create_dataset(v, dtype=float, shape=(args.nbins+1,))
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

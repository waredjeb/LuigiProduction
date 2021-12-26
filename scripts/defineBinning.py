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
from luigi_conf import _binedges, _sel

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
    quant_down, quant_up = 0.00, 1.

    # Initialization
    nTotEntries = 0
    _maxedge, _minedge = ({} for _ in range(2))
    for var in args.variables:
        _maxedge[var], _minedge[var] = ({} for _ in range(2))
        for chn in args.channels:
            _maxedge[var][chn], _minedge[var][chn] = ({} for _ in range(2))

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
            quantiles = {k: [] for k in args.channels }
            branches = args.variables + ('pairType',)
            for ib,batch in enumerate(up.iterate(files=filelist, expressions=branches,
                                                 step_size='10 GB', library='pd')):
                print('{}/{} {} files\r'.format(ib+1,len(filelist),sample),
                      end='' if ib+1!=len(filelist) else '\n', flush=True)
                treesize += batch.shape[0]
                df_chn = {}
                for chn in args.channels:
                    if chn == 'all':
                        sel_chn = batch['pairType']<_sel[chn]['pairType'][1]
                    else:
                        sel_chn = batch['pairType']==_sel[chn]['pairType'][1]
                        
                    df_chn = batch[ sel_chn ]
                    quantiles[chn].append( df_chn.quantile([quant_down, quant_up]) )
                
            for chn in args.channels:
                quantiles[chn] = pd.concat(quantiles[chn], axis=0).groupby(level=0).mean()
            nTotEntries += treesize

            for var in args.variables:
                for chn in args.channels:
                    if var not in _binedges or chn not in _binedges[var]:
                        _minedge[var][chn][sample] = treesize*quantiles[chn].loc[quant_down, var]
                        _maxedge[var][chn][sample] = treesize*quantiles[chn].loc[quant_up, var]

                        if args.debug:
                            print( '{} quantiles in channel {}:  Q({})={}, Q({})={}'
                                   .format(var, chn,
                                           quant_down[chn], _minedge[var][chn][sample],
                                           quant_up[chn],   _maxedge[var][chn][sample]) )
                    else:
                        if args.debug:
                            print('Quantiles were not calculated. Custom bins for variable {} and channel {} were instead used.'
                                  .format(var, chn))

        # Do weighted average based on the number of events in each dataset
        maxedge, minedge = ({} for _ in range(2))
        for var in args.variables:
            maxedge[var], minedge[var] = ({} for _ in range(2))
            for chn in args.channels:
                maxedge[var].update({chn: sum(_maxedge[var][chn].values()) / nTotEntries})
                minedge[var].update({chn: sum(_minedge[var][chn].values()) / nTotEntries})
                assert(maxedge[var][chn] > minedge[var][chn])
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
                vargroup = group.create_group(v)
                for chn in args.channels:

                    try:
                        if chn in _binedges[v]:
                            dset = vargroup.create_dataset(chn, dtype=float, shape=(len(_binedges[v][chn]),))
                            if args.debug:
                                print( '[' + os.path.basename(__file__) + '] Using custom binning for variable {}: {}'
                                       .format(v, _binedges[v][chn]) )
                            dset[:] = _binedges[v][chn]
                        else:
                            raise KeyError #a "go-to" to the except clause

                    except KeyError:
                        dset = vargroup.create_dataset(chn, dtype=float, shape=(args.nbins+1,))
                        _binwidth = (maxedge[v][chn]-minedge[v][chn])/args.nbins
                        _data = [minedge[v][chn]+k*_binwidth for k in range(args.nbins+1)]
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
    parser.add_argument('-c', '--channels',   dest='channels',         required=True, nargs='+', type=str,
                        help='Select the channels over which the workflow will be run.' )
    parser.add_argument('--debug', action='store_true', help='debug verbosity')
    args = parser.parse_args()

    defineBinning( args )

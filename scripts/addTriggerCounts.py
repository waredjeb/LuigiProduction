import os
import re
import numpy as np
import argparse
import sys
sys.path.append(os.path.join(os.environ['CMSSW_BASE'], 'src', 'METTriggerStudies'))
from pathlib import Path

from utils import utils

from matplotlib import pyplot as plt, cm, colors
from matplotlib.font_manager import FontProperties
plt.rcParams.update({'font.size': 32})

@utils.setPureInputNamespace
def addTriggerCounts_outputs(args):
    extensions = ('png', 'txt')
    Path(args.outdir).mkdir(parents=False, exist_ok=True)
    t = tuple([] for _ in range(len(extensions)))
    for chn in args.channels:
        for i,ext in enumerate(extensions):
            t[i].append( os.path.join( args.outdir, args.dataset_name + '_' + chn + '_triggerCounts.' + ext ) )
    return t

@utils.setPureInputNamespace
def addTriggerCounts(args):
    """Adds ROOT histograms"""
    regex = re.compile( args.tprefix + '.+_[0-9]{1,5}' + args.subtag + '.txt' )

    inputs_join = []
    for smpl in args.samples:
        for root, dirs, files in os.walk( os.path.join(args.indir, smpl) ):
            for afile in files:
                if regex.match( os.path.basename(afile) ):
                    inputs_join.append( os.path.join(root, afile) )

    if len(inputs_join)==0:
        m =  '\nThe walk was performed in {} .\n'.format(os.path.join(args.indir, smpl))
        m += 'The regular expression was {}.\n'.format(regex.pattern)
        m += 'The regular expression could not retrieve any files.'
        raise ValueError(m)
        
    counter, counterRef = ({} for _ in range(2))
    for afile in inputs_join:
        with open(afile, 'r') as f:
            for line in f.readlines():
                trig, chn, count = [x.replace('\n', '') for x in line.split('\t')]
                counter.setdefault(chn, {})
                counterRef.setdefault(chn, 0)
                counter[chn].setdefault(trig, 0)
                
                if trig != 'Total':
                    counter[chn][trig] += int(count)
                else:
                    counterRef[chn] += int(count)

    outputs_png, outputs_txt = addTriggerCounts_outputs(args)

    for ic,k1 in enumerate(counter):
        triggers = np.array([x+' \n('+str(y)+')' for x,y in counter[k1].items()])
        vals = np.array(list(counter[k1].values()))
        assert(triggers.shape[0]==vals.shape[0])

        #sort
        vals, triggers = (np.array(t[::-1]) for t in zip(*sorted(zip(vals, triggers))))

        #remove zeros
        zeromask = vals == 0
        vals = vals[~zeromask]
        triggers = triggers[~zeromask]

        ncols = 4
        remainder = vals.shape[0] % ncols
        nrows = int(vals.shape[0] / ncols) + bool(remainder)

        #padding for obtaining a square table
        triggers = list(np.pad(triggers, (0,nrows*ncols-triggers.shape[0])))
        vals = list(np.pad(vals, (0,nrows*ncols-vals.shape[0])))

        with open( outputs_txt[ic], 'w') as ftxt:
            for i,j in zip(vals,triggers):
                if i != 0: #do not print the padding
                    #remove the extra info after the line break
                    ftxt.write(str(i) + '\t' +  str(j).split('\n')[0] + '\n')

        triggers = np.reshape(triggers, (nrows,ncols))
        vals = np.reshape(vals, (nrows,ncols)) + 0.001
        norm = colors.LogNorm(vals.min(), vals.max(), clip=False)

        ## Saving the same information on a pictur
        fig, ax = plt.subplots(figsize=(30, 16))
        # hide axes
        fig.patch.set_visible(False)
        ax.axis('off')
        ax.axis('tight')

        atable = plt.table( cellText=triggers,
                            colWidths = [0.07]*vals.shape[1],
                            loc='center',
                            cellLoc='center',
                            cellColours=plt.cm.Oranges_r(norm(vals))
                           )

        atable.scale(4,10)

        for (row, col), cell in atable.get_celld().items():
            cell.set_text_props(fontproperties=FontProperties(weight='bold'))
            
        plt.title( ( ('Data' if (smpl in args.data) else 'MC') +
                     ' (' + k1 + ': ' + str(counterRef[k1]) + ' events)' ),
                   fontdict={'fontsize': 70, 'fontweight': 30}
                   )
        plt.savefig( outputs_png[ic] )
        
# Run with:
# python3 /home/llr/cms/alves/CMSSW_12_2_0_pre1/src/METTriggerStudies/scripts/addTriggerCounts.py --indir /data_CMS/cms/alves/TriggerScaleFactors/CountsTest/ --outdir /data_CMS/cms/alves/TriggerScaleFactors/CountTest/ --samples SKIM_MET2018 --channels etau mutau tautau --subtag _default --tprefix count_ --triggers IsoMuIsoTau EleIsoTau VBFTau VBFTauHPS METNoMu120 IsoTau50 IsoTau180 --debug

# -- Parse input arguments
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Command line parser')

    parser.add_argument('--indir',       dest='indir',       required=True, help='SKIM directory')
    parser.add_argument('--outdir',      dest='outdir',      required=True, help='output directory')
    parser.add_argument('--samples',     dest='samples',     required=True, help='Process name as in SKIM directory', nargs='+', type=str)
    parser.add_argument('--dataset_name',dest='dset_name',   required=True, help='Name of the dataset.')
    parser.add_argument('--subtag',      dest='subtag',      required=True,
                        help='Additional (sub)tag to differ  entiate similar runs within the same tag.')
    parser.add_argument('--isData',      dest='isData',      required=True, help='Whether it is data or MC', type=int)
    parser.add_argument('--tprefix',     dest='tprefix',     required=True, help='Targets name prefix.')
    parser.add_argument('--channels',    dest='channels',    required=True, nargs='+', type=str,  
                        help='Select the channels over w  hich the workflow will be run.' )
    parser.add_argument('--triggers',    dest='triggers',    required=True, nargs='+', type=str,
                        help='Select the triggers over w  hich the workflow will be run.' )
    parser.add_argument('--debug', action='store_true', help='debug verbosity')

    args = parser.parse_args()

    addTriggerCounts( args )

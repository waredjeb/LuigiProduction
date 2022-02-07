import os
import re
import argparse
import sys
sys.path.append(os.path.join(os.environ['CMSSW_BASE'], 'src', 'METTriggerStudies'))
from utils import utils

@utils.set_pure_input_namespace
def addTriggerCount_outputs(args):
    t = os.path.join( args.indir, 'triggerCounts.html' ) #bokeh output
    print(t)

    return t

@utils.set_pure_input_namespace
def addTriggerCount(args):
    """Adds ROOT histograms"""
    target = addTriggerCount_outputs(args)
    regex = re.compile( 'count_.+_[0-9]{1,5}' + args.subtag + '.txt' )
    
    inputs_join = []
    for smpl in args.samples:
        for root, dirs, files in os.walk( os.path.join(args.indir, smpl) ):
            for afile in files:
                if regex.match( os.path.basename(afile) ):
                    print(root, afile)
                    inputs_join.append( os.path.join(root, afile) )

    for afile in inputs_join:
        with open(afile, 'r') as f:
            for line in f.readlines():
                print([x.replace('\n', '') for x in line.split('\t')])

    # reduce

# -- Parse input arguments
parser = argparse.ArgumentParser(description='Command line parser')

parser.add_argument('--indir',       dest='indir',       required=True, help='SKIM directory')
parser.add_argument('--outdir',      dest='outdir',      required=True, help='output directory')
parser.add_argument('--samples',     dest='samples',     required=True, help='Process name as in SKIM directory',
                    nargs='+', type=str)
parser.add_argument('--isData',      dest='isData',      required=True, help='Whether it is data or MC', type=int)
parser.add_argument('--subtag',      dest='subtag',      required=True,
                    help='Additional (sub)tag to differ  entiate similar runs within the same tag.')
parser.add_argument('--tprefix',     dest='tprefix',     required=True, help='Targets name prefix.')
parser.add_argument('--channels',    dest='channels',    required=True, nargs='+', type=str,  
                    help='Select t   he channels over w  hich the workflow will be run.' )
parser.add_argument('--triggers',    dest='triggers',    required=True, nargs='+', type=str,
                    help='Select t   he triggers over w  hich the workflow will be run.' )
parser.add_argument('--debug', action='store_true', help='debug verbosity')

args = parser.parse_args()

addTriggerCount( args )

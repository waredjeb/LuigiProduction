import os
import re
import json
import argparse

def discriminator(args, f, chn, var):
    """
    Associates each trigger combination to a set of variables, ordered by importance.
    The variables will be used to retrieve the corresponding trigger efficiencies when evaluating the scale factors.
    """
    result = {}
    
    for k in args.triggers:
        #get efficiency object only for single trigger scale factors

        keyList = ROOT.TIter(f.GetListOfKeys())

        print(keyList)

        # for key in keyList:
        #     cl = ROOT.gROOT.GetClass(key.GetClassName())
        #     if not cl.InheritsFrom("TGraph"):
        #         continue
        #     h = key.ReadObj()

        #     for datapoint in range(h.GetN()):
        #         print(h.GetPointX(datapoint), h.GetPointY(datapoint))
        #         print(h.GetErrorXlow(datapoint), h.GetErrorXhigh(datapoint))
        #         print(h.GetErrorYlow(datapoint), h.GetErrorYhigh(datapoint))
        #         print()

    return result

def discriminatorExecutor_outputs(args):
    return [ os.path.join(args.outdir, '{}_{}.json'.format(os.path.basename(__file__), chn))
             for chn in args.channels ]
        
def discriminatorExecutor(args):
    match = re.compile('')
    
    inName = os.path.join(args.indir, args.targetsPrefix, args.subtag )
    inFile = TFile.Open(inName)

    if args.debug:
        print('[=debug=] Open file: {}'.format(name_data))

    outputs = discriminatorExecutor_outputs(args)
    assert(len(outputs) == len(args.channels))
    
    for i,chn in enumerate(args.channels):
        #initialize discriminator related variables
        # obtain vars ordered by relative importance + metric (variance for instance)

        orderedVars = discriminator(args, inFile, chn)
        with open(outputs[i], 'w') as f:
            json.dump(orderedVars, f)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Choose the most significant variables to draw the efficiencies.')

    parser.add_argument('-i', '--indir', help='Inputs directory', required=True)
    parser.add_argument('-x', '--targetsPrefix', help='prefix to the names of the produced outputs (targets in luigi lingo)', required=True)
    parser.add_argument('-t', '--tag', help='string to diferentiate between different workflow runs', required=True)
    parser.add_argument('--debug', action='store_true', help='debug verbosity')
    args = parser.parse_args()

    discriminatorExecutor(args)

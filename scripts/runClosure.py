import os
import glob
import h5py
import argparse
import ctypes
import numpy as np
from copy import copy

import ROOT
ROOT.gROOT.SetBatch(True)
from ROOT import TCanvas
from ROOT import TPad
from ROOT import TFile
from ROOT import TEfficiency
from ROOT import TGraphAsymmErrors
from ROOT import TH2D
from ROOT import TLatex
from ROOT import TLegend

import sys
sys.path.append( os.path.join(os.environ['CMSSW_BASE'], 'src', 'METTriggerStudies'))

from utils.utils import (
    create_single_dir,
    find_bin,
    getKeyList,
    get_root_object,
    get_histo_names,
    load_binning,
)

from luigi_conf import (
    _extensions,
    _placeholder_cuts,
    _variables_unionweights,
)

def draw_single_eff(indir_union, indir_ref, channel, var, trig,
                    nbins, edges,
                    eff_names, prof_names, subtag, debug):
    #potentially many MC processes

    globfiles = []
    for proc in args.mc_processes:
        name_mc = os.path.join(indir_union, proc, args.inprefix + '*' + subtag + '.hdf5')
        globfiles.extend( glob.glob(name_mc) )

    nbins_eff = 6
    values = {}
    for i in range(nbins):
        values[str(i)] = []

    for globf in globfiles:
        indata = h5py.File(globf, mode='r')
        for i in range(nbins):
            values[str(i)].extend( indata[channel][var][trig][str(i)]['prob_ratios'][:] )

    # Check f everything is empty
    flag = True
    for i in range(nbins):
        flag = flag and len(values[str(i)])==0
    if flag:
        return False
        
    hname_mc = get_histo_names('Ref1D')(channel, var)
    if args.debug:
        print('MC Name: ', hname_mc)

    # get Y max and min edges
    ymax, ymin = -1., 2.
    for i in range(nbins):
        if len(values[str(i)]) > 0:
            if max(values[str(i)]) > ymax:
                ymax = max(values[str(i)])
            if min(values[str(i)]) < ymin:
                ymin = min(values[str(i)])

    histo_name = args.inprefix + '_' + channel + '_' + var + '_' + trig
    eff2D_mc = TH2D( histo_name,
                     histo_name,
                     nbins, edges[0], edges[-1],
                     nbins_eff, ymin, ymax)
    for ix in range(nbins):
        yvals = values[str(ix)]
        if len(yvals)>0:
            fake_xval = (edges[ix]+edges[ix+1])/2 #used only for filling the correct bin
            for yval in yvals:
                eff2D_mc.Fill(fake_xval, yval)

    if debug:
        print('[=debug=] Plotting...')  

    eff_canvas_name  = os.path.basename(eff_names[0]).split('.')[0]
    prof_canvas_name = os.path.basename(prof_names[0]).split('.')[0]

    #################################################################
    ############## DRAW 2D EFFICIENCIES #############################
    #################################################################
    eff_canvas = TCanvas( eff_canvas_name, eff_canvas_name, 600, 600 )
    eff_canvas.cd()

    eff2D_mc.SetLineColor(1)
    eff2D_mc.SetLineWidth(2)
    eff2D_mc.SetMarkerColor(1)
    eff2D_mc.SetMarkerSize(1.3)
    eff2D_mc.SetMarkerStyle(20)
    eff2D_mc.Draw('colz')

    lX, lY, lYstep = 0.25, 0.84, 0.1
    l = TLatex()
    l.SetNDC()
    l.SetTextFont(72)
    l.SetTextColor(1)
    l.DrawLatex( lX, lY,        'Channel: '+channel)
    l.DrawLatex( lX, lY-lYstep, 'Trigger: '+trig)

    for aname in eff_names:
      eff_canvas.SaveAs( aname )

    #################################################################
    ############## DRAW PROFILES ####################################
    #################################################################
    prof_canvas = TCanvas( prof_canvas_name, prof_canvas_name, 600, 600 )
    prof_canvas.cd()

    prof_canvas = TCanvas( eff_canvas_name, eff_canvas_name, 600, 600 )
    prof_canvas.cd()
    eff_prof = eff2D_mc.ProfileX()
    eff_prof.SetLineColor(1)
    eff_prof.SetLineWidth(2)
    eff_prof.SetMarkerColor(1)
    eff_prof.SetMarkerSize(1.3)
    eff_prof.SetMarkerStyle(20)

    l.DrawLatex( lX, lY,        'Channel: '+channel)
    l.DrawLatex( lX, lY-lYstep, 'Trigger: '+trig)

    for aname in prof_names:
      prof_canvas.SaveAs( aname )

def _get_plot_name(chn, var, trig, subtag, isprofile):
    """
    A 'XXX' placeholder is added for later replacement by all cuts considered
      for the same channel, variable and trigger combination.
    Without the placeholder one would have to additionally calculate the number
      of cuts beforehand, which adds complexity with no major benefit.
    """
    add = chn + '_' + var + '_' + trig
    prefix = 'ClosureEff_' if not isprofile else 'ClosureProf_'
    n = prefix + add + subtag
    n += _placeholder_cuts
    return n

#@setPureInputNamespace
def runClosure_outputs(outdir, channel, variables, triggers, subtag):
  outputs = [[] for _ in range(len(_extensions[:-1]))]
  outdict = {} #redundant but convenient
  
  for var in variables:
      outdict[var] = {}
      for trig in triggers:
          outdict[var][trig] = {}
          eff_name = _get_plot_name(channel, var, trig, subtag, isprofile=False)
          prof_name = _get_plot_name(channel, var, trig, subtag, isprofile=True)
          thisbase = os.path.join(outdir, channel, var, '')
          create_single_dir( thisbase )

          for ext,out in zip(_extensions[:-1], outputs):
              outdict[var][trig][ext] = {}
            
              eff_full = os.path.join( thisbase, eff_name + '.' + ext )
              out.append( eff_full )
              outdict[var][trig][ext]['eff'] = eff_full
              prof_full = os.path.join( thisbase, prof_name + '.' + ext )
              out.append( prof_full )
              outdict[var][trig][ext]['prof'] = prof_full
            
  #join all outputs in the same list
  return sum(outputs, []), outdict

def runClosure(indir_union, indir_ref,
               outdir,
               channel,
               variables,
               triggers,
               subtag,
               debug):
    ROOT.gStyle.SetOptStat(0)
    ROOT.gStyle.SetOptTitle(0)

    _, outs = runClosure_outputs(outdir, channel, variables, triggers, subtag)

    edges, nbins = load_binning(afile=args.binedges_fname, key=args.subtag,
                                variables=args.variables, channels=[channel])

    for ivar,var in enumerate(_variables_unionweights):
        for itrig,trig in enumerate(triggers):
            eff_names  = [ outs[var][trig][x]['eff']  for x in _extensions[:-1] ]
            prof_names = [ outs[var][trig][x]['prof'] for x in _extensions[:-1] ]

            draw_single_eff( indir_union, indir_ref, channel, var, trig,
                             nbins[var][channel], edges[var][channel],
                             eff_names, prof_names,
                             subtag, debug )

parser = argparse.ArgumentParser(description='Draw trigger scale factors')

parser.add_argument('--binedges_fname', dest='binedges_fname', required=True, help='where the bin edges are stored')
parser.add_argument('--indir_ref', required=True,
                    help='Input directory for data reference efficiencies')
parser.add_argument('--indir_union', required=True,
                    help='Input directory for MC corrected efficiencies')
parser.add_argument('--inprefix', dest='inprefix', required=True,
                    help='Closure data prefix.')
parser.add_argument('--mc_processes', dest='mc_processes', required=True, nargs='+', type=str,
                    help='Different MC processes considered.')
parser.add_argument('--outdir', help='Output directory', required=True, )
parser.add_argument('--channel', dest='channel', required=True,
                    help='Select the channels over which the workflow will be run.' )
parser.add_argument('--variables', dest='variables', required=True, nargs='+', type=str,
                    help='Select the variables over which the workflow will be run.' )
parser.add_argument('--triggers', dest='triggers', required=True, nargs='+', type=str,
                    help='Select the triggers over which the workflow will be run.' )
parser.add_argument('--subtag', dest='subtag', required=True, help='subtag')
parser.add_argument('--debug', action='store_true', help='debug verbosity')
args = parser.parse_args()

runClosure(args.indir_union, args.indir_ref,
           args.outdir,
           args.channel, args.variables, args.triggers,
           args.subtag, args.debug)

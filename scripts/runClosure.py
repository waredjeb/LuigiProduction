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

def get_ref_2Dhisto( indir_union, indir_eff, channel, var,
                     nbins, edges, prefix, data_name, mc_name,
                     subtag, debug ):
    globfiles = []
    for proc in args.mc_processes:
        name_mc = os.path.join(indir_union, proc, prefix + '*' + subtag + '.hdf5')
        globfiles.extend( glob.glob(name_mc) )

    nbins_eff = nbins
    values = {}
    for i in range(nbins):
        values[str(i)] = []

    for globf in globfiles:
        indata = h5py.File(globf, mode='r')
        for i in range(nbins):
            values[str(i)].extend( indata[channel][var][str(i)]['ref_prob_ratios'][:] )

    # Check f everything is empty
    flag = True
    for i in range(nbins):
        flag = flag and len(values[str(i)])==0
    if flag:
        return False
        
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

    histo_name = prefix + '_' + channel + '_' + var + '_ref'
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

    return eff2D_mc


def draw_single_eff( ref_histo, indir_union, indir_eff, channel, var, trig,
                     nbins, edges, prefix, data_name, mc_name,
                     eff_names, prof_names, subtag, debug ):
    #potentially many MC processes

    globfiles = []
    for proc in args.mc_processes:
        name_mc = os.path.join(indir_union, proc, prefix + '*' + subtag + '.hdf5')
        globfiles.extend( glob.glob(name_mc) )

    nbins_eff = nbins
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

    histo_name = prefix + '_' + channel + '_' + var + '_' + trig
    weights2D_mc = TH2D( histo_name,
                     histo_name,
                     nbins, edges[0], edges[-1],
                     nbins_eff, ymin, ymax)
    for ix in range(nbins):
        yvals = values[str(ix)]
        if len(yvals)>0:
            fake_xval = (edges[ix]+edges[ix+1])/2 #used only for filling the correct bin
            for yval in yvals:
                weights2D_mc.Fill(fake_xval, yval)

    eff2D_mc = weights2D_mc.Clone('eff2D')
    eff2D_mc.Divide(ref_histo)

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

    lX, lY, lYstep = 0.11, 0.95, 0.03
    l = TLatex()
    l.SetNDC()
    l.SetTextFont(72)
    l.SetTextColor(1)
    l.SetTextSize(0.03)
    l.DrawLatex( lX, lY,        'Channel: '+channel)
    l.DrawLatex( lX, lY-lYstep, 'Trigger: '+trig)

    for aname in eff_names:
      eff_canvas.SaveAs( aname )

    #################################################################
    ############## DRAW PROFILES ####################################
    #################################################################
    draw_options = 'ap'
    prof_canvas = TCanvas( prof_canvas_name, prof_canvas_name, 600, 600 )
    prof_canvas.cd()

    prof_canvas = TCanvas( eff_canvas_name + '2', eff_canvas_name + '2', 600, 600 )
    prof_canvas.cd()
    eff_prof = eff2D_mc.ProfileX(prof_canvas_name + '_prof', 1, -1, '')

    eff1D_name = prefix + data_name + '_' + mc_name + '_' + channel + '_' + var + '_' + trig + subtag + '*root'    
    eff1D_name = os.path.join(args.indir_eff, channel, var, eff1D_name)
    glob_name = glob.glob(eff1D_name)
    assert len(glob_name) > 0
    # CHANGE!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    eff1D_name = min(glob_name, key=len) #select the shortest string (NoCut)
    eff1D_file = TFile.Open(eff1D_name)

    eff1D_data = get_root_object('Data', eff1D_file)
    eff1D_mc = get_root_object('MC', eff1D_file)

    eff_prof.GetYaxis().SetTitle('Efficiency')
    eff_prof.GetXaxis().SetTitle(var)
    #eff_prof.GetXaxis().SetLabelOffset(1)
    eff_prof.GetYaxis().SetTitleSize(0.04)
    eff_prof.GetYaxis().SetTitleOffset(1.05)
    eff_prof.GetXaxis().SetLabelSize(0.03)
    eff_prof.GetYaxis().SetLabelSize(0.03)
    eff_prof.SetLineColor(ROOT.kGreen+3)
    eff_prof.SetLineWidth(2)
    eff_prof.SetMarkerColor(ROOT.kGreen+3)
    eff_prof.SetMarkerSize(1.3)
    eff_prof.SetMarkerStyle(20)

    def get_graph_max_min(graph, npoints, prof):
        vmax, vmin = 0, 1e10
        for point in range(npoints):
            if prof:
                val = graph.GetBinContent(point+1)
            else:
                val = graph.GetPointY(point)
            if val > vmax:
                vmax = val
            if val < vmin:
                vmin = val
        return vmax, vmin

    max1, min1 = get_graph_max_min(eff_prof,   nbins, True)
    max2, min2 = get_graph_max_min(eff1D_data, nbins, False)
    max3, min3 = get_graph_max_min(eff1D_mc,    nbins, False)
    prof_max = max([ max1, max2, max3 ])
    prof_min = min([ min1, min2, min3 ])
    eff_prof.SetMaximum(prof_max+0.3*(prof_max-prof_min))
    eff_prof.SetMinimum(prof_min-0.1*(prof_max-prof_min))
    eff_prof.Draw()

    eff1D_data.GetYaxis().SetTitleSize(0.04)
    eff1D_data.GetXaxis().SetLabelSize(0.03)
    eff1D_data.GetYaxis().SetLabelSize(0.03)
    eff1D_data.SetLineColor(ROOT.kBlack)
    eff1D_data.SetLineWidth(2)
    eff1D_data.SetMarkerColor(ROOT.kBlack)
    eff1D_data.SetMarkerSize(1.3)
    eff1D_data.SetMarkerStyle(20)
    eff1D_data.SetMaximum(1.1)
    eff1D_data.Draw('p same')

    eff1D_mc.GetYaxis().SetTitleSize(0.04)
    eff1D_mc.GetXaxis().SetLabelSize(0.03)
    eff1D_mc.GetYaxis().SetLabelSize(0.03)
    eff1D_mc.SetLineColor(ROOT.kRed)
    eff1D_mc.SetLineWidth(2)
    eff1D_mc.SetMarkerColor(ROOT.kRed)
    eff1D_mc.SetMarkerSize(1.3)
    eff1D_mc.SetMarkerStyle(20)
    eff1D_mc.SetMaximum(1.1)
    eff1D_mc.Draw('p same')

    l.DrawLatex( lX, lY,        'Channel: '+channel)
    l.DrawLatex( lX, lY-lYstep, 'Trigger: '+trig)

    leg = TLegend(0.62, 0.79, 0.96, 0.89)
    leg.SetFillColor(0)
    leg.SetShadowColor(0)
    leg.SetBorderSize(0)
    leg.SetTextSize(0.035)
    leg.SetFillStyle(0)
    leg.SetTextFont(42)
    leg.AddEntry(eff_prof, 'MC weighted', 'p')
    leg.AddEntry(eff1D_data, 'Data', 'p')
    leg.AddEntry(eff1D_mc, 'MC', 'p')
    leg.Draw('same')

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
def run_closure_outputs(outdir, channel, variables, triggers, subtag):
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

def run_closure(indir_union, indir_eff,
                outdir,
                channel,
                variables,
                triggers,
                subtag,
                prefix,
                data_name,
                mc_name,
                debug):
    ROOT.gStyle.SetOptStat(0)
    ROOT.gStyle.SetOptTitle(0)

    _, outs = run_closure_outputs(outdir, channel, variables, triggers, subtag)

    edges, nbins = load_binning(afile=args.binedges_fname, key=args.subtag,
                                variables=args.variables, channels=[channel])

    for ivar,var in enumerate(_variables_unionweights):
        refhisto = get_ref_2Dhisto( ref_histo, indir_union, indir_eff, channel, var,
                                    nbins[var][channel], edges[var][channel], prefix,
                                    data_name, mc_name,
                                    subtag, debug )

        for itrig,trig in enumerate(triggers):
            eff_names  = [ outs[var][trig][x]['eff']  for x in _extensions[:-1] ]
            prof_names = [ outs[var][trig][x]['prof'] for x in _extensions[:-1] ]

            draw_single_eff( ref_histo, indir_union, indir_eff, channel, var, trig,
                             nbins[var][channel], edges[var][channel], prefix,
                             data_name, mc_name,
                             eff_names, prof_names,
                             subtag, debug )

parser = argparse.ArgumentParser(description='Draw trigger scale factors')

parser.add_argument('--binedges_fname', dest='binedges_fname', required=True, help='where the bin edges are stored')
parser.add_argument('--indir_eff', required=True,
                    help='Input directory for data and unweighted MC efficiencies')
parser.add_argument('--indir_union', required=True,
                    help='Input directory for MC corrected efficiencies')
parser.add_argument('--inprefix', dest='inprefix', required=True,
                    help='Closure data prefix.')
parser.add_argument('--eff_prefix', dest='eff_prefix', required=True,
                    help='Data and unweighted MC efficiencies prefix.')
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
parser.add_argument('--data_name', dest='data_name', required=True, help='Data sample name')
parser.add_argument('--mc_name', dest='mc_name', required=True, help='MC sample name')

parser.add_argument('--debug', action='store_true', help='debug verbosity')
args = parser.parse_args()

run_closure( args.indir_union, args.indir_eff,
             args.outdir,
             args.channel, args.variables, args.triggers,
             args.subtag, args.eff_prefix,
             args.data_name, args.mc_name,
             args.debug )

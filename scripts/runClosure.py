import os
import json
import glob
import h5py
import argparse
import ctypes
import numpy as np
from copy import copy

import ROOT
ROOT.gROOT.SetBatch(True)
from ROOT import (
    gStyle,
    kBlack,
    kGreen,
    kRed,
    TCanvas,
    TPad,
    TFile,
    TEfficiency,
    TGraph,
    TGraphAsymmErrors,
    TH1D,
    TH2D,
    TLatex,
    TLegend,
)

import sys
sys.path.append( os.path.join(os.environ['CMSSW_BASE'], 'src', 'METTriggerStudies'))

from utils.utils import (
    create_single_dir,
    find_bin,
    getKeyList,
    generate_trigger_combinations,
    get_root_object,
    get_histo_names,
    load_binning,
)

from luigi_conf import (
    _extensions,
    _placeholder_cuts,
    _variables_unionweights,
)

def get_ref_obj( indir_union, channel, var, weightvar,
                   nbins, edges, prefix,
                   subtag ):
    globfiles = []
    for proc in args.mc_processes:
        name_mc = os.path.join(indir_union, proc, prefix + '*' + subtag + '.hdf5')
        globfiles.extend( glob.glob(name_mc) )

    values = {}
    for i in range(nbins):
        values[str(i)] = []

    for globf in globfiles:
        indata = h5py.File(globf, mode='r')
        for i in range(nbins):
            values[str(i)].extend( indata[channel][var][weightvar][str(i)]['ref_prob_ratios'][:] )

    # Check f everything is empty
    flag = True
    for i in range(nbins):
        flag = flag and len(values[str(i)])==0
    if flag:
        return False

    eff1d_ref = TGraph(nbins)
    for ix in range(nbins):
        yvals = values[str(ix)]
        if len(yvals)>0:
            yvals_sum = sum(yvals)
            fake_xval = (edges[ix]+edges[ix+1])/2 #used only for filling the correct bin
            eff1d_ref.SetPoint(ix, fake_xval, yvals_sum)

    return eff1d_ref


def draw_single_eff( ref_obj, indir_union, indir_eff, channel, var, weightvar, trig,
                     nbins, edges, in_prefix, eff_prefix, data_name, mc_name,
                     weights2d_names, prof_names, ref_names, subtag ):
    #potentially many MC processes

    globfiles = []
    for proc in args.mc_processes:
        name_mc = os.path.join(indir_union, proc, in_prefix + '*' + subtag + '.hdf5')
        globfiles.extend( glob.glob(name_mc) )

    nbins_eff = nbins
    values = {}
    for i in range(nbins):
        values[str(i)] = []

    for globf in globfiles:
        indata = h5py.File(globf, mode='r')
        for i in range(nbins):
            values[str(i)].extend( indata[channel][var][weightvar][trig][str(i)]['prob_ratios'][:] )

    # Check f everything is empty
    flag = True
    for i in range(nbins):
        flag = flag and len(values[str(i)])==0
    if flag:
        return False
        
    # get Y max and min edges
    ymax, ymin = -1., 2.
    for i in range(nbins):
        if len(values[str(i)]) > 0:
            if max(values[str(i)]) > ymax:
                ymax = max(values[str(i)])
            if min(values[str(i)]) < ymin:
                ymin = min(values[str(i)])

    histo_name = in_prefix + '_' + channel + '_' + var + '_' + weightvar + '_' + trig
    eff_prof = TGraph(nbins)
    #counts_prof = TGraph( histo_name + '_counts', histo_name + '_counts' )
    weights2d_mc = TH2D( histo_name + '_weights', histo_name + '_weights',
                         nbins, edges[0], edges[-1],
                         nbins_eff, ymin, ymax )

    for ix in range(nbins):
        yvals = values[str(ix)]
        yvals_count = len(yvals)
        if yvals_count>0:
            yvals_sum = sum(yvals)
            fake_xval = (edges[ix]+edges[ix+1])/2 #used only for filling the correct bin
            eff_prof.SetPoint(ix, fake_xval, yvals_sum)
            #counts_prof.AddPoint(fake_xval, yvals_count)
            for yval in yvals:
                weights2d_mc.Fill(fake_xval, yval)

    weights2d_canvas_name  = os.path.basename(weights2d_names[0]).split('.')[0]
    prof_canvas_name = os.path.basename(prof_names[0]).split('.')[0]
    ref_canvas_name = os.path.basename(ref_names[0]).split('.')[0]

    #################################################################
    ############## DRAW 1D WEIGHTED DISTRIBUTIONS ###################
    #################################################################
    ref_canvas = TCanvas( ref_canvas_name, ref_canvas_name, 600, 600 )
    ref_canvas.cd()

    ref_obj.GetYaxis().SetTitle('Weighted Yield')
    ref_obj.GetYaxis().SetTitleOffset(1.25)
    ref_obj.SetLineColor(1)
    ref_obj.SetLineWidth(2)
    ref_obj.SetMarkerColor(kRed)
    ref_obj.SetMarkerSize(1.3)
    ref_obj.SetMarkerStyle(20)
    ref_obj.SetLineColor(kRed)
    ref_obj.GetXaxis().SetTitle(var)
    ref_obj.Draw('ap')

    eff_prof.SetLineColor(1)
    eff_prof.SetLineWidth(2)
    eff_prof.SetMarkerColor(kBlack)
    eff_prof.SetMarkerSize(1.3)
    eff_prof.SetMarkerStyle(20)
    eff_prof.SetLineColor(kBlack)
    eff_prof.Draw('p same')
    #counts_prof.Draw('text same')

    lX, lY, lYstep = 0.20, 0.94, 0.03
    l = TLatex()
    l.SetNDC()
    l.SetTextFont(72)
    l.SetTextColor(1)
    l.SetTextSize(0.03)
    l.DrawLatex( lX, lY,        'Channel: {}   /   Single Trigger: {} '.format(channel,trig))
    l.DrawLatex( lX, lY-lYstep, 'MC weighted by {}'.format(weightvar))

    leg = TLegend(0.62, 0.65, 0.96, 0.9)
    leg.SetFillColor(0)
    leg.SetShadowColor(0)
    leg.SetBorderSize(0)
    leg.SetTextSize(0.035)
    leg.SetFillStyle(0)
    leg.SetTextFont(42)
    leg.AddEntry(ref_obj, '#sum_{i}^{all}w_{i}', 'p')
    leg.AddEntry(eff_prof, '#sum_{i}^{pass trigger}w_{i}', 'p')
    leg.Draw('same')
    
    for aname in ref_names:
      ref_canvas.SaveAs( aname )

    #################################################################
    ############## DRAW 2D EFFICIENCIES #############################
    #################################################################
    weights2d_canvas = TCanvas( weights2d_canvas_name, weights2d_canvas_name, 600, 600 )
    weights2d_canvas.cd()

    weights2d_mc.GetYaxis().SetTitle('P_{Data} / P_{MC}')
    weights2d_mc.GetYaxis().SetTitleOffset(1.05)
    weights2d_mc.SetLineColor(1)
    weights2d_mc.SetLineWidth(2)
    weights2d_mc.SetMarkerColor(1)
    weights2d_mc.SetMarkerSize(1.3)
    weights2d_mc.SetMarkerStyle(20)
    weights2d_mc.Draw('colz')

    lX, lY, lYstep = 0.11, 0.94, 0.03
    l = TLatex()
    l.SetNDC()
    l.SetTextFont(72)
    l.SetTextColor(1)
    l.SetTextSize(0.03)
    l.DrawLatex( lX, lY,        'Channel: {}   /   Single Trigger: {} '.format(channel,trig))
    l.DrawLatex( lX, lY-lYstep, 'MC weighted by {}'.format(weightvar))

    for aname in weights2d_names:
      weights2d_canvas.SaveAs( aname )

    #################################################################
    ############## DRAW PROFILES ####################################
    #################################################################
    prof_canvas = TCanvas( prof_canvas_name, prof_canvas_name, 600, 600 )
    prof_canvas.cd()

    eff1d_name = eff_prefix + data_name + '_' + mc_name + '_' + channel + '_' + var + '_' + trig + subtag + '*root'    
    eff1d_name = os.path.join(indir_eff, channel, var, eff1d_name)
    glob_name = glob.glob(eff1d_name)
    assert len(glob_name) > 0
    # CHANGE!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    eff1d_name = min(glob_name, key=len) #select the shortest string (NoCut)
    eff1d_file = TFile.Open(eff1d_name)

    eff1d_data = get_root_object('Data', eff1d_file)
    eff1d_mc = get_root_object('MC', eff1d_file)

    # get the efficiency by dividing the weighted yields
    for point in range(nbins):
        eff_prof.SetPoint(point,
                          eff_prof.GetPointX(point),
                          eff_prof.GetPointY(point) / ref_obj.GetPointY(point) )

    eff_prof.GetYaxis().SetTitle('Efficiency')
    eff_prof.GetXaxis().SetTitle(var)
    #eff_prof.GetXaxis().SetLabelOffset(1)
    eff_prof.GetYaxis().SetTitleSize(0.04)
    eff_prof.GetYaxis().SetTitleOffset(1.05)
    eff_prof.GetXaxis().SetLabelSize(0.03)
    eff_prof.GetYaxis().SetLabelSize(0.03)
    eff_prof.SetLineColor(kGreen+3)
    eff_prof.SetLineWidth(2)
    eff_prof.SetMarkerColor(kGreen+3)
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

    max1, min1 = get_graph_max_min(eff_prof,   nbins, False)
    max2, min2 = get_graph_max_min(eff1d_data, nbins, False)
    max3, min3 = get_graph_max_min(eff1d_mc,    nbins, False)
    prof_max = max([ max1, max2, max3 ])
    prof_min = min([ min1, min2, min3 ])
    eff_prof.SetMaximum(prof_max+0.3*(prof_max-prof_min))
    eff_prof.SetMinimum(prof_min-0.1*(prof_max-prof_min))
    eff_prof.Draw('ap')

    eff1d_data.GetYaxis().SetTitleSize(0.04)
    eff1d_data.GetXaxis().SetLabelSize(0.03)
    eff1d_data.GetYaxis().SetLabelSize(0.03)
    eff1d_data.SetLineColor(kBlack)
    eff1d_data.SetLineWidth(2)
    eff1d_data.SetMarkerColor(kBlack)
    eff1d_data.SetMarkerSize(1.3)
    eff1d_data.SetMarkerStyle(20)
    eff1d_data.SetMaximum(1.1)
    eff1d_data.Draw('p same')

    eff1d_mc.GetYaxis().SetTitleSize(0.04)
    eff1d_mc.GetXaxis().SetLabelSize(0.03)
    eff1d_mc.GetYaxis().SetLabelSize(0.03)
    eff1d_mc.SetLineColor(kRed)
    eff1d_mc.SetLineWidth(2)
    eff1d_mc.SetMarkerColor(kRed)
    eff1d_mc.SetMarkerSize(1.3)
    eff1d_mc.SetMarkerStyle(20)
    eff1d_mc.SetMaximum(1.1)
    eff1d_mc.Draw('p same')

    l.DrawLatex( lX, lY,        'Channel: {}   /   Single Trigger: {} '.format(channel,trig))
    l.DrawLatex( lX, lY-lYstep, 'MC weighted by {}'.format(weightvar))

    leg = TLegend(0.62, 0.79, 0.96, 0.89)
    leg.SetFillColor(0)
    leg.SetShadowColor(0)
    leg.SetBorderSize(0)
    leg.SetTextSize(0.035)
    leg.SetFillStyle(0)
    leg.SetTextFont(42)
    leg.AddEntry(eff_prof, 'MC weighted', 'p')
    leg.AddEntry(eff1d_data, 'Data', 'p')
    leg.AddEntry(eff1d_mc, 'MC', 'p')
    leg.Draw('same')

    for aname in prof_names:
      prof_canvas.SaveAs( aname )

def _get_plot_name(chn, var, wvar, trig, subtag, prefix):
    """
    A 'XXX' placeholder is added for later replacement by all cuts considered
      for the same channel, variable and trigger combination.
    Without the placeholder one would have to additionally calculate the number
      of cuts beforehand, which adds complexity with no major benefit.
    """
    add = chn + '_' + var + '_' + wvar + '_' + trig
    prefix = 'Closure' + prefix + '_'
    n = prefix + add + subtag
    n += _placeholder_cuts
    return n

#@setPureInputNamespace
def run_closure_outputs(outdir, channel, variables, weightvars, triggers, subtag):
    outputs = [[] for _ in range(len(_extensions[:-1]))]
    outdict = {} #redundant but convenient

    for var in variables:
        outdict[var] = {}
        for weightvar in weightvars:
            outdict[var][weightvar] = {}
            for trig in triggers:
                outdict[var][weightvar][trig] = {}

                outname = lambda x: _get_plot_name(channel, var, weightvar, trig, subtag, prefix=x)
                weights2d_name = outname('Weights')
                prof_name = outname('Prof')
                ref_name = outname('Ref')
                
                thisbase = os.path.join(outdir, channel, var, '')
                create_single_dir( thisbase )

                for ext,out in zip(_extensions[:-1], outputs):
                    outdict[var][weightvar][trig][ext] = {}

                    weights2d_full = os.path.join( thisbase, weights2d_name + '.' + ext )
                    out.append( weights2d_full )
                    outdict[var][weightvar][trig][ext]['weights2d'] = weights2d_full
                    prof_full = os.path.join( thisbase, prof_name + '.' + ext )
                    out.append( prof_full )
                    outdict[var][weightvar][trig][ext]['prof'] = prof_full
                    ref_full = os.path.join( thisbase, ref_name + '.' + ext )
                    out.append( ref_full )
                    outdict[var][weightvar][trig][ext]['ref'] = ref_full

    #join all outputs in the same list
    return sum(outputs, []), outdict

def run_closure( indir_union,
                 indir_eff,
                 outdir,
                 channel,
                 variables,
                 triggers,
                 subtag,
                 in_prefix,
                 eff_prefix,
                 data_name,
                 mc_name ):
    gStyle.SetOptStat(0)
    gStyle.SetOptTitle(0)
    gStyle.SetPaintTextFormat("4.4f");

    edges, nbins = load_binning(afile=args.binedges_fname, key=args.subtag,
                                variables=args.variables, channels=[channel])

    # load efficiency variables obtained previously
    effvars = {}
    json_name = os.path.join(args.indir_json,
                             'runVariableImportanceDiscriminator_{}.json'.format(channel))
    with open(json_name, 'r') as f:
        effvars[channel] = json.load(f)
    weightvars = effvars[channel][generate_trigger_combinations(args.triggers)[0][0]][0]
    
    _, outs = run_closure_outputs(outdir, channel, variables, weightvars, triggers, subtag)
    
    for ivar,var in enumerate(_variables_unionweights):
        #any trigger works for the constant list
        for weightvar in weightvars:
            ref_obj = get_ref_obj( indir_union, channel, var, weightvar,
                                   nbins[var][channel], edges[var][channel], in_prefix,
                                   subtag )

            for trig in triggers:
                weights2d_names  = [ outs[var][weightvar][trig][x]['weights2d']  for x in _extensions[:-1] ]
                prof_names = [ outs[var][weightvar][trig][x]['prof'] for x in _extensions[:-1] ]
                ref_names = [ outs[var][weightvar][trig][x]['ref'] for x in _extensions[:-1] ]
                draw_single_eff( ref_obj, indir_union, indir_eff, channel, var, weightvar, trig,
                                 nbins[var][channel], edges[var][channel],
                                 in_prefix, eff_prefix,
                                 data_name, mc_name,
                                 weights2d_names, prof_names, ref_names,
                                 subtag )

parser = argparse.ArgumentParser(description='Draw trigger scale factors')

parser.add_argument('--binedges_fname', dest='binedges_fname', required=True, help='where the bin edges are stored')
parser.add_argument('--indir_eff', required=True,
                    help='Input directory for data and unweighted MC efficiencies')
parser.add_argument('--indir_union', required=True,
                    help='Input directory for MC corrected efficiencies')
parser.add_argument('--indir_json'
                    , help='Input directory where discriminator JSON files are stored',
                    required=True)
parser.add_argument('--in_prefix', dest='in_prefix', required=True,
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
             args.subtag,
             args.in_prefix, args.eff_prefix,
             args.data_name, args.mc_name )

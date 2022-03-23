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
    TGraphAsymmErrors,
    TH2D,
    TLatex,
    TLegend,
    TLine,
    TMath,
)
import array
import sys
sys.path.append( os.path.join(os.environ['CMSSW_BASE'], 'src', 'METTriggerStudies'))

from utils.utils import (
    create_single_dir,
    find_bin,
    generate_trigger_combinations,
    get_root_object,
    get_histo_names,
    load_binning,
    redraw_border,
)

from luigi_conf import (
    _extensions,
    _placeholder_cuts,
    _variables_unionweights,
)

def get_div_error_propagation(num, den, enum, eden):
    """Ratio propagation of errors (numerator and denominator as arguments)"""
    if num==0. or den==0.:
        return 0.
    first = (enum*enum)/(num*num)
    second = (eden*eden)/(den*den)
    val = num/den
    return val * TMath.Sqrt(first + second)

def get_ref_obj( indir_union, channel, var, weightvar,
                 nbins, edges, prefix,
                 subtag ):
    globfiles = []
    for proc in args.mc_processes:
        name_mc = os.path.join(indir_union, proc, 'Closure_' + args.closure_single_trigger[0],
                               prefix + '*' + subtag + '.hdf5')
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

    graph_xvals, graph_yvals = ([] for _ in range(2))
    graph_exvals_low, graph_exvals_high = ([] for _ in range(2))
    graph_eyvals_low, graph_eyvals_high = ([] for _ in range(2))
    for ix in range(nbins):
        yvals = values[str(ix)]
        fake_xval = (edges[ix]+edges[ix+1])/2 #used only for filling the correct bin
        graph_xvals.append( fake_xval )
        graph_exvals_low.append( abs(fake_xval-edges[ix]) )
        graph_exvals_high.append( abs(fake_xval-edges[ix+1]) )
        if len(yvals)>0:
            yvals_sum = sum(yvals)
            graph_yvals.append( yvals_sum )
            graph_eyvals_low.append( TMath.Sqrt(yvals_sum)/2 )
            graph_eyvals_high.append( TMath.Sqrt(yvals_sum)/2 )
        else:
            graph_yvals.append(0.)
            graph_eyvals_low.append(0.)
            graph_eyvals_high.append(0.)

    
    eff1d_ref = TGraphAsymmErrors( nbins,
                                   array.array('d', graph_xvals),
                                   array.array('d', graph_yvals),
                                   array.array('d', graph_exvals_low),
                                   array.array('d', graph_exvals_high),
                                   array.array('d', graph_eyvals_low),
                                   array.array('d', graph_eyvals_high) )
    return eff1d_ref


def draw_single_eff( ref_obj, indir_union, indir_eff, channel, var, weightvar, trig,
                     nbins, edges, in_prefix, eff_prefix, data_name, mc_name,
                     weights2d_names, prof_names, ref_names, subtag, debug ):
    #potentially many MC processes
    globfiles = []
    for proc in args.mc_processes:
        name_mc = os.path.join(indir_union, proc, 'Closure_' + args.closure_single_trigger[0],
                               in_prefix + '*' + subtag + '.hdf5')
        globfiles.extend( glob.glob(name_mc) )

    nbins_eff = nbins
    values = {}
    for i in range(nbins):
        values[str(i)] = []

    for globf in globfiles:
        indata = h5py.File(globf, mode='r')
        for i in range(nbins):
            values[str(i)].extend( indata[channel][var][weightvar][trig][str(i)]['prob_ratios'][:] )

    # Check if everything is empty
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
    graph_xvals, graph_yvals = ([] for _ in range(2))
    graph_exvals_low, graph_exvals_high = ([] for _ in range(2))
    graph_eyvals_low, graph_eyvals_high = ([] for _ in range(2))

    weights2d_mc = TH2D( histo_name + '_weights', histo_name + '_weights',
                         nbins, edges[0], edges[-1],
                         nbins_eff, ymin, ymax )

    for ix in range(nbins):
        yvals = values[str(ix)]
        yvals_count = len(yvals)
        fake_xval = (edges[ix]+edges[ix+1])/2 #used only for filling the correct bin
        graph_xvals.append( fake_xval )
        graph_exvals_low.append( abs(fake_xval-edges[ix]) )
        graph_exvals_high.append( abs(fake_xval-edges[ix+1]) )
        if yvals_count>0:
            yvals_sum = sum(yvals)
            graph_yvals.append(yvals_sum)
            graph_eyvals_low.append( TMath.Sqrt(yvals_sum)/2 )
            graph_eyvals_high.append( TMath.Sqrt(yvals_sum)/2 )

            for yval in yvals:
                weights2d_mc.Fill(fake_xval, yval)
        else:
            graph_yvals.append(0.)
            graph_eyvals_low.append(0.)
            graph_eyvals_high.append(0.)


    eff_prof = TGraphAsymmErrors( nbins,
                                  array.array('d', graph_xvals),
                                  array.array('d', graph_yvals),
                                  array.array('d', graph_exvals_low),
                                  array.array('d', graph_exvals_high),
                                  array.array('d', graph_eyvals_low),
                                  array.array('d', graph_eyvals_high) )

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

    lX, lY, lYstep = 0.20, 0.94, 0.03
    l = TLatex()
    l.SetNDC()
    l.SetTextFont(72)
    l.SetTextColor(1)
    l.SetTextSize(0.03)
    l.DrawLatex( lX, lY, 'Single Trigger Closure, MC weighted by {}'.format(weightvar))
    l.DrawLatex( lX, lY-lYstep, 'Channel: {}   /   Single Trigger: {} '.format(channel,trig))

    leg = TLegend(0.62, 0.7, 0.96, 0.9)
    leg.SetFillColor(0)
    leg.SetShadowColor(0)
    leg.SetBorderSize(0)
    leg.SetTextSize(0.05)
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
    weights2d_mc.GetYaxis().SetTitleOffset(.7)
    weights2d_mc.SetLineColor(1)
    weights2d_mc.SetLineWidth(2)
    weights2d_mc.SetMarkerColor(1)
    weights2d_mc.SetMarkerSize(1.3)
    weights2d_mc.SetMarkerStyle(20)
    weights2d_mc.Draw('colz')

    lX, lY, lYstep = 0.21, 0.94, 0.03
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

    for point in range(nbins):
        orig_yvalue = eff_prof.GetPointY(point)
        eff_prof.SetPoint(point,
                          eff_prof.GetPointX(point),
                          orig_yvalue / ref_obj.GetPointY(point) )
        eff_prof.SetPointEXlow(point, eff_prof.GetErrorXlow(point))
        eff_prof.SetPointEXhigh(point, eff_prof.GetErrorXhigh(point))
        binomial_variance = ref_obj.GetPointY(point)*eff_prof.GetPointY(point)*(1-eff_prof.GetPointY(point))
        binomial_std = TMath.Sqrt(binomial_variance)
        binomial_ratio_std = get_div_error_propagation( binomial_std, #eff_prof.GetPointY(point)
                                                        ref_obj.GetPointY(point),
                                                        eff_prof.GetErrorXlow(point) + eff_prof.GetErrorXhigh(point),
                                                        ref_obj.GetErrorXlow(point)  + ref_obj.GetErrorXhigh(point),
                                                       )
        eff_prof.SetPointEYlow(point, binomial_ratio_std/2)
        eff_prof.SetPointEYhigh(point, binomial_ratio_std/2)

    pad1 = TPad('pad1', 'pad1', 0, 0.35, 1, 1)
    pad1.SetBottomMargin(0.005)
    pad1.SetLeftMargin(0.2)
    pad1.Draw()
    pad1.cd()

    def get_obj_max_min(graph, npoints, ishisto):
        vmax, vmin = 0, 1e10
        for point in range(npoints):
            if ishisto:
                val = graph.GetBinContent(point+1)
            else:
                val = graph.GetPointY(point)
            if val > vmax:
                vmax = val
            if val < vmin:
                vmin = val
        return vmax, vmin

    max1, min1 = get_obj_max_min(eff_prof,   nbins, False)
    max2, min2 = get_obj_max_min(eff1d_data, nbins, False)
    max3, min3 = get_obj_max_min(eff1d_mc,    nbins, False)
    prof_max = max([ max1, max2, max3 ])
    prof_min = min([ min1, min2, min3 ])
    
    halfbinwidths = (edges[1:]-edges[:-1])/2
    axor_info = nbins+1, -1, nbins
    axor_ndiv = 605, 705
    axor = TH2D( 'axor', 'axor',
                 axor_info[0], axor_info[1], axor_info[2],
                 100, prof_min-0.1*(prof_max-prof_min), prof_max+0.4*(prof_max-prof_min) )
    axor.GetXaxis().SetNdivisions(axor_ndiv[0])
    axor.GetYaxis().SetNdivisions(axor_ndiv[1])
    axor.GetXaxis().SetTickLength(0)
    axor.GetYaxis().SetTitle('Efficiency')
    axor.GetYaxis().SetTitleSize(0.07)
    axor.GetYaxis().SetTitleOffset(1.05)
    axor.GetYaxis().SetLabelSize(0.06)
    axor.Draw()

    ########################
    # Change X axis labels #
    ########################
    eff_prof_new = TGraphAsymmErrors( nbins )
    for ip in range(eff_prof.GetN()):
        eff_prof_new.SetPoint(ip, ip, eff_prof.GetPointY(ip) )
        eff_prof_new.SetPointError(ip, .5, .5,
                                   eff_prof.GetErrorYlow(ip), eff_prof.GetErrorYhigh(ip) )

    eff1d_mc_new = TGraphAsymmErrors( nbins )
    for ip in range(eff1d_mc.GetN()):
        eff1d_mc_new.SetPoint(ip, ip, eff1d_mc.GetPointY(ip) )
        eff1d_mc_new.SetPointError(ip, .5, .5,
                                   eff1d_mc.GetErrorYlow(ip), eff1d_mc.GetErrorYhigh(ip) )

    eff1d_data_new = TGraphAsymmErrors( nbins )
    for ip in range(eff1d_data.GetN()):
        eff1d_data_new.SetPoint(ip, ip, eff1d_data.GetPointY(ip) )
        eff1d_data_new.SetPointError(ip, .5, .5,
                                   eff1d_data.GetErrorYlow(ip), eff1d_data.GetErrorYhigh(ip) )

    eff_prof_new.SetLineColor(kGreen+3)
    eff_prof_new.SetLineWidth(2)
    eff_prof_new.SetMarkerColor(kGreen+3)
    eff_prof_new.SetMarkerSize(1.3)
    eff_prof_new.SetMarkerStyle(20)
    eff_prof_new.Draw('same p0')

    eff1d_data_new.SetLineColor(kBlack)
    eff1d_data_new.SetLineWidth(2)
    eff1d_data_new.SetMarkerColor(kBlack)
    eff1d_data_new.SetMarkerSize(1.3)
    eff1d_data_new.SetMarkerStyle(20)
    eff1d_data_new.Draw('same p0')

    eff1d_mc_new.SetLineColor(kRed)
    eff1d_mc_new.SetLineWidth(2)
    eff1d_mc_new.SetMarkerColor(kRed)
    eff1d_mc_new.SetMarkerSize(1.3)
    eff1d_mc_new.SetMarkerStyle(20)
    eff1d_mc_new.Draw('same p0 e')

    l.DrawLatex( lX, lY,        'Channel: {}   /   Single Trigger: {} '.format(channel,trig))
    l.DrawLatex( lX, lY-lYstep, 'MC weighted by {}'.format(weightvar))

    pad1.RedrawAxis()
    l = TLine()
    l.SetLineWidth(2)
    padmin = prof_min-0.1*(prof_max-prof_min)
    padmax = prof_max+0.1*(prof_max-prof_min)
    fraction = (padmax-padmin)/45
    for i in range(nbins):
      x = axor.GetXaxis().GetBinLowEdge(i) + 1.5;
      l.DrawLine(x,padmin-fraction,x,padmin+fraction)
    l.DrawLine(x+1,padmin-fraction,x+1,padmin+fraction)

    
    leg = TLegend(0.62, 0.73, 0.96, 0.89)
    leg.SetFillColor(0)
    leg.SetShadowColor(0)
    leg.SetBorderSize(0)
    leg.SetTextSize(0.05)
    leg.SetFillStyle(0)
    leg.SetTextFont(42)
    leg.AddEntry(eff_prof_new, 'MC weighted', 'p')
    leg.AddEntry(eff1d_data_new, 'Data', 'p')
    leg.AddEntry(eff1d_mc_new, 'MC', 'p')
    leg.Draw('same')

    prof_canvas.cd()
    pad2 = TPad('pad2','pad2',0,0.0,1,0.35)
    pad2.SetTopMargin(0.01)
    pad2.SetBottomMargin(0.4)
    pad2.SetLeftMargin(0.2)
    pad2.Draw()
    pad2.cd()

    x_data, y_data   = ( [] for _ in range(2) )
    eu_data, ed_data = ( [] for _ in range(2) )
    x_prof, y_prof   = ( [] for _ in range(2) )
    eu_prof, ed_prof = ( [] for _ in range(2) )
    x_mc, y_mc   = ( [] for _ in range(2) )
    eu_mc, ed_mc = ( [] for _ in range(2) )

    x_sf1, y_sf1   = ( [] for _ in range(2) )
    eu_sf1, ed_sf1 = ( [] for _ in range(2) )
    x_sf2, y_sf2   = ( [] for _ in range(2) )
    eu_sf2, ed_sf2 = ( [] for _ in range(2) )

    for i in range(nbins):
        #ctypes conversions needed
        x_mc.append( ctypes.c_double(0.) )
        y_mc.append( ctypes.c_double(0.) )
        eff1d_mc.GetPoint(i, x_mc[i], y_mc[i])
        x_mc[i] = x_mc[i].value
        y_mc[i] = y_mc[i].value

        eu_mc.append( eff1d_mc.GetErrorYhigh(i) )
        ed_mc.append( eff1d_mc.GetErrorYlow(i) )
        if debug:
            print('xp[{}] = {} - yp[{}] = {} +{}/-{}\n'.format(i,x_mc[i],iy_mc[i],eu_mc[i],ed_mc[i]))

        x_prof.append( ctypes.c_double(0.) )
        y_prof.append( ctypes.c_double(0.) )
        x_data.append( ctypes.c_double(0.) )
        y_data.append( ctypes.c_double(0.) )
        eff_prof.GetPoint(i, x_prof[i], y_prof[i])
        eff1d_data.GetPoint(i, x_data[i], y_data[i])
        x_prof[i] = x_prof[i].value
        y_prof[i] = y_prof[i].value
        x_data[i] = x_data[i].value
        y_data[i] = y_data[i].value

        eu_data.append( eff1d_data.GetErrorYhigh(i) )
        ed_data.append( eff1d_data.GetErrorYlow(i) )
        eu_prof.append( eff_prof.GetErrorYhigh(i) )
        ed_prof.append( eff_prof.GetErrorYlow(i) )
        
        if debug:
            print('xp[{}] = {} - yp[{}] = {} +{}/-{}\n'.format(i,x_prof[i],i,y_prof[i],eu_prof[i],ed_prof[i]))
            print('xp[{}] = {} - yp[{}] = {} +{}/-{}\n'.format(i,x_data[i],i,y_data[i],eu_data[i],ed_data[i]))
            print()

        x_sf1.append( x_mc[i] )
        x_sf2.append( x_mc[i] )
        assert(x_data[i] == x_mc[i])
        assert(x_prof[i] == x_mc[i])

        try:
            y_sf1.append( y_data[i] / y_mc[i] )
        except ZeroDivisionError:
            print('WARNING: There was a division by zero!')
            y_sf1.append( 0 )
            
        try:
            y_sf2.append( y_data[i] / y_prof[i] )
        except ZeroDivisionError:
            print('WARNING: There was a division by zero!')
            y_sf2.append( 0 )

        if y_sf1[i] == 0:
            eu_sf1.append(0.)
            ed_sf1.append(0.)
        else:
            eu_sf1.append(np.sqrt( eu_mc[i]**2 + eu_data[i]**2 ))
            ed_sf1.append(np.sqrt( ed_mc[i]**2 + ed_data[i]**2 ))

        if y_sf2[i] == 0:
            eu_sf2.append(0.)
            ed_sf2.append(0.)
        else:
            eu_sf2.append(np.sqrt( eu_prof[i]**2 + eu_data[i]**2 ))
            ed_sf2.append(np.sqrt( ed_prof[i]**2 + ed_data[i]**2 ))

        if debug:
            print('=== Scale Factors ====')
            for i in range(nbins):
                print('Unweighted: xp[{}] = {} - yp[{}] = {} +{}/-{}\n'
                      .format(i,x_sf1[i],i,y_sf1[i],eu_sf1[i],ed_sf1[i]))
                print('Weighted: xp[{}] = {} - yp[{}] = {} +{}/-{}\n'
                      .format(i,x_sf2[i],i,y_sf2[i],eu_sf2[i],ed_sf2[i]))
            print()

    darr = lambda x : np.array(x).astype(dtype=np.double)
    sf1 = TGraphAsymmErrors( nbins,
                             darr(x_sf1),
                             darr(y_sf1),
                             darr(halfbinwidths),
                             darr(halfbinwidths),
                             darr(ed_sf1),
                             darr(eu_sf1) )
    sf2 = TGraphAsymmErrors( nbins,
                             darr(x_sf2),
                             darr(y_sf2),
                             darr(halfbinwidths),
                             darr(halfbinwidths),
                             darr(ed_sf2),
                             darr(eu_sf2) )

    max1, min1 = max(y_sf1)+max(eu_sf1),min(y_sf1)-max(ed_sf1)
    max2, min2 = max(y_sf2)+max(eu_sf2),min(y_sf2)-max(ed_sf2)
    prof_max = max([ max1, max2 ])
    prof_min = min([ min1, min2 ])
    axor2 = TH2D( 'axor2', 'axor2',
                  axor_info[0], axor_info[1], axor_info[2],
                  100, prof_min-0.1*(prof_max-prof_min), prof_max+0.1*(prof_max-prof_min) )
    axor2.GetXaxis().SetNdivisions(axor_ndiv[0])
    axor2.GetYaxis().SetNdivisions(axor_ndiv[1])
    for i,elem in enumerate(sf1.GetX()):
        axor2.GetXaxis().SetBinLabel(i+1, str(round(elem-sf1.GetErrorXlow(i),2)))
    axor2.GetXaxis().SetBinLabel(i+2, str(round(elem+sf1.GetErrorXlow(i),2)))

    axor2.GetYaxis().SetLabelSize(0.08)
    axor2.GetXaxis().SetLabelSize(0.15)
    axor2.GetXaxis().SetLabelOffset(0.01)
    axor2.SetTitleSize(0.1,'X')
    axor2.SetTitleSize(0.11,'Y')
    axor2.GetXaxis().SetTitleOffset(1.)
    axor2.GetYaxis().SetTitleOffset(.5)
    axor2.GetYaxis().SetTitleSize(0.1)
    axor2.GetYaxis().SetTitle('Data/MC')
    axor2.GetXaxis().SetTitle(var)

    axor2.GetXaxis().SetTickLength(0)
    axor2.Draw()

    sf1_new = TGraphAsymmErrors( nbins )
    for ip in range(sf1.GetN()):
        sf1_new.SetPoint(ip, ip, sf1.GetPointY(ip) )
        sf1_new.SetPointError(ip, .5, .5,
                                   sf1.GetErrorYlow(ip), sf1.GetErrorYhigh(ip) )
    sf1_new.SetLineColor(kRed)
    sf1_new.SetLineWidth(2)
    sf1_new.SetMarkerColor(kRed)
    sf1_new.SetMarkerSize(1.3)
    sf1_new.SetMarkerStyle(22)
    sf1_new.GetXaxis().SetTitle(var)
    sf1_new.Draw('same p0')

    sf2_new = TGraphAsymmErrors( nbins )
    for ip in range(sf2.GetN()):
        sf2_new.SetPoint(ip, ip, sf2.GetPointY(ip) )
        sf2_new.SetPointError(ip, .5, .5,
                                   sf2.GetErrorYlow(ip), sf2.GetErrorYhigh(ip) )

    sf2_new.SetLineColor(ROOT.kGreen+3)
    sf2_new.SetLineWidth(2)
    sf2_new.SetMarkerColor(ROOT.kGreen+3)
    sf2_new.SetMarkerSize(1.3)
    sf2_new.SetMarkerStyle(22)
    sf2_new.Draw('same p0')

    pad2.cd()
    l = TLine()
    l.SetLineWidth(2)
    padmin = prof_min-0.1*(prof_max-prof_min)
    padmax = prof_max+0.1*(prof_max-prof_min)
    fraction = (padmax-padmin)/30
    for i in range(nbins):
      x = axor2.GetXaxis().GetBinLowEdge(i) + 1.5;
      l.DrawLine(x,padmin-fraction,x,padmin+fraction)
    l.DrawLine(x+1,padmin-fraction,x+1,padmin+fraction)

    redraw_border()

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
                
                thisbase = os.path.join(outdir, channel, var, 'Closure')
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
                 mc_name,
                 debug ):
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
    weightvars = effvars[channel][generate_trigger_combinations(triggers)[0][0]][0]
    
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
                                 subtag, debug )

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
parser.add_argument('--closure_single_trigger', dest='closure_single_trigger', nargs='+',
                    type=str, required=True,
                    help='Triggers considered for the closure. Originally used to find the expected perfect closure for a single trigger efficiency.')
parser.add_argument('--subtag', dest='subtag', required=True, help='subtag')
parser.add_argument('--data_name', dest='data_name', required=True, help='Data sample name')
parser.add_argument('--mc_name', dest='mc_name', required=True, help='MC sample name')

parser.add_argument('--debug', action='store_true', help='debug verbosity')
args = parser.parse_args()

run_closure( args.indir_union, args.indir_eff,
             args.outdir,
             args.channel, args.variables, args.closure_single_trigger,
             args.subtag,
             args.in_prefix, args.eff_prefix,
             args.data_name, args.mc_name,
             args.debug )

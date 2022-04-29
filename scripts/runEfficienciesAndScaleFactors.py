import os
import argparse
import ctypes
import numpy as np
from copy import copy

import warnings
warnings.filterwarnings('error', '.*Number of graph points is different than histogram bin.*')

import ROOT
ROOT.gROOT.SetBatch(True)
from ROOT import (
    TCanvas,
    TEfficiency,
    TFile,
    TGraphAsymmErrors,
    TH1D,
    TH2D,
    TLatex,
    TLegend,
    TLine,
    TPad,
)

import sys
sys.path.append( os.path.join(os.environ['CMSSW_BASE'], 'src', 'METTriggerStudies'))
from utils.utils import (
    create_single_dir,
    get_display_variable_name,
    get_histo_names,
    get_key_list,
    get_obj_max_min,
    get_root_object,
    load_binning,
    print_configuration,
    redraw_border,
    rewriteCutString,
    uniformize_bin_width,
)
from luigi_conf import (
    _extensions,
    _placeholder_cuts,
)

def drawEfficienciesAndScaleFactors(proc, channel, variable, trig, save_names, binedges, nbins,
                                    tprefix, indir, subtag, mc_name, data_name,
                                    intersection_str, debug):
    _name = lambda a,b,c,d : a + b + c + d + '.root'

    name_data = os.path.join(indir, _name( tprefix, data_name,
                                           '_Sum', subtag ) )
    file_data = TFile.Open(name_data)

    name_mc = os.path.join(indir, _name( tprefix, mc_name,
                                         '_Sum', subtag ))
    file_mc   = TFile.Open(name_mc)

    if debug:
        print('[=debug=] Open files:')
        print('[=debug=]  - Data: {}'.format(name_data))
        print('[=debug=]  - MC: {}'.format(name_mc))
        print('[=debug=]  - Args: proc={proc}, channel={channel}, variable={variable}, trig={trig}'
              .format(proc=proc, channel=channel, variable=variable, trig=trig))
   
    hnames = { 'ref':  get_histo_names('Ref1D')(channel, variable),
               'trig': get_histo_names('Trig1D')(channel, variable, trig)
              }
   
    keylist_data = get_key_list(file_data, inherits=['TH1'])
    keylist_mc = get_key_list(file_mc, inherits=['TH1'])
    
    for k in keylist_data:
        if k not in keylist_mc:
            m = 'Histogram {} was present in data but not in MC.\n'.format(k)
            m += 'This is possible, but highly unlikely (based on statistics).\n'
            m += 'Check everything is correct, and .qedit this check if so.\n'
            m += 'Data file: {}\n'.format(name_data)
            m += 'MC file: {}'.format(name_mc)
            raise ValueError(m)
   
    keys_to_remove = []
    for k in keylist_mc:
        if k not in keylist_data:
            histo = get_root_object(k, file_mc)
            stats_cut = 10
            if histo.GetNbinsX() < stats_cut:
                keys_to_remove.append(k)
            else:
                m = 'Histogram {} was present in MC but not in data.\n'.format(k)
                m += 'The current statistics cut is {}, but this one had {} events.\n'.format(stats_cut, histo.GetNbinsX())
                m += 'Check everything is correct, and edit this check if so.'
                raise ValueError(m)
   
    for k in keys_to_remove:
        keylist_mc.remove(k)
    assert(set(keylist_data)==set(keylist_mc))
      
    histos_data, histos_mc = ({} for _ in range(2))
    histos_data['ref'] = get_root_object(hnames['ref'], file_data)
    histos_mc['ref'] = get_root_object(hnames['ref'], file_mc)
   
    histos_data['trig'], histos_mc['trig'] = ({} for _ in range(2))
    for key in keylist_mc:
        rewritten_str = rewriteCutString(hnames['trig'], '')
        if key.startswith(rewritten_str):
            histos_data['trig'][key] = get_root_object(key, file_data)
            histos_mc['trig'][key] = get_root_object(key, file_mc)
            # print('File Data: {}'.format(file_data))
            # print('File MC: {}'.format(file_mc))
            # print('Data histo name: {}.'.format(histos_data['trig'][key].GetName()))
            # print('MC histo name: {}.'.format(histos_mc['trig'][key].GetName()))
            # print('Data Ref histo nBins: {}.'.format(histos_data['ref'].GetNbinsX()))
            # print('Data histo nBins: {}.'.format(histos_data['trig'][key].GetNbinsX()))
            # print('MC Ref histo nBins: {}.'.format(histos_mc['ref'].GetNbinsX()))
            # print('MC histo nBins: {}.'.format(histos_mc['trig'][key].GetNbinsX()))
            # for ibin in range(1,histos_mc['trig'][key].GetNbinsX()+1):
            #     print(ibin, histos_mc['trig'][key].GetBinContent(ibin))
            # print()

            # "solve" TGraphasymmErrors bin skipping when denominator=0
            # see TGraphAsymmErrors::Divide() (the default behaviour is very error prone!)
            # https://root.cern.ch/doc/master/classTGraphAsymmErrors.html#a37a202762b286cf4c7f5d34046be8c0b
            for ibin in range(1,histos_data['trig'][key].GetNbinsX()+1):
                if ( histos_data['trig'][key].GetBinContent(ibin)==0. and
                     histos_data['ref'].GetBinContent(ibin)==0. ):
                    histos_data['ref'].SetBinContent(ibin, 1)
            for ibin in range(1,histos_mc['trig'][key].GetNbinsX()+1):
                if ( histos_mc['trig'][key].GetBinContent(ibin)==0. and
                     histos_mc['ref'].GetBinContent(ibin)==0. ):
                    histos_mc['ref'].SetBinContent(ibin, 1)
            
    # some triggers or their intersection naturally never fire for some channels
    # example: 'IsoMu24' for the etau channel
    if len(histos_mc['trig']) == 0:
        print('WARNING: Trigger {} never fired for channel {} in MC.'.format(trig, channel))
        return
    
    eff_data, eff_mc = ({} for _ in range(2))
    try:
        for khisto1, vhisto1 in histos_data['trig'].items():
            eff_data[khisto1] = TGraphAsymmErrors( vhisto1, histos_data['ref'])
        for khisto2, vhisto2 in histos_mc['trig'].items():
            eff_mc[khisto2] = TGraphAsymmErrors( vhisto2, histos_mc['ref'] )
    except SystemError:
        m = 'There is likely a mismatch in the number of bins.'
        raise RuntimeError(m)

    npoints = eff_mc[khisto2].GetN() #they all have the same binning
    halfbinwidths = (binedges[1:]-binedges[:-1])/2
    darr = lambda x : np.array(x).astype(dtype=np.double)
    sf = {}
    assert len(eff_data) == len(eff_mc)
    
    for (kmc,vmc),(kdata,vdata) in zip(eff_mc.items(),eff_data.items()):
        assert(kmc == kdata)
      
        x_data, y_data   = ( [[] for _ in range(npoints)] for _ in range(2) )
        eu_data, ed_data = ( [[] for _ in range(npoints)] for _ in range(2) )
   
        x_mc, y_mc   = ( [[] for _ in range(npoints)] for _ in range(2) )
        eu_mc, ed_mc = ( [[] for _ in range(npoints)] for _ in range(2) )
   
        x_sf, y_sf   = ( [[] for _ in range(npoints)] for _ in range(2) )
        eu_sf, ed_sf = ( [[] for _ in range(npoints)] for _ in range(2) )
   
        for i in range(npoints):
            #ctypes conversions needed
            x_mc[i] = ctypes.c_double(0.)
            y_mc[i] = ctypes.c_double(0.)
            vmc.GetPoint(i, x_mc[i], y_mc[i])
            x_mc[i] = x_mc[i].value
            y_mc[i] = y_mc[i].value
   
            eu_mc[i] = vmc.GetErrorYhigh(i)
            ed_mc[i] = vmc.GetErrorYlow(i)
            if debug:
                print('xp[{}] = {} - yp[{}] = {} +{}/-{}\n'.format(i,x_mc[i],iy_mc[i],eu_mc[i],ed_mc[i]))
   
            x_data[i] = ctypes.c_double(0.)
            y_data[i] = ctypes.c_double(0.)
            vdata.GetPoint(i, x_data[i], y_data[i])
            x_data[i] = x_data[i].value
            y_data[i] = y_data[i].value
   
            eu_data[i] = vdata.GetErrorYhigh(i)
            ed_data[i] = vdata.GetErrorYlow(i)
            if debug:
                print('xp[{}] = {} - yp[{}] = {} +{}/-{}\n'.format(i,x_data[i],i,y_data[i],eu_data[i],ed_data[i]))
   
            x_sf[i] = x_mc[i]
            assert x_data[i] == x_mc[i]
   
            try:
                y_sf[i] = y_data[i] / y_mc[i]
            except ZeroDivisionError:
                print('[runEfficienciesAndScaleFactors.py] WARNING: There was a division by zero!')
                y_sf[i] = 0
   
            if y_sf[i] == 0:
                eu_sf[i] = 0
                ed_sf[i] = 0
            else:
                eu_sf[i] = np.sqrt( eu_mc[i]**2 + eu_data[i]**2 )
                ed_sf[i] = np.sqrt( ed_mc[i]**2 + ed_data[i]**2 )
   
            if debug:
                print('=== Scale Factors ====')
                for i in range(npoints):
                    print('xp[{}] = {} - yp[{}] = {} +{}/-{}\n'.format(i,x_sf[i],i,y_sf[i],eu_sf[i],ed_sf[i]))
   
        sf[kdata] = TGraphAsymmErrors( npoints,
                                       darr(x_sf),
                                       darr(y_sf),
                                       darr(halfbinwidths),
                                       darr(halfbinwidths),
                                       darr(ed_sf),
                                       darr(eu_sf) )
   
    ####### Scale Factors #######################################################################
    # eff_mc_histo = histos_mc['trig'].Clone('eff_mc_histo')
    # eff_data_histo = histos_data['trig'].Clone('eff_data_histo')
    # eff_data_histo.Sumw2(True)
   
    # # binomial errors are also correct ('b') when considering weighted histograms
    # # https://root.cern.ch/doc/master/TH1_8cxx_source.html#l03027
    # flag = eff_mc_histo.Divide(eff_mc_histo, histos_mc['ref'], 1, 1, 'b')
    # if not flag:
    #   raise RuntimeError('[drawTriggerSF.py] MC division failed!')
    # flag = eff_data_histo.Divide(eff_data_histo, histos_data['ref'], 1, 1, 'b')
    # if not flag:
    #   raise RuntimeError('[drawTriggerSF.py] Data division failed!')
   
    # sf = eff_data.Clone('sf')
   
    # sf.Divide(eff_data_histo, eff_mc_histo, 'pois') #independent processes (Data/MC) with poisson
    #############################################################################################
   
      
    if debug:
        print('[=debug=] Plotting...')
   
    for akey in sf:
        canvas_name = os.path.basename(save_names[0]).split('.')[0]
        canvas_name = rewriteCutString(canvas_name, akey, regex=True)
         
        canvas = TCanvas( canvas_name, 'canvas', 600, 600 )
        ROOT.gStyle.SetOptStat(0)
        ROOT.gStyle.SetOptTitle(0)
        canvas.cd()
         
        pad1 = TPad('pad1', 'pad1', 0, 0.35, 1, 1)
        pad1.SetBottomMargin(0.005)
        pad1.SetLeftMargin(0.2)
        pad1.Draw()
        pad1.cd()

        max1, min1 = get_obj_max_min(eff_data[akey], nbins, False)
        max2, min2 = get_obj_max_min(eff_mc[akey], nbins, False)
        eff_max = max([ max1, max2 ])
        eff_min = min([ min1, min2 ])

        axor_info = nbins+1, -1, nbins
        axor_ndiv = 605, 705
        axor = TH2D('axor'+akey,'axor'+akey,
                    axor_info[0], axor_info[1], axor_info[2],
                    100, eff_min-0.1*(eff_max-eff_min), eff_max+0.4*(eff_max-eff_min) )
        axor.GetYaxis().SetTitle('Efficiency')
        axor.GetYaxis().SetNdivisions(axor_ndiv[1])
        axor.GetXaxis().SetLabelOffset(1)
        axor.GetXaxis().SetNdivisions(axor_ndiv[0])
        axor.GetYaxis().SetTitleSize(0.08)
        axor.GetYaxis().SetTitleOffset(.85)
        axor.GetXaxis().SetLabelSize(0.07)
        axor.GetYaxis().SetLabelSize(0.07)
        axor.GetXaxis().SetTickLength(0)
        axor.Draw()

        eff_data_new = uniformize_bin_width(eff_data[akey])
        eff_mc_new = uniformize_bin_width(eff_mc[akey])

        eff_data_new.SetLineColor(1)
        eff_data_new.SetLineWidth(2)
        eff_data_new.SetMarkerColor(1)
        eff_data_new.SetMarkerSize(1.3)
        eff_data_new.SetMarkerStyle(20)
        eff_data_new.Draw('same p0 e')

        eff_mc_new.SetLineColor(ROOT.kRed)
        eff_mc_new.SetLineWidth(2)
        eff_mc_new.SetMarkerColor(ROOT.kRed)
        eff_mc_new.SetMarkerSize(1.3)
        eff_mc_new.SetMarkerStyle(22)
        eff_mc_new.Draw('same p0')

        pad1.RedrawAxis()
        l = TLine()
        l.SetLineWidth(2)
        padmin = eff_min-0.1*(eff_max-eff_min)
        padmax = eff_max+0.1*(eff_max-eff_min)
        fraction = (padmax-padmin)/45
        for i in range(nbins):
          x = axor.GetXaxis().GetBinLowEdge(i) + 1.5;
          l.DrawLine(x,padmin-fraction,x,padmin+fraction)
        l.DrawLine(x+1,padmin-fraction,x+1,padmin+fraction)

        leg = TLegend(0.77, 0.77, 0.96, 0.87)
        leg.SetFillColor(0)
        leg.SetShadowColor(0)
        leg.SetBorderSize(0)
        leg.SetTextSize(0.05)
        leg.SetFillStyle(0)
        leg.SetTextFont(42)

        leg.AddEntry(eff_data_new, 'Data', 'p')
        leg.AddEntry(eff_mc_new,   proc,   'p')
        leg.Draw('same')

        redraw_border()

        lX, lY, lYstep = 0.23, 0.84, 0.05
        l = TLatex()
        l.SetNDC()
        l.SetTextFont(72)
        l.SetTextColor(1)

        latexChannel = copy(channel)
        latexChannel.replace('mu','#mu')
        latexChannel.replace('tau','#tau_{h}')
        latexChannel.replace('Tau','#tau_{h}')

        splitcounter = 0
        ucode = '+'
        trig_str = trig.split(intersection_str)
        trig_names_str = ''
        if len(trig_str)==1:
            trig_names_str = trig
        else:
            for i,elem in enumerate(trig_str):
                if elem == trig_str[-1]:
                    trig_names_str += elem + '}' + '}'*(splitcounter-1)
                else:
                    splitcounter += 1
                    trig_names_str += '#splitline{'
                    trig_names_str += elem + ' ' + ucode + '}{'

        trig_start_str = 'Trigger' + ('' if len(trig_str)==1 else 's') + ': '
        l.DrawLatex( lX, lY, 'Channel: '+latexChannel)
        l.DrawLatex( lX, lY-lYstep, trig_start_str+trig_names_str)

        canvas.cd()
        pad2 = TPad('pad2','pad2',0,0.0,1,0.35)
        pad2.SetTopMargin(0.01)
        pad2.SetBottomMargin(0.4)
        pad2.SetLeftMargin(0.2)
        pad2.Draw()
        pad2.cd()
        pad2.SetGridy()

        max1, min1 = max(y_sf)+max(eu_sf),min(y_sf)-max(ed_sf)
        axor2 = TH2D( 'axor2'+akey,'axor2'+akey,
                      axor_info[0], axor_info[1], axor_info[2],
                      100, min1-0.1*(max1-min1), max1+0.1*(max1-min1) )
        axor2.GetXaxis().SetNdivisions(axor_ndiv[0])
        axor2.GetYaxis().SetNdivisions(axor_ndiv[1])

        # Change bin labels
        rounding = lambda x: int(x) if 'pt' in variable else round(x, 2)
        for i,elem in enumerate(sf[akey].GetX()):
            axor2.GetXaxis().SetBinLabel(i+1, str(rounding(elem-sf[akey].GetErrorXlow(i))))
        axor2.GetXaxis().SetBinLabel(i+2, str(rounding(elem+sf[akey].GetErrorXlow(i))))

        axor2.GetYaxis().SetLabelSize(0.12)
        axor2.GetXaxis().SetLabelSize(0.18)
        axor2.GetXaxis().SetLabelOffset(0.015)
        axor2.SetTitleSize(0.13,'X')
        axor2.SetTitleSize(0.12,'Y')
        axor2.GetXaxis().SetTitleOffset(1.)
        axor2.GetYaxis().SetTitleOffset(0.5)
        axor2.GetYaxis().SetTitle('Data/MC')
            
        axor2.GetXaxis().SetTitle( get_display_variable_name(channel, variable) )
        axor2.GetXaxis().SetTickLength(0)
        axor2.Draw()

        sf_new = uniformize_bin_width(sf[akey])
        sf_new.SetLineColor(ROOT.kRed)
        sf_new.SetLineWidth(2)
        sf_new.SetMarkerColor(ROOT.kRed)
        sf_new.SetMarkerSize(1.3)
        sf_new.SetMarkerStyle(22)
        sf_new.GetYaxis().SetLabelSize(0.12)
        sf_new.GetXaxis().SetLabelSize(0.12)
        sf_new.GetXaxis().SetTitleSize(0.15)
        sf_new.GetYaxis().SetTitleSize(0.15)
        sf_new.GetXaxis().SetTitleOffset(1.)
        sf_new.GetYaxis().SetTitleOffset(0.45)
        sf_new.GetYaxis().SetTitle('Data/MC')
        sf_new.GetXaxis().SetTitle( get_display_variable_name(channel, variable) )
        sf_new.Draw('same P0')

        pad2.cd()
        l = TLine()
        l.SetLineWidth(2)
        padmin = min1-0.1*(max1-min1)
        padmax = max1+0.1*(max1-min1)
        fraction = (padmax-padmin)/30
        for i in range(nbins):
          x = axor2.GetXaxis().GetBinLowEdge(i) + 1.5;
          l.DrawLine(x,padmin-fraction,x,padmin+fraction)
        l.DrawLine(x+1,padmin-fraction,x+1,padmin+fraction)

        redraw_border()

        for aname in save_names[:-1]:
            _name = rewriteCutString(aname, akey, regex=True)
            canvas.SaveAs( _name )

        _name = rewriteCutString(save_names[-1], akey, regex=True)
        eff_file = TFile.Open(_name, 'RECREATE')
        eff_file.cd()

        eff_data[akey].SetName('Data')
        eff_mc[akey].SetName('MC')
        sf[akey].SetName('ScaleFactors')

        eff_data[akey].Write('Data')
        eff_mc[akey].Write('MC')
        sf[akey].Write('ScaleFactors')

def _getCanvasName(proc, chn, var, trig, data_name, subtag):
    """
    A 'XXX' placeholder is added for later replacement by all cuts considered
      for the same channel, variable and trigger combination.
    Without the placeholder one would have to additionally calculate the number
      of cuts beforehand, which adds complexity with no major benefit.
    """
    add = proc + '_' + chn + '_' + var + '_' + trig
    n = args.canvas_prefix + data_name + '_' + add + subtag
    n += _placeholder_cuts
    return n

#@setPureInputNamespace
def runEfficienciesAndScaleFactors_outputs(outdir,
                                           mc_processes,
                                           mc_name, data_name,
                                           trigger_combination,
                                           channels, variables,
                                           subtag,
                                           draw_independent_MCs):
  outputs = [[] for _ in range(len(_extensions))]
  processes = mc_processes if draw_independent_MCs else [mc_name]
  
  for proc in processes:
    for ch in channels:
      for var in variables:
        canvas_name = _getCanvasName(proc, ch, var,
                                     trigger_combination,
                                     data_name, subtag)
        thisbase = os.path.join(outdir, ch, var, '')
        create_single_dir( thisbase )

        for ext,out in zip(_extensions, outputs):
          out.append( os.path.join( thisbase, canvas_name + '.' + ext ) )

  #join all outputs in the same list
  return sum(outputs, []), _extensions, processes

def runEfficienciesAndScaleFactors(indir, outdir,
                                   mc_processes, mc_name, data_name,
                                   trigger_combination,
                                   channels, variables,
                                   binedges_filename, subtag,
                                   draw_independent_MCs,
                                   tprefix,
                                   intersection_str,
                                   debug):
  outputs, extensions, processes = runEfficienciesAndScaleFactors_outputs(outdir,
                                                                          mc_processes, mc_name, data_name,
                                                                          trigger_combination,
                                                                          channels, variables,
                                                                          subtag,
                                                                          draw_independent_MCs)
  
  binedges, nbins = load_binning(binedges_filename, subtag, variables, channels)
  
  dv = len(args.variables)
  dc = len(args.channels) * dv
  dp = len(processes) * dc
  
  for ip,proc in enumerate(processes):
    for ic,chn in enumerate(channels):
      for iv,var in enumerate(variables):
        index = ip*dc + ic*dv + iv
        names = [ outputs[index + dp*x] for x in range(len(extensions)) ]

        if args.debug:
          for name in names:
            print('[=debug=] {}'.format(name))
            m = "process={}, channel={}".format(proc, chn)
            m += ", variable={}, trigger={}".format(var, trig)
            m += ", trigger_combination={}\n".format(trigger_combination)

        drawEfficienciesAndScaleFactors( proc, chn, var,
                                         trigger_combination,
                                         names,
                                         binedges[var][chn], nbins[var][chn],
                                         tprefix,
                                         indir, subtag,
                                         mc_name, data_name,
                                         intersection_str,
                                         debug)


parser = argparse.ArgumentParser(description='Draw trigger scale factors')

parser.add_argument('--indir', help='Inputs directory', required=True)
parser.add_argument('--outdir', help='Output directory', required=True, )
parser.add_argument('--tprefix', help='prefix to the names of the produced outputs (targets in luigi lingo)', required=True)
parser.add_argument('--canvas_prefix', help='canvas prefix', required=True)
parser.add_argument('--subtag',           dest='subtag',           required=True, help='subtag')
parser.add_argument('--mc_processes', help='MC processes to be analyzed', required=True, nargs='+', type=str)
parser.add_argument('--binedges_filename', dest='binedges_filename', required=True, help='in directory')
parser.add_argument('--data_name', dest='data_name', required=True, help='Data sample name')
parser.add_argument('--mc_name', dest='mc_name', required=True, help='MC sample name')
parser.add_argument('--triggercomb', dest='triggercomb', required=True,
                    help='Trigger intersection combination.')
parser.add_argument('--channels',   dest='channels',         required=True, nargs='+', type=str,
                    help='Select the channels over which the workflow will be run.' )
parser.add_argument('--variables',        dest='variables',        required=True, nargs='+', type=str,
                    help='Select the variables over which the workflow will be run.' )
parser.add_argument('--draw_independent_MCs', action='store_true', help='debug verbosity')
parser.add_argument('--intersection_str', dest='intersection_str', required=False, default='_PLUS_',
                    help='String used to represent set intersection between triggers.')
parser.add_argument('--debug', action='store_true', help='debug verbosity')
args = parser.parse_args()
print_configuration(args)

runEfficienciesAndScaleFactors(args.indir, args.outdir,
                               args.mc_processes, args.mc_name, args.data_name,
                               args.triggercomb,
                               args.channels, args.variables,
                               args.binedges_filename, args.subtag,
                               args.draw_independent_MCs,
                               args.tprefix,
                               args.intersection_str,
                               args.debug)

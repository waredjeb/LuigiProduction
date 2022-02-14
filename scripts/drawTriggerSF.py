import os
import re
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
from ROOT import TH1D
from ROOT import TH2D
from ROOT import TLatex
from ROOT import TLegend

from utils.utils import (
  createSingleDir,
  generateTriggerCombinations,
  getKeyList,
  getROOTObject,
  getHistoNames,
  joinNameTriggerIntersection as joinNTC,
  redrawBorder,
  replacePlaceholder,
  restoreBinning,
  setPureInputNamespace,
)

from luigi_conf import (
  _extensions,
  _placeholder_cuts,
)

def drawEfficienciesAndScaleFactors(args, proc, channel, variable, trig, save_names, binedges, nbins):
  _name = lambda a,b,c,d : a + b + c + d + '.root'

  name_data = os.path.join(args.indir, _name( args.tprefix, args.data_name,
                                                args.tsuffix, args.subtag ) )
  file_data = TFile.Open(name_data)
  
  name_mc = os.path.join(args.indir, _name( args.tprefix, args.mc_name,
                                            args.tsuffix, args.subtag ))
  file_mc   = TFile.Open(name_mc)
  
  if args.debug:
    print('[=debug=] Open files:')
    print('[=debug=]  - Data: {}'.format(name_data))
    print('[=debug=]  - MC: {}'.format(name_mc))
    print('[=debug=]  - Args: proc={proc}, channel={channel}, variable={variable}, trig={trig}'
          .format(proc=proc, channel=channel, variable=variable, trig=trig))

  hnames = { 'ref':  getHistoNames('Ref1D')(channel, variable),
             'trig': getHistoNames('Trig1D')(channel, variable, joinNTC(trig))
            }

  keylist_data = getKeyList(file_data, inherits=['TH1'])
  keylist_mc = getKeyList(file_mc, inherits=['TH1'])
  
  for k in keylist_data:
    if k not in keylist_mc:
      m = 'Histogram {} was present in data but not in MC.\n'.format(k)
      m += 'This is possible, but highly unlikely (based on statistics).\n'
      m += 'Check everything is correct, and edit this check if so.'
      raise ValueError(m)

  keys_to_remove = []
  for k in keylist_mc:
    if k not in keylist_data:
      histo = getROOTObject(k, file_mc)
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
  histos_data['ref'] = getROOTObject(hnames['ref'], file_data)
  histos_mc['ref'] = getROOTObject(hnames['ref'], file_mc)

  histos_data['trig'], histos_mc['trig'] = ({} for _ in range(2))
  for key in keylist_mc:
    if key.startswith( replacePlaceholder('cuts', hnames['trig'], '') ):
      histos_mc['trig'][key] = getROOTObject(key, file_mc)
      histos_data['trig'][key] = getROOTObject(key, file_data)

  # some triggers or their intersection naturally never fire for some channels
  # example: 'IsoMu24' for the etau channel
  if len(histos_mc['trig']) == 0:
    print('WARNING: {} {}'.format(trig, channel))
    return
  
  eff_data, eff_mc = ({} for _ in range(2))
  for khisto, vhisto in histos_data['trig'].items():
    eff_data[khisto] = TGraphAsymmErrors( vhisto, histos_data['ref'])
  for khisto, vhisto in histos_mc['trig'].items():
    eff_mc[khisto] = TGraphAsymmErrors( vhisto, histos_mc['ref'] )

  npoints = eff_mc[khisto].GetN() #they all have the same binning
  halfbinwidths = (binedges[1:]-binedges[:-1])/2
  darr = lambda x : np.array(x).astype(dtype=np.double)
  sf = {}
  assert( len(eff_data) == len(eff_mc) )
  
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
      if args.debug:
        print('xp[{}] = {} - yp[{}] = {} +{}/-{}\n'.format(i,x_mc[i],iy_mc[i],eu_mc[i],ed_mc[i]))

      x_data[i] = ctypes.c_double(0.)
      y_data[i] = ctypes.c_double(0.)
      vdata.GetPoint(i, x_data[i], y_data[i])
      x_data[i] = x_data[i].value
      y_data[i] = y_data[i].value

      eu_data[i] = vdata.GetErrorYhigh(i)
      ed_data[i] = vdata.GetErrorYlow(i)
      if args.debug:
        print('xp[{}] = {} - yp[{}] = {} +{}/-{}\n'.format(i,x_data[i],i,y_data[i],eu_data[i],ed_data[i]))

      x_sf[i] = x_mc[i]
      assert(x_data[i] == x_mc[i])

      try:
        y_sf[i] = y_data[i] / y_mc[i]
      except ZeroDivisionError:
        print('WARNING: There was a division by zero!')
        y_sf[i] = 0

      if y_sf[i] == 0:
        eu_sf[i] = 0
        ed_sf[i] = 0
      else:
        eu_sf[i] = np.sqrt( eu_mc[i]**2 + eu_data[i]**2 )
        ed_sf[i] = np.sqrt( ed_mc[i]**2 + ed_data[i]**2 )

    if args.debug:
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

    
  if args.debug:
    print('[=debug=] Plotting...')  

  for akey in sf:
    canvas_name = replacePlaceholder('cuts', os.path.basename(save_names[0]).split('.')[0], akey)
    canvas = TCanvas( canvas_name, 'canvas', 600, 600 )
    ROOT.gStyle.SetOptStat(0)
    ROOT.gStyle.SetOptTitle(0)
    canvas.cd()

    pad1 = TPad('pad1', 'pad1', 0, 0.35, 1, 1)
    pad1.SetBottomMargin(0.005)
    pad1.SetLeftMargin(0.2)
    pad1.Draw()
    pad1.cd()

    axor = TH2D('axor'+akey,'axor'+akey, nbins,
                binedges[0]-halfbinwidths[0]/2, binedges[-1]+halfbinwidths[-1]/2,
                100, -0.1, 1.4)
    axor.GetYaxis().SetTitle('Efficiency')
    axor.GetXaxis().SetLabelOffset(1)
    axor.GetXaxis().SetLabelOffset(1.)
    axor.GetYaxis().SetTitleSize(0.08)
    axor.GetYaxis().SetTitleOffset(.85)
    axor.GetXaxis().SetLabelSize(0.07)
    axor.GetYaxis().SetLabelSize(0.07)
    axor.Draw()

    eff_data[akey].SetLineColor(1)
    eff_data[akey].SetLineWidth(2)
    eff_data[akey].SetMarkerColor(1)
    eff_data[akey].SetMarkerSize(1.3)
    eff_data[akey].SetMarkerStyle(20)
    eff_data[akey].Draw('same p0 e')

    eff_mc[akey].SetLineColor(ROOT.kRed)
    eff_mc[akey].SetLineWidth(2)
    eff_mc[akey].SetMarkerColor(ROOT.kRed)
    eff_mc[akey].SetMarkerSize(1.3)
    eff_mc[akey].SetMarkerStyle(22)
    eff_mc[akey].Draw('same p0')

    pad1.RedrawAxis()

    leg = TLegend(0.77, 0.77, 0.96, 0.87)
    leg.SetFillColor(0)
    leg.SetShadowColor(0)
    leg.SetBorderSize(0)
    leg.SetTextSize(0.06)
    leg.SetFillStyle(0)
    leg.SetTextFont(42)

    leg.AddEntry(eff_data[akey], 'Data', 'p')
    leg.AddEntry(eff_mc[akey],   proc,   'p')
    leg.Draw('same')

    redrawBorder()

    lX, lY, lYstep = 0.25, 0.84, 0.1
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
    trig_names_str = '#splitline{'
    for i,elem in enumerate(trig):
      splitcounter += 1
      if elem == trig[-1]:
        trig_names_str += elem + '}{}' + '}'*(splitcounter-1)
      else:
        trig_names_str += elem + ' ' + ucode
        trig_names_str += '}{#splitline{'
    print('========================= ', trig_names_str)

    trig_start_str = 'Trigger' + ('' if len(trig)==1 else 's') + ': '
    l.DrawLatex( lX, lY,        'Channel: '+latexChannel)
    l.DrawLatex( lX, lY-lYstep, trig_start_str+trig_names_str)

    canvas.cd()
    pad2 = TPad('pad2','pad2',0,0.0,1,0.35)
    pad2.SetTopMargin(0.01)
    pad2.SetBottomMargin(0.4)
    pad2.SetLeftMargin(0.2)
    pad2.Draw()
    pad2.cd()
    pad2.SetGridy()

    axor2 = TH2D('axor2'+akey, 'axor2'+akey, nbins, binedges[0], binedges[-1], 100, 0.45, 1.55)
    axor2.GetYaxis().SetNdivisions(507)
    axor2.GetYaxis().SetLabelSize(0.12)
    axor2.GetXaxis().SetLabelSize(0.12)
    axor2.SetTitleSize(0.15,'X')
    axor2.SetTitleSize(0.15,'Y')
    axor2.GetXaxis().SetTitleOffset(1.)
    axor2.GetYaxis().SetTitleOffset(0.45)
    axor2.GetYaxis().SetTitle('Data/MC')
    axor2.GetXaxis().SetTitle(variable)
    axor2.Draw()

    sf[akey].SetLineColor(ROOT.kRed)
    sf[akey].SetLineWidth(2)
    sf[akey].SetMarkerColor(ROOT.kRed)
    sf[akey].SetMarkerSize(1.3)
    sf[akey].SetMarkerStyle(22)
    sf[akey].GetYaxis().SetNdivisions(507)
    sf[akey].GetYaxis().SetLabelSize(0.12)
    sf[akey].GetXaxis().SetLabelSize(0.12)
    sf[akey].GetXaxis().SetTitleSize(0.15)
    sf[akey].GetYaxis().SetTitleSize(0.15)
    sf[akey].GetXaxis().SetTitleOffset(1.)
    sf[akey].GetYaxis().SetTitleOffset(0.45)
    sf[akey].GetYaxis().SetTitle('Data/MC')
    sf[akey].GetXaxis().SetTitle(variable)
    sf[akey].Draw('same P0')

    redrawBorder()

    for aname in save_names:
      _regex = re.findall(r'^.*(CUTS_.+)$', akey)
      assert(len(_regex)==1)
      _regex = _regex[0]
      _regex = _regex.replace('>', 'L').replace('<', 'S').replace('.', 'p')
      _name = replacePlaceholder('cuts', aname, _regex )
      canvas.SaveAs( _name )

def _getCanvasName(proc, chn, var, trig, data_name, subtag, intersection_str):
    """
    A 'XXX' placeholder is added for later replacement by all cuts considered
      for the same channel, variable and trigger combination.
    Without the placeholder one would have to additionally calculate the number
      of cuts beforehand, which adds complexity with no major benefit.
    """
    trigstr = intersection_str.join(trig)
    add = proc + '_' + chn + '_' + var + '_' + trigstr
    n = 'trigSF_' + data_name + '_' + add + subtag
    n += _placeholder_cuts
    return n

@setPureInputNamespace
def drawTriggerSF_outputs(args):
  outputs = [[] for _ in range(len(_extensions))]
  processes = args.mc_processes if args.draw_independent_MCs else [args.mc_name]

  triggercomb = generateTriggerCombinations(args.triggers)
  
  for proc in processes:
    for ch in args.channels:
      for var in args.variables:
        for tcomb in triggercomb:
          canvas_name = _getCanvasName(proc, ch, var, tcomb,
                                       args.data_name, args.subtag, args.intersection_str)
          thisbase = os.path.join(args.outdir, ch, var, '')
          createSingleDir( thisbase )

          for ext,out in zip(_extensions, outputs):
            out.append( os.path.join( thisbase, canvas_name + '.' + ext ) )

  #join all outputs in the same list
  return sum(outputs, []), _extensions
    
@setPureInputNamespace
def drawTriggerSF(args):
  outputs, extensions = drawTriggerSF_outputs(args)
  processes = args.mc_processes if args.draw_independent_MCs else [args.mc_name]

  triggercomb = generateTriggerCombinations(args.triggers)
  
  binedges, nbins = restoreBinning(args.binedges_filename, args.channels,
                                   args.variables, args.subtag)
  
  dt = len(triggercomb)
  dv = len(args.variables) * dt
  dc = len(args.channels) * dv
  dp = len(processes) * dc
  
  for ip,proc in enumerate(processes):
    for ic,chn in enumerate(args.channels):
      for iv,var in enumerate(args.variables):
        for it,tcomb in enumerate(triggercomb):
          index = ip*dc + ic*dv + iv*dt + it
          names = [ outputs[index + dp*x] for x in range(len(extensions)) ]

          if args.debug:
            for name in names:
              print('[=debug=] {}'.format(name))
            print("process={}, channel={}, variable={}, trigger={}".format(proc, chn, var, trig))
            print()

          drawEfficienciesAndScaleFactors( args, proc, chn, var, tcomb, names,
                                           binedges[var][chn], nbins[var][chn] )

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Draw trigger scale factors')

    parser.add_argument('-i', '--indir', help='Inputs directory', required=True)
    parser.add_argument('-x', '--tprefix', help='prefix to the names of the produced outputs (targets in luigi lingo)', required=True)
    parser.add_argument('-t', '--tag', help='string to diferentiate between different workflow runs', required=True)
    parser.add_argument('-d', '--data', help='dataset to be analyzed/plotted', required=True)
    parser.add_argument('-p', '--mc_processes', help='MC processes to be analyzed', required=True)
    parser.add_argument('--binedges_filename', dest='binedges_filename', required=True, help='in directory')
    parser.add_argument('--draw_independent_MCs', action='store_true', help='debug verbosity')
    parser.add_argument('--intersection_str', dest='intersection_str', required=False, default='_PLUS_',
                    help='String used to represent set intersection between triggers.')
    parser.add_argument('--nocut_dummy_str', dest='tprefix', required=True,
                        help='Dummy string associated to trigger histograms were no cuts are applied.')
    parser.add_argument('--debug', action='store_true', help='debug verbosity')
    args = parser.parse_args()

    drawTriggerSF(args)

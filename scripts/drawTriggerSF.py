import os
import argparse
import ctypes
import numpy as np
import h5py
from copy import copy

import ROOT
ROOT.gROOT.SetBatch(True)
from ROOT import TCanvas
from ROOT import TPad
from ROOT import TStyle
from ROOT import TFile
from ROOT import TEfficiency
from ROOT import TGraphAsymmErrors
from ROOT import TH1D
from ROOT import TH2D
from ROOT import TLatex
from ROOT import TLine
from ROOT import TLegend
from ROOT import TString

from utils import utils
from luigi_conf import _extensions

def checkTrigger(args, proc, channel, variable, trig, save_names, binedges, nbins):
  _name = lambda a,b,c,d : a + b + c + d + '.root'

  name_data = os.path.join(args.indir, _name( args.targetsPrefix, args.data_name,
                                                args.target_suffix, args.subtag ) )
  file_data = TFile( name_data, 'READ');
  
  name_mc = os.path.join(args.indir, _name( args.targetsPrefix, args.mc_name,
                                            args.target_suffix, args.subtag ))
  file_mc   = TFile( name_mc, 'READ');

  if args.debug:
    print('[=debug=] Open files:')
    print('[=debug=]  - Data: {}'.format(name_data))
    print('[=debug=]  - MC: {}'.format(name_mc))
    print('[=debug=]  - Args: proc={proc}, channel={channel}, variable={variable}, trig={trig}'
          .format(proc=proc, channel=channel, variable=variable, trig=trig))

  histo_names = { 'ref': 'Ref_{}_{}'.format(channel, variable),
                  'trig': 'Trig_{}_{}_{}'.format(channel, variable, trig)
                  # 'noref' : 'NoRef_{}_{}_{}'.format(channel, variable, trig)
                 }
  
  histos_mc   = { k: utils.getROOTObject(v, file_mc)   for k,v in histo_names.items() }
  histos_data = { k: utils.getROOTObject(v, file_data) for k,v in histo_names.items() }
  
  if args.debug:
    print('[=debug=] MC efficiency...')  
  #eff_mc = TEfficiency( histos_mc['trig'], histos_mc['ref'] )
  #SetConfidenceLevel, SetStatisticOption
  eff_mc = TGraphAsymmErrors( histos_mc['trig'], histos_mc['ref'] )
  eff_mc_clone = histos_mc['trig'].Clone('eff_mc_')
  # geffmc = TGraphAsymmErrors()
  # geffmc.Divide(heffmc,h_passALL,'cp')
  flag = eff_mc_clone.Divide(eff_mc_clone, histos_mc['ref'], 1, 1, 'B')
  if not flag:
    raise RuntimeError('[drawTriggerSF.py] MC division failed!')

  if args.debug:
    print('[=debug=] Data efficiency...')  
  #eff_data = TEfficiency( histos_data['trig'], histos_data['ref'])
  eff_data = TGraphAsymmErrors( histos_data['trig'], histos_data['ref'])
  eff_data_clone = histos_data['trig'].Clone('eff_data_')
  # geff = TGraphAsymmErrors()
  # geff.Divide(heff, d_passALL, 'cp')
  flag = eff_data_clone.Divide(eff_data_clone, histos_data['ref'], 1, 1, 'B')
  if not flag:
    raise RuntimeError('[drawTriggerSF.py] Data division failed!')


  if args.debug:
    print('[=debug=] Scale Factors...')  

  npoints = eff_mc.GetN()
  x_mc, y_mc   = ( [[] for _ in range(npoints)] for _ in range(2) )
  eu_mc, ed_mc = ( [[] for _ in range(npoints)] for _ in range(2) )
  
  for i in range(npoints):
    #ctypes conversions needed
    x_mc[i] = ctypes.c_double(0.)
    y_mc[i] = ctypes.c_double(0.)
    eff_mc.GetPoint(i, x_mc[i], y_mc[i])
    x_mc[i] = x_mc[i].value
    y_mc[i] = y_mc[i].value

    eu_mc[i] = eff_mc.GetErrorYhigh(i)
    ed_mc[i] = eff_mc.GetErrorYlow(i)
    if args.debug:
      print('xp[{}] = {} - yp[{}] = {} +{}/-{}\n'.format(i,x_mc[i],i,y_mc[i],eu_mc[i],ed_mc[i]))

  x_data, y_data   = ( [[] for _ in range(npoints)] for _ in range(2) )
  eu_data, ed_data = ( [[] for _ in range(npoints)] for _ in range(2) )
  
  x_sf, y_sf   = ( [[] for _ in range(npoints)] for _ in range(2) )
  eu_sf, ed_sf = ( [[] for _ in range(npoints)] for _ in range(2) )
  
  for i in range(npoints):
    x_data[i] = ctypes.c_double(0.)
    y_data[i] = ctypes.c_double(0.)
    eff_data.GetPoint(i, x_data[i], y_data[i])
    x_data[i] = x_data[i].value
    y_data[i] = y_data[i].value
    
    eu_data[i] = eff_data.GetErrorYhigh(i)
    ed_data[i] = eff_data.GetErrorYlow(i)
    if args.debug:
      print('xp[{}] = {} - yp[{}] = {} +{}/-{}\n'.format(i,x_data[i],i,y_data[i],eu_data[i],ed_data[i]))

    x_sf[i] = x_mc[i]
    
    try:
      y_sf[i]  = y_data[i] / y_mc[i]
    except ZeroDivisionError:
      print('WARNING: THere was a division by zero!')
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
  
  halfbinwidths = (binedges[1:]-binedges[:-1])/2
  darr = lambda x : np.array(x).astype(dtype=np.double)
  sf = TGraphAsymmErrors(npoints,
                         darr(x_sf),
                         darr(y_sf),
                         darr(halfbinwidths),
                         darr(halfbinwidths),
                         darr(ed_sf),
                         darr(eu_sf))

  # sf_ = eff_data_clone.Clone('sf_')

  # nbins = sf_.GetNbinsX()
  # assert(nbins == eff_mc_clone.GetNbinsX())

  # sf = TGraphAsymmErrors(nbins)
  # sf.Divide(sf_, eff_mc_clone, 'n') #before was set to 'cp': Clopper-Pearson

  if args.debug:
    print('[=debug=] Plotting...')  
  canvas = TCanvas( os.path.basename(save_names[0]).split('.')[0], 'canvas', 600, 600 )
  ROOT.gStyle.SetOptStat(0)
  ROOT.gStyle.SetOptTitle(0)
  canvas.cd()
  
  eff_data.SetLineColor(1)
  eff_data.SetLineWidth(2)
  eff_data.SetMarkerColor(1)
  eff_data.SetMarkerSize(1.5)
  eff_data.SetMarkerStyle(20)
    
  eff_mc.SetLineColor(ROOT.kRed)
  eff_mc.SetLineWidth(2)
  eff_mc.SetMarkerColor(ROOT.kRed)
  eff_mc.SetMarkerSize(1.4)
  eff_mc.SetMarkerStyle(22)
  
  sf.SetLineColor(ROOT.kRed)
  sf.SetLineWidth(2)
  sf.SetMarkerColor(ROOT.kRed)
  sf.SetMarkerSize(1.4)
  sf.SetMarkerStyle(22)
    
  pad1 = TPad('pad1', 'pad1', 0, 0.35, 1, 1)
  pad1.SetBottomMargin(0.005)
  pad1.SetLeftMargin(0.2)
  pad1.Draw()
  pad1.cd()
  
  axor = TH2D('axor','axor', nbins, binedges[0], binedges[-1], 100, -0.1, 1.7)
  axor.GetYaxis().SetTitle('Efficiency')
  axor.GetXaxis().SetLabelOffset(1)
  axor.GetXaxis().SetLabelOffset(1.)
  axor.GetYaxis().SetTitleSize(0.08)
  axor.GetYaxis().SetTitleOffset(.85)
  axor.GetXaxis().SetLabelSize(0.07)
  axor.GetYaxis().SetLabelSize(0.07)
  axor.Draw()
  
  eff_data.Draw('SAME p0 e ')
  eff_mc.Draw('SAME p0 ')
  pad1.RedrawAxis()
  
  leg = TLegend(0.25, 0.55, 0.47, 0.75)
  leg.SetFillColor(0)
  leg.SetShadowColor(0)
  leg.SetBorderSize(0)
  leg.SetTextSize(0.06)
  leg.SetFillStyle(0)
  leg.SetTextFont(42)
  
  leg.AddEntry(eff_data, 'Data', 'p')
  leg.AddEntry(eff_mc,   proc,   'p')
  leg.Draw('same')
  
  utils.redrawBorder()
  
  lX, lY, lYstep = 0.25, 0.84, 0.035
  l = TLatex()
  l.SetNDC()
  l.SetTextFont(72)
  l.SetTextColor(1)
  
  latexChannel = copy(channel)
  latexChannel.replace('mu','#mu')
  latexChannel.replace('tau','#tau_{h}')
  latexChannel.replace('Tau','#tau_{h}')
  
  l.DrawLatex( lX, lY,        'Channel: '+latexChannel)
  l.DrawLatex( lX, lY-lYstep, 'Trigger: '+trig)
  
  canvas.cd()
  pad2 = TPad('pad2','pad2',0,0.0,1,0.35)
  pad2.SetTopMargin(0.01)
  pad2.SetBottomMargin(0.4)
  pad2.SetLeftMargin(0.2)
  pad2.Draw()
  pad2.cd()
  pad2.SetGridy()
  
  axor2 = TH2D('axor2', 'axor2', nbins, binedges[0], binedges[-1], 100, 0.55, 1.45)
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
  line = TLine(20,1,120,1)
  line.SetLineColor(1)
  line.SetLineWidth(2)
  
  sf.GetYaxis().SetNdivisions(507)
  sf.GetYaxis().SetLabelSize(0.12)
  sf.GetXaxis().SetLabelSize(0.12)
  sf.GetXaxis().SetTitleSize(0.15)
  sf.GetYaxis().SetTitleSize(0.15)
  sf.GetXaxis().SetTitleOffset(1.)
  sf.GetYaxis().SetTitleOffset(0.45)
  sf.GetYaxis().SetTitle('Data/MC')
  sf.GetXaxis().SetTitle(variable)
  
  ##line.Draw()
  sf.Draw('same P0')
  utils.redrawBorder()

  for aname in save_names:
    canvas.SaveAs( aname )

@utils.set_pure_input_namespace
def drawTriggerSF_outputs(args):
  outputs = [[] for _ in range(len(_extensions))]
  for proc in args.mc_processes:
    for ch in args.channels:
      for var in args.variables:
        for trig in args.triggers:
          add = proc + '_' + ch + '_' + var + '_' + trig
          canvas_name = 'trigSF_' + args.data_name + '_' + add + args.subtag
          thisbase = os.path.join(args.outdir, ch, var, '')
          utils.create_single_dir( thisbase )

          for ext,out in zip(_extensions, outputs):
            out.append( os.path.join( thisbase, canvas_name + '.' + ext ) )

  #join all outputs in the same list
  return sum(outputs, []), _extensions
    
@utils.set_pure_input_namespace
def drawTriggerSF(args):
  outputs, extensions = drawTriggerSF_outputs(args)

  # Recover binning
  binedges, nbins = ({} for _ in range(2))
  with h5py.File(args.binedges_filename, 'r') as f:
    group = f[args.subtag]
    for var in args.variables:
      subgroup = group[var]
      binedges[var], nbins[var] = ({} for _ in range(2))
      for chn in args.channels:
        binedges[var][chn] = subgroup[chn][:]
        nbins[var][chn] = len(binedges[var][chn]) - 1

  dt = len(args.triggers)
  dv = len(args.variables) * dt
  dc = len(args.channels) * dv
  dp = len(args.mc_processes) * dc
  for ip,proc in enumerate(args.mc_processes):
    for ic,ch in enumerate(args.channels):
      for iv,var in enumerate(args.variables):
        for it,trig in enumerate(args.triggers):
          index = ip*dc + ic*dv + iv*dt + it
          names = [ outputs[index + dp*x] for x in range(len(extensions)) ]

          if args.debug:
            for name in names:
              print('[=debug=] {}'.format(name))
            print("process={}, channel={}, variable={}, trigger={}".format(proc, ch, var, trig))
            print()

          checkTrigger( args, proc, ch, var, trig, names,
                        binedges[var][chn], nbins[var][chn] )
          
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Draw trigger scale factors')

    parser.add_argument('-i', '--indir', help='Inputs directory', required=True)
    parser.add_argument('-x', '--targetsPrefix', help='prefix to the names of the produced outputs (targets in luigi lingo)', required=True)
    parser.add_argument('-t', '--tag', help='string to diferentiate between different workflow runs', required=True)
    parser.add_argument('-d', '--data', help='dataset to be analyzed/plotted', required=True)
    parser.add_argument('-p', '--mc_processes', help='MC processes to be analyzed: Radions, TT, ...', required=True)
    parser.add_argument('--binedges_filename', dest='binedges_filename', required=True, help='in directory')
    parser.add_argument('--debug', action='store_true', help='debug verbosity')
    args = parser.parse_args()

    drawTriggerSF(args)

import os
import argparse
import ctypes
import numpy as np
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
from ROOT import kBlue, kRed

from utils import utils
from luigi_conf import _2Dpairs, _extensions

def setHistoProperties(histo, variables):
  histo.GetYaxis().SetNdivisions(6)
  histo.GetXaxis().SetNdivisions(6)
  histo.GetYaxis().SetLabelSize(0.04)
  histo.GetXaxis().SetLabelSize(0.04)
  histo.GetXaxis().SetTitleSize(0.04)
  histo.GetYaxis().SetTitleSize(0.04)
  histo.GetXaxis().SetTitleOffset(0.)
  histo.GetYaxis().SetTitleOffset(1.25)
  histo.GetXaxis().SetTitle(variables[0])
  histo.GetYaxis().SetTitle(variables[1])

def setHisto(histo, variables):
  setHistoProperties(histo, variables)
  return histo

def drawOverlayed2DHistograms(effdata, var, opt):
  histo_data = setHisto(effdata.GetPaintedHistogram(), var)
  histo_data_pass = effdata.GetCopyPassedHisto()
  histo_data_tot = setHisto( effdata.GetCopyTotalHisto(), var )
  histo_data.SetBarOffset(0.15);
  histo_data.SetMarkerSize(.75)
  histo_data_pass.SetBarOffset(-0.15);
  histo_data_pass.SetMarkerColor(kBlue-4);
  histo_data_pass.SetMarkerSize(.8)
  histo_data_tot.SetBarOffset(-0.3);
  histo_data_tot.SetMarkerColor(kRed-9);
  histo_data_tot.SetMarkerSize(.8)
  histo_data.Draw(opt)
  histo_data_pass.Draw("text min0 same")
  histo_data_tot.Draw("text min0 same")

def paintChannelAndTrigger(channel, trig):
  lX, lY, lYstep = 0.06, 0.96, 0.03
  l = TLatex()
  l.SetNDC()
  l.SetTextFont(72)
  l.SetTextSize(0.03)
  l.SetTextColor(1)

  latexChannel = copy(channel)
  latexChannel.replace('mu','#mu')
  latexChannel.replace('tau','#tau_{h}')
  latexChannel.replace('Tau','#tau_{h}')
  l.DrawLatex( lX, lY, 'Channel: '+latexChannel)
  l.DrawLatex( lX, lY-lYstep, 'Trigger: '+trig)

def check2DTrigger(args, proc, channel, var, trig, save_names):
  _name = lambda a,b,c,d : a + b + c + d + '.root'
  histo_options = 'colz text e min0'
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

  addVarNames = lambda var1,var2 : var1 + '_VERSUS_' + var2
  vname = addVarNames( var[0], var[1] )
  eff_names = { 'ref_vs_trig': 'effRefVsTrig_{}_{}_{}'.format(channel, trig, vname),
               }
  
  eff2D_mc   = { k: utils.getROOTObject(v, file_mc)   for k,v in eff_names.items() }
  eff2D_data = { k: utils.getROOTObject(v, file_data) for k,v in eff_names.items() }

  if args.debug:
    print('[=debug=] Plotting...')  
  canvas_data = TCanvas( os.path.basename(save_names[0][0]).split('.')[0], 'canvas_data', 600, 600 )
  canvas_data.SetLeftMargin(0.10);
  canvas_data.SetRightMargin(0.15);
  canvas_data.cd()

  eff2D_data['ref_vs_trig'].Draw('colz')
  ROOT.gPad.Update()
  histo_data = setHisto(eff2D_data['ref_vs_trig'].GetPaintedHistogram(), var)
  histo_data_pass = eff2D_data['ref_vs_trig'].GetCopyPassedHisto()
  histo_data_tot = setHisto( eff2D_data['ref_vs_trig'].GetCopyTotalHisto(), var )
  histo_data.SetBarOffset(0.15);
  histo_data_tot.SetMarkerSize(.75)
  histo_data_pass.SetBarOffset(0.);
  histo_data_pass.SetMarkerColor(kRed-9);
  histo_data_pass.SetMarkerSize(.8)
  histo_data_tot.SetBarOffset(-0.15);
  histo_data_tot.SetMarkerColor(kBlue-4);
  histo_data_tot.SetMarkerSize(.8)
  histo_data.Draw(histo_options)
  histo_data_pass.Draw("text min0 same")
  histo_data_tot.Draw("text min0 same")
  ROOT.gPad.Update();
  
  lX, lY, lYstep = 0.8, 0.92, 0.045
  l_data = TLatex()
  l_data.SetNDC()
  l_data.SetTextFont(72)
  l_data.SetTextColor(2)
  l_data.DrawLatex(lX, lY, 'Data')

  paintChannelAndTrigger(channel, trig)
  utils.redrawBorder()

  canvas_mc = TCanvas( os.path.basename(save_names[0][1]).split('.')[0], 'canvas_mc', 600, 600 )
  canvas_mc.SetLeftMargin(0.10);
  canvas_mc.SetRightMargin(0.15);
  canvas_mc.cd()

  eff2D_mc['ref_vs_trig'].Draw('colz')
  ROOT.gPad.Update()
  histo_mc = setHisto(eff2D_mc['ref_vs_trig'].GetPaintedHistogram(), var)
  histo_mc_pass = eff2D_mc['ref_vs_trig'].GetCopyPassedHisto()
  histo_mc_tot = setHisto( eff2D_mc['ref_vs_trig'].GetCopyTotalHisto(), var )
  histo_mc.SetBarOffset(0.15);
  histo_data.SetMarkerSize(.75)
  histo_data_pass.SetBarOffset(0.);
  histo_data_pass.SetMarkerColor(kRed-9);
  histo_data_pass.SetMarkerSize(.8)
  histo_data_tot.SetBarOffset(-0.15);
  histo_data_tot.SetMarkerColor(kBlue-4);
  histo_data_tot.SetMarkerSize(.8)
  histo_data.Draw(histo_options)
  histo_data_pass.Draw("text min0 same")
  histo_data_tot.Draw("text min0 same")
  ROOT.gPad.Update();

  lX, lY, lYstep = 0.7, 0.92, 0.045
  l_mc = TLatex()
  l_mc.SetNDC()
  l_mc.SetTextFont(72)
  l_mc.SetTextColor(2)
  l_mc.DrawLatex(lX, lY, proc)

  paintChannelAndTrigger(channel, trig)
  utils.redrawBorder()

  canvas_sf = TCanvas( os.path.basename(save_names[0][2]).split('.')[0], 'canvas_sf', 600, 600 )
  canvas_sf.SetLeftMargin(0.10);
  canvas_sf.SetRightMargin(0.15);
  canvas_sf.cd()

  histo_sf = histo_data.Clone('sf')
  histo_sf.Divide(histo_mc)
  histo_sf.SetAxisRange(-0.5, 2.5, 'Z');
  histo_sf.SetMarkerSize(.75)
  histo_sf.Draw(histo_options)

  lX, lY, lYstep = 0.6, 0.92, 0.045
  l_mc = TLatex()
  l_mc.SetNDC()
  l_mc.SetTextFont(72)
  l_mc.SetTextColor(2)
  l_mc.DrawLatex(lX, lY, 'Data / {}'.format(proc))

  paintChannelAndTrigger(channel, trig)
  utils.redrawBorder()

  for aname in save_names:
    canvas_data.SaveAs( aname[0]  )
    canvas_mc.SaveAs(   aname[1]  )
    canvas_sf.SaveAs(   aname[2]  )
  
@utils.set_pure_input_namespace
def draw2DTriggerSF_outputs(args):
  outputs = [[] for _ in range(len(_extensions))]

  for proc in args.mc_processes:
    for ch in args.channels:
      for trig in args.triggers:
        if trig in _2Dpairs.keys():
          for variables in _2Dpairs[trig]:
            add = proc + '_' + ch + '_' + trig + '_' + variables[0] + '_VS_' + variables[1]
            canvas_data_name = 'EffData_' + args.data_name + '_' + add + args.subtag
            canvas_mc_name = 'EffMC_' + args.data_name + '_' + add + args.subtag
            canvas_sf_name = 'SF_' + args.data_name + '_' + add + args.subtag
            thisbase = os.path.join(args.outdir, ch, '')
            utils.create_single_dir( thisbase )

            for ext,out in zip(_extensions, outputs):
              out.append( ( os.path.join( thisbase, canvas_data_name + '.' + ext ),
                            os.path.join( thisbase, canvas_mc_name   + '.' + ext ),
                            os.path.join( thisbase, canvas_sf_name   + '.' + ext )) )

  #join all outputs in the same list
  return sum(outputs, []), _extensions
    
@utils.set_pure_input_namespace
def draw2DTriggerSF(args):
  ROOT.gStyle.SetOptStat(0)
  ROOT.gStyle.SetOptTitle(0)
  ROOT.gStyle.SetPaintTextFormat("4.2f")

  outputs, extensions = draw2DTriggerSF_outputs(args)

  # loop through variables, triggers, channels and processes
  dv = 0
  for key in _2Dpairs:
    dv += len(_2Dpairs[key])
  dc = len(args.channels) * dv
  dp = len(args.mc_processes) * dc
  for ip,proc in enumerate(args.mc_processes):
    for ic,ch in enumerate(args.channels):
      iv = -1
      for trig in args.triggers:
        if trig in _2Dpairs.keys():
          for variables in _2Dpairs[trig]:
            iv += 1
            index = ip*dc + ic*dv + iv
            names = [ outputs[index + dp*x] for x in range(len(extensions)) ]

            if args.debug:
              for name in names:
                print('[=debug=] {}'.format(name))
              print("process={}, channel={}, variables={}, trigger={}".format(proc, ch, variables, trig))
              print()

            check2DTrigger( args, proc, ch, variables, trig, names )
          
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Draw trigger scale factors')
    parser.add_argument('-i', '--indir', help='Inputs directory', required=True)
    parser.add_argument('-x', '--targetsPrefix', help='prefix to the names of the produced outputs (targets in luigi lingo)', required=True)
    parser.add_argument('-t', '--tag', help='string to diferentiate between different workflow runs', required=True)
    parser.add_argument('-d', '--data', help='dataset to be analyzed/plotted', required=True)
    parser.add_argument('-p', '--mc_processes', help='MC processes to be analyzed: Radions, TT, ...', required=True)
    parser.add_argument('--debug', action='store_true', help='debug verbosity')
    args = parser.parse_args()

    draw2DTriggerSF(args)

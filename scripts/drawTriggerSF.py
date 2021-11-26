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

from utils import utils

def getHisto(name, afile):
  _keys = afile.GetListOfKeys()
  if name not in _keys:
    print('Name={}'.format(name))
    print('Keys: {}'.format([n.GetName() for n in _keys]))
    raise ValueError('Wrong histogram name!')
  return afile.Get(name)
  
def RedrawBorder():
  """
  this little macro redraws the axis tick marks and the pad border lines.
  """
  ROOT.gPad.Update();
  ROOT.gPad.RedrawAxis()
  l = TLine()
  l.SetLineWidth(2)

  l.DrawLine(ROOT.gPad.GetUxmin(), ROOT.gPad.GetUymax(), ROOT.gPad.GetUxmax(), ROOT.gPad.GetUymax()) #top border
  l.DrawLine(ROOT.gPad.GetUxmax(), ROOT.gPad.GetUymin(), ROOT.gPad.GetUxmax(), ROOT.gPad.GetUymax()) #right border
  l.DrawLine(ROOT.gPad.GetUxmin(), ROOT.gPad.GetUymin(), ROOT.gPad.GetUxmin(), ROOT.gPad.GetUymax()) #left border
  l.DrawLine(ROOT.gPad.GetUxmin(), ROOT.gPad.GetUymin(), ROOT.gPad.GetUxmax(), ROOT.gPad.GetUymin()) #bottom border

def checkTrigger(args, proc, channel, variable, trig, save_names):
  _name = lambda a,b,c,d : a + b + c + '.' + d + '.root'
  fname = _name(args.targetsPrefix, args.mc_name,
                args.target_suffix, args.subtag )
  # fname_data = args.targetsPrefix+data+'_sum.HTcut600.root'
  fname_data = _name( args.targetsPrefix, args.data_name,
                      args.target_suffix, args.subtag )

  f_data_name = os.path.join(args.indir, fname_data)
  f_data = TFile( f_data_name, 'READ');
  f_in_name = os.path.join(args.indir, fname)
  f_in   = TFile( f_in_name, 'READ');

  if args.debug:
    print('Open files:')
    print(' - Data: {}'.format(f_data_name))
    print(' - MC: {}'.format(f_in_name))
    print(' - Args: proc={proc}, channel={channel}, variable={variable}, trig={trig}'
          .format(proc=proc, channel=channel, variable=variable, trig=trig))
    
  hname_all     = 'passALL_{}_{}'.format(channel, variable)
  hname_met     = 'passMET_{}_{}_{}'.format(channel, variable, trig)
  hname_metonly = 'passMETOnly_{}_{}_{}'.format(channel, variable, trig)
  hname_met.replace('_all_all','_all')
  hname_metonly.replace('_all_all','_all')

  h_passALL     = getHisto(hname_all, f_in)
  h_passMET     = getHisto(hname_met, f_in)
  h_passMETonly = getHisto(hname_metonly, f_in)

  d_passALL     = getHisto(hname_all, f_data)
  d_passMET     = getHisto(hname_met, f_data)
  d_passMETonly = getHisto(hname_metonly, f_data)
  
  if args.debug:
    print('Calculating MC efficiency...')  
  effmc = TEfficiency(h_passMET, h_passALL)
  heffmc = h_passMET.Clone('h_eff')
  geffmc = TGraphAsymmErrors()
  geffmc.Divide(heffmc,h_passALL,'cp')
  heffmc.Divide(heffmc,h_passALL,1,1,'B')

  if args.debug:
    print('Calculating Data efficiency...')  
  eff = TEfficiency(d_passMET, d_passALL)
  heff = d_passMET.Clone('d_eff')
  geff = TGraphAsymmErrors()
  #heff.Divide(heff,d_passALL,1,1,'B')
  geff.Divide(heff, d_passALL, 'cp')
  heff.Divide(heff, d_passALL, 1, 1, 'B')
  
  #loop on tgraphs to get errors
  npoint = geffmc.GetN()
  sizelist = 6
  xp, yp = ( [[] for _ in range(sizelist)] for _ in range(2) )
  ye_up, ye_down = ( [[] for _ in range(sizelist)] for _ in range(2) )
  
  for i in range(sizelist):
    #ctypes conversions needed
    xp[i] = ctypes.c_double(0.)
    yp[i] = ctypes.c_double(0.)
    geffmc.GetPoint(i, xp[i], yp[i])
    xp[i] = xp[i].value
    yp[i] = yp[i].value

    ye_up[i] = geffmc.GetErrorYhigh(i)
    ye_down[i] = geffmc.GetErrorYlow(i)
    if args.debug:
      print('xp[{}] = {} - yp[{}] = {} +{}/-{}\n'.format(i,xp[i],i,yp[i],ye_up[i],ye_down[i]))

  if args.debug:
    print('=======')

  dxp, dyp = ( [[] for _ in range(sizelist)] for _ in range(2) )
  dye_up, dye_down = ( [[] for _ in range(sizelist)] for _ in range(2) )
  
  sf_xp, sf_yp = ( [[] for _ in range(sizelist)] for _ in range(2) )
  sf_ye_up, sf_ye_down = ( [[] for _ in range(sizelist)] for _ in range(2) )
  
  for i in range(sizelist):
    dxp[i] = ctypes.c_double(0.)
    dyp[i] = ctypes.c_double(0.)
    geff.GetPoint(i, dxp[i], dyp[i])
    dxp[i] = dxp[i].value
    dyp[i] = dyp[i].value
    
    dye_up[i] = geff.GetErrorYhigh(i)
    dye_down[i] = geff.GetErrorYlow(i)
    if args.debug:
      print('xp[{}] = {} - yp[{}] = {} +{}/-{}\n'.format(i,dxp[i],i,dyp[i],dye_up[i],dye_down[i]))

    sf_xp[i] = xp[i]
    sf_yp[i] = dyp[i]/yp[i]
    sf_ye_up[i]   = np.sqrt( ye_up[i]*ye_up[i] + dye_up[i]*dye_up[i] )
    sf_ye_down[i] = np.sqrt( ye_down[i]*ye_down[i] + dye_down[i]*dye_down[i] )

  if args.debug:
    print('=== Scale Factors ====')
    for i in range(sizelist):
      print('xp[{}] = {} - yp[{}] = {} +{}/-{}\n'.format(i,sf_xp[i],i,sf_yp[i],sf_ye_up[i],sf_ye_down[i]))

    print('all good')
  
  effmc = TEfficiency(h_passMET, h_passALL)
  sf1 = heff.Clone('sf')
  a = np.array([50 for _ in range(sizelist)])

  sf = TGraphAsymmErrors(sizelist,
                         np.array(sf_xp).astype(dtype=np.double), np.array(sf_yp).astype(dtype=np.double),
                         a.astype(dtype=np.double), a.astype(dtype=np.double),
                         np.array(sf_ye_down).astype(dtype=np.double), np.array(sf_ye_up).astype(dtype=np.double))
  #sf.Divide(heffmc) COMMENT???
  sf.Divide(sf1, heffmc, 'cp')
  if args.debug:
    print('all good 2')
  
  #DRAW MET & TRIG EFF ON SAME CANVAS
  #c1 = TCanvas('c1','',600,400)
  
  canvas = TCanvas( os.path.basename(save_names[0]).split('.')[0], 'canvas', 600, 600)
  ROOT.gStyle.SetOptStat(0)
  ROOT.gStyle.SetOptTitle(0)
  canvas.cd()
  
  eff.SetLineColor(1)
  eff.SetLineWidth(2)
  eff.SetMarkerColor(1)
  eff.SetMarkerSize(1.5)
  eff.SetMarkerStyle(20)
    
  effmc.SetLineColor(ROOT.kRed)
  effmc.SetLineWidth(2)
  effmc.SetMarkerColor(ROOT.kRed)
  effmc.SetMarkerSize(1.4)
  effmc.SetMarkerStyle(22)
  
  sf.SetLineColor(ROOT.kRed)
  sf.SetLineWidth(2)
  sf.SetMarkerColor(ROOT.kRed)
  sf.SetMarkerSize(1.4)
  sf.SetMarkerStyle(22)
  
  #  TH1D *line_sf      = (TH1D*)sf      . Clone( 'line_sf     ' )
  #  TH1D *line_eff     = (TH1D*)eff     . Clone( 'line_eff    ' )
  #  TH1D *line_effmc   = (TH1D*)effmc   . Clone( 'line_effmc  ' )
  #  for(int i = 1 i<11 ++i){
  #    line_sf      . SetBinError(i, 0.000000001 )
  #    line_eff     . SetBinError(i, 0.000000001 )
  #    line_effmc   . SetBinError(i, 0.000000001 )
  #  }
  
  if args.debug:
    print('did it break')
  
  pad1 = TPad('pad1', 'pad1', 0, 0.35, 1, 1)
  pad1.SetBottomMargin(0.005)
  pad1.SetLeftMargin(0.2)
  pad1.Draw()
  pad1.cd()
  
  axor = TH2D('axor','axor',4,0,600,100,-0.1,1.7)
  axor.GetYaxis().SetTitle('Efficiency')
  axor.GetXaxis().SetLabelOffset(1)
  axor.GetXaxis().SetLabelOffset(1.)
  axor.GetYaxis().SetTitleSize(0.08)
  axor.GetYaxis().SetTitleOffset(.85)
  axor.GetXaxis().SetLabelSize(0.07)
  axor.GetYaxis().SetLabelSize(0.07)
  axor.Draw()
  
  #line_eff.Draw('SAME ')
  #line_effmc.Draw('SAME ')
  
  eff.Draw('SAME p0 e ')
  effmc.Draw('SAME p0 ')
  pad1.RedrawAxis()
  
  leg = TLegend(0.25, 0.55, 0.47, 0.75)
  leg.SetFillColor(0)
  leg.SetShadowColor(0)
  leg.SetBorderSize(0)
  leg.SetTextSize(0.06)
  leg.SetFillStyle(0)
  leg.SetTextFont(42)
  
  leg.AddEntry(eff, 'Data', 'p')
  #sprintf(message, proc)
  leg.AddEntry(effmc, proc, 'p')
  leg.Draw('same')
  
  RedrawBorder()
  
  lX, lY, lYstep = 0.25, 0.84, 0.045
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
  
  axor2 = TH2D('axor2','axor2',4,0,600,100,0.8,1.2)
  axor2.GetYaxis().SetNdivisions(507)
  axor2.GetYaxis().SetLabelSize(0.12)
  axor2.GetXaxis().SetLabelSize(0.12)
  axor2.SetTitleSize(0.15,'X')
  axor2.SetTitleSize(0.15,'Y')
  axor2.GetXaxis().SetTitleOffset(1.)
  axor2.GetYaxis().SetTitleOffset(0.45)
  axor2.GetYaxis().SetTitle('Data/MC')
  axor2.GetXaxis().SetTitle('MET [GeV]')
  #if strstr(args.subtag,'MET'):
  #  axor2.GetXaxis().SetTitle('H_{T} [GeV]')
    
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
  sf.GetXaxis().SetTitle('MET [GeV]')
  
  ##line.Draw()
  sf.Draw('same P0')
  RedrawBorder()

  for aname in save_names:
    canvas.SaveAs( aname )

@utils.set_pure_input_namespace
def drawTriggerSF_outputs(args):
  ext1, ext2, ext3 = 'png', 'pdf', 'C'
  outputs_png, outputs_pdf, outputs_c = ([] for _ in range(3))
  for proc in args.mc_processes:
    for trig in args.triggers:
      canvas_name = 'triggerSF_' + args.data_name + '_' + proc + '_trig_' + trig + '.' + args.subtag
      for var in args.variables:
        for ch in args.channels:
          thisbase = os.path.join('/eos/home-b/bfontana/www/TriggerScaleFactors', 'fig', ch, var, '')

          utils.create_single_dir( thisbase )
          png_out = os.path.join( thisbase, canvas_name + '.' + ext1 )
          outputs_png.append(png_out)
          
          utils.create_single_dir( thisbase )
          pdf_out = os.path.join( thisbase, canvas_name + '.' + ext2)
          outputs_pdf.append(pdf_out)

          utils.create_single_dir( thisbase )
          c_out = os.path.join( thisbase, canvas_name + '.' + ext3)
          outputs_c.append(c_out)

  #join all outputs in the same list
  outputs_png.extend(outputs_pdf)
  outputs_png.extend(outputs_c)
  
  return outputs_png
    
@utils.set_pure_input_namespace
def drawTriggerSF(args):
  outputs = drawTriggerSF_outputs(args)
  dt = len(args.triggers)
  dv = len(args.variables) * dt
  dc = len(args.channels) * dv
  dp = len(args.mc_processes) * dc
  for ip,proc in enumerate(args.mc_processes):
    for ic,ch in enumerate(args.channels):
      for iv,var in enumerate(args.variables):
        for it,trig in enumerate(args.triggers):
          index = ip*dc + ic*dv + iv*dt + it
          names = ( outputs[index], outputs[index + dp], outputs[index + 2*dp] )
          if args.debug:
            print('Calling checkTrigger: ' + proc, ch, var, trig, names)
          checkTrigger( args, proc, ch, var, trig, names )
          
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Draw trigger scale factors')
    parser.add_argument('-i', '--indir', help='Inputs directory', required=True)
    parser.add_argument('-x', '--targetsPrefix', help='prefix to the names of the produced outputs (targets in luigi lingo)', required=True)
    parser.add_argument('-t', '--tag', help='string to diferentiate between different workflow runs', required=True)
    parser.add_argument('-d', '--data', help='dataset to be analyzed/plotted', required=True)
    parser.add_argument('-p', '--mc_processes', help='MC processes to be analyzed: Radions, TT, ...', required=True)
    parser.add_argument('--debug', action='store_true', help='debug verbosity')
    args = parser.parse_args()

    drawTriggerSF(args)

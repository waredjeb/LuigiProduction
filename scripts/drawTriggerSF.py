import os
import argparse
from copy import copy

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

'''
0 - HLT_IsoMu24_v
1 - HLT_IsoMu27_v
2 - HLT_Ele32_WPTight_Gsf_v
3 - HLT_Ele35_WPTight_Gsf_v
4 - HLT_DoubleMediumChargedIsoPFTauHPS35_Trk1_eta2p1_Reg_v
5 - HLT_IsoMu20_eta2p1_LooseChargedIsoPFTauHPS27_eta2p1_CrossL1_v
6 - HLT_Ele24_eta2p1_WPTight_Gsf_LooseChargedIsoPFTauHPS30_eta2p1_CrossL1_v
7 - HLT_VBF_DoubleLooseChargedIsoPFTau20_Trk1_eta2p1_v
8 - HLT_VBF_DoubleLooseChargedIsoPFTauHPS20_Trk1_eta2p1_v

9 - HLT_PFHT500_PFMET100_PFMHT100_IDTight_v
10 - HLT_PFMET100_PFMHT100_IDTight_PFHT60_v
11 - HLT_PFMET110_PFMHT110_IDTight_v
12 - HLT_PFMET200_HBHECleaned_v
13 - HLT_PFMETNoMu100_PFMHTNoMu100_IDTight_PFHT60_v
14 - HLT_PFMETNoMu110_PFMHTNoMu110_IDTight_v
'''

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
  gPad.Update();
  gPad.RedrawAxis()
  l = TLine()
  l.SetLineWidth(2)

  l.DrawLine(gPad.GetUxmin(), gPad.GetUymax(), gPad.GetUxmax(), gPad.GetUymax()) #top border
  l.DrawLine(gPad.GetUxmax(), gPad.GetUymin(), gPad.GetUxmax(), gPad.GetUymax()) #right border
  l.DrawLine(gPad.GetUxmin(), gPad.GetUymin(), gPad.GetUxmin(), gPad.GetUymax()) #left border
  l.DrawLine(gPad.GetUxmin(), gPad.GetUymin(), gPad.GetUxmax(), gPad.GetUymin()) #bottom border

def check_trigger(args, proc, trig, channel, save_names):
  _name = lambda a,b,c,d : a + b + c + '.' + d + '.root'
  fname = _name(args.targetsPrefix, args.mc_name,
                args.target_suffix, args.subtag )
  # fname_data = args.targetsPrefix+data+'_sum.HTcut600.root'
  fname_data = _name( args.targetsPrefix, args.dataset_name,
                      args.target_suffix, args.subtag )

  f_data = TFile( os.path.join(args.indir, fname_data), 'READ');
  f_in   = TFile( os.path.join(args.indir, fname), 'READ');

  print( 'Opening file: {}'.format( os.path.join(args.indir, fname) ) )
    
  hname_all     = 'passALL_'     + channel
  hname_met     = 'passMET_'     + channel + '_' + trig
  hname_metonly = 'passMETonly_' + channel + '_' + trig
  hname_met.replace('_all_all','_all')
  hname_metonly.replace('_all_all','_all')

  h_passMET     = getHisto(hname_met, f_in)
  h_passALL     = getHisto(hname_all, f_in)
  h_passMETonly = getHisto(hname_metonly, f_in)
  
  d_passMET     = getHisto(hname_met, f_data)
  d_passALL     = getHisto(hname_all, f_data)
  d_passMETonly = getHisto(hname_metonly, f_data)
  
  #  h_passMET     .Rebin(3)
  #  h_passALL     .Rebin(3)
  #  h_passMETonly .Rebin(3)
  #  d_passMET     .Rebin(3)
  #  d_passALL     .Rebin(3)
  #  d_passMETonly .Rebin(3)

  print('effmc')
  
  effmc = TEfficiency(h_passMET, h_passALL)
  heffmc = h_passMET.Clone('h_eff')
  geffmc = TGraphAsymmErrors()
  geffmc.Divide(heffmc,h_passALL,'cp')
  heffmc.Divide(heffmc,h_passALL,1,1,'B')
  
  print('effdata')
  eff = TEfficiency(d_passMET, d_passALL)
  heff = d_passMET.Clone('d_eff')
  geff = TGraphAsymmErrors()
  #heff.Divide(heff,d_passALL,1,1,'B')
  geff.Divide(heff,d_passALL,'cp')
  heff.Divide(heff,d_passALL,1,1,'B')
  
  #loop on tgraphs to get errors
  npoint = geffmc.GetN()
  sizelist = 6
  xp, yp = ( [[] for _ in range(sizelist)] for _ in range(2) )
  ye_up, ye_down = ( [[] for _ in range(sizelist)] for _ in range(2) )
  
  for i in range(sizelist):
    geffmc.GetPoint(i, xp[i], yp[i])
    ye_up[i] = geffmc.GetErrorYhigh(i)
    ye_down[i] = geffmc.GetErrorYlow(i)
    print('xp[{}] = {} - yp[{}] = {} +{}/-{}\n'.format(i,xp[i],i,yp[i],ye_up[i],ye_down[i]))
  print('=======')

  dxp, dyp = ( [[] for _ in range(sizelist)] for _ in range(2) )
  dye_up, dye_down = ( [[] for _ in range(sizelist)] for _ in range(2) )
  
  sf_xp, sf_yp = ( [[] for _ in range(sizelist)] for _ in range(2) )
  sf_ye_up, sf_ye_down = ( [[] for _ in range(sizelist)] for _ in range(2) )
  
  for i in range(sizelist):
    geff.GetPoint(i,dxp[i],dyp[i])
    dye_up[i] = geff.GetErrorYhigh(i)
    dye_down[i] = geff.GetErrorYlow(i)
    printf('xp[%d] = %g - yp[%d] = %g +%g/-%g\n',i,dxp[i],i,dyp[i],dye_up[i],dye_down[i])

  sf_xp[i] = xp[i]
  sf_yp[i] = dyp[i]/yp[i]
  sf_ye_up[i]   = np.sqrt( ye_up[i]*ye_up[i] + dye_up[i]*dye_up[i] )
  sf_ye_down[i] = np.sqrt( ye_down[i]*ye_down[i] + dye_down[i]*dye_down[i] )
    
  print('=== SF ====')
  for i in range(sizelist):
    print('xp[{}] = [] - yp[{}] = {} +{}/-{}\n'.format(i,sf_xp[i],i,sf_yp[i],sf_ye_up[i],sf_ye_down[i]))

  print('all good')
  
  effmc = TEfficiency(h_passMET, h_passALL)
  sf1 = heff.Clone('sf')
  a = [50 for _ in range(sizelist)]
  sf = TGraphAsymmErrors(sizelist, sf_xp, sf_yp, a, a, sf_ye_down, sf_ye_up)
  sf.Divide(heffmc)
  sf.Divide(sf1, heffmc, 'cp')
  print('all good 2')
  
  #DRAW MET & TRIG EFF ON SAME CANVAS
  #c1 = TCanvas('c1','',600,400)
  
  canvas = TCanvas('canvas', '', 600, 600)
  gStyle.SetOptStat(0)
  gStyle.SetOptTitle(0)
  canvas.cd()
  
  eff.SetLineColor(1)
  eff.SetLineWidth(2)
  eff.SetMarkerColor(1)
  eff.SetMarkerSize(1.5)
  eff.SetMarkerStyle(20)
    
  effmc.SetLineColor(kRed)
  effmc.SetLineWidth(2)
  effmc.SetMarkerColor(kRed)
  effmc.SetMarkerSize(1.4)
  effmc.SetMarkerStyle(22)
  
  sff.SetLineColor(kRed)
  sff.SetLineWidth(2)
  sff.SetMarkerColor(kRed)
  sff.SetMarkerSize(1.4)
  sff.SetMarkerStyle(22)
  
  #  TH1D *line_sf      = (TH1D*)sf      . Clone( 'line_sf     ' )
  #  TH1D *line_eff     = (TH1D*)eff     . Clone( 'line_eff    ' )
  #  TH1D *line_effmc   = (TH1D*)effmc   . Clone( 'line_effmc  ' )
  #  for(int i = 1 i<11 ++i){
  #    line_sf      . SetBinError(i, 0.000000001 )
  #    line_eff     . SetBinError(i, 0.000000001 )
  #    line_effmc   . SetBinError(i, 0.000000001 )
  #  }
  
  
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
  sprintf(message, proc)
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
  l.DrawLatex( lX, lY-lYstep, 'Trigger(s): '+trig)
  
  canvas.cd()
  pad2 = TPad('pad2','pad2',0,0.0,1,0.35)
  pad2.SetTopMargin(0.005)
  pad2.SetBottomMargin(0.4)
  pad2.SetLeftMargin(0.2)
  pad2.Draw()
  pad2.cd()
  pad2.SetGridy()
  
  axor2 = TH2D('axor2','axor2',4,0,600,100,0.8,1.2)
  axor2.GetYaxis().SetNdivisions(507)
  axor2.GetYaxis().SetLabelSize(0.13)
  axor2.GetXaxis().SetLabelSize(0.13)
  axor2.SetTitleSize(0.15,'X')
  axor2.SetTitleSize(0.15,'Y')
  axor2.GetXaxis().SetTitleOffset(1.)
  axor2.GetYaxis().SetTitleOffset(0.45)
  axor2.GetYaxis().SetTitle('Data/MC')
  axor2.GetXaxis().SetTitle('MET [GeV]')
  if strstr(args.subtag,'MET'):
    axor2.GetXaxis().SetTitle('H_{T} [GeV]')
    
  axor2.Draw()
  line = TLine(20,1,120,1)
  line.SetLineColor(1)
  line.SetLineWidth(2)
  
  sf.GetYaxis().SetNdivisions(507)
  sf.GetYaxis().SetLabelSize(0.13)
  sf.GetXaxis().SetLabelSize(0.13)
  sf.GetXaxis().SetTitleSize(0.15)
  sf.GetYaxis().SetTitleSize(0.15)
  sf.GetXaxis().SetTitleOffset(1.)
  sf.GetYaxis().SetTitleOffset(0.45)
  sf.GetYaxis().SetTitle('Data/MC')
  sf.GetXaxis().SetTitle('MET [GeV]')
  
  ##line.Draw()
  sf.Draw('same P0')
  RedrawBorder()

  canvas.SaveAs( save_names[0] )
  canvas.SaveAs( save_names[1] )

@utils.set_pure_input_namespace
def drawTriggerSF_outputs(args):
  outputs_png, outputs_pdf = ([] for _ in range(2))
  for proc in args.mc_processes:
    for trig in args.triggers:
      canvas_name = 'triggerSF_' + args.dataset_name + '_' + proc + '_trig_' + trig + '.' + args.subtag
      for ch in args.channels:
        basename = os.path.join(args.indir, 'fig', ch, 'png', '')
        utils.create_single_dir(basename)
        png_out = os.path.join(basename, canvas_name + '.png')
        outputs_png.append(png_out)
        pdf_out = os.path.join(args.indir, 'fig', ch, 'pdf', canvas_name +'.pdf')
        outputs_pdf.append(pdf_out)
  outputs_png.extend(outputs_pdf) #join all outputs in the same list

  return outputs_png
    
@utils.set_pure_input_namespace
def drawTriggerSF(args):
  outputs = drawTriggerSF_outputs(args)
  dim3 = len(args.channels)
  dim2 = len(args.triggers) * dim3
  dim1 = len(args.mc_processes) * dim2
  for i,proc in enumerate(args.mc_processes):
    for j,trig in enumerate(args.triggers):
      for ch in args.channels:
        names = ( outputs[i*dim2 + j*dim3],
                  outputs[i*dim2 + j*dim3 + dim1] )
        check_trigger( args, proc, trig, ch, names )
        
# check_trigger( 'MET2018_partial', i, 'HTcut600', j, 'all'     , true) #comb
# check_trigger( 'MET2018_partial', i, 'HTcut600', j,  'etau'   , true) #mutau
# check_trigger( 'MET2018_partial', i, 'HTcut600', j,  'mutau'  , true) #etau
# check_trigger( 'MET2018_partial', i, 'HTcut600', j,  'tautau' , true) #tautau
# check_trigger( 'MET2018_partial', i, 'HTcut600', j,  'mumu'   , true) #tautau
# check_trigger( 'MET2018_partial', i, 'HTcut600', j,  'tautau' , true) #tautau
# check_trigger( 'MET2018_sum', i, 'HTcut100', j, 'all'     , true) #comb
# check_trigger( 'MET2018_sum', i, 'HTcut100', j,  'etau'   , true) #mutau
# check_trigger( 'MET2018_sum', i, 'HTcut100', j,  'mutau'  , true) #etau
# check_trigger( 'MET2018_sum', i, 'HTcut100', j,  'tautau' , true) #tautau
# check_trigger( 'MET2018_sum', i, 'HTcut100', j,  'mumu'   , true) #tautau
# check_trigger( 'MET2018_sum', i, 'HTcut100', j,  'tautau' , true) #tautau
# check_trigger( 'MET2018_sum', i, 'noHTcut', j, 'all'     , true) #comb
# check_trigger( 'MET2018_sum', i, 'noHTcut', j,  'etau'   , true) #mutau
# check_trigger( 'MET2018_sum', i, 'noHTcut', j,  'mutau'  , true) #etau
# check_trigger( 'MET2018_sum', i, 'noHTcut', j,  'tautau' , true) #tautau
# check_trigger( 'MET2018_sum', i, 'noHTcut', j,  'mumu'   , true) #tautau
# check_trigger( 'MET2018_sum', i, 'noHTcut', j,  'tautau' , true) #tautau
# check_trigger( 'MET2018_sum', i, 'METcut200', j, 'all'     , true) #comb
# check_trigger( 'MET2018_sum', i, 'METcut200', j,  'etau'   , true) #mutau
# check_trigger( 'MET2018_sum', i, 'METcut200', j,  'mutau'  , true) #etau
# check_trigger( 'MET2018_sum', i, 'METcut200', j,  'tautau' , true) #tautau
# check_trigger( 'MET2018_sum', i, 'METcut200', j,  'mumu'   , true) #tautau
# check_trigger( 'MET2018_sum', i, 'METcut200', j,  'tautau' , true) #tautau

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Draw trigger scale factors')
    parser.add_argument('-i', '--indir', help='Inputs directory', required=True)
    parser.add_argument('-x', '--targetsPrefix', help='prefix to the names of the produced outputs (targets in luigi lingo)', required=True)
    parser.add_argument('-t', '--tag', help='string to diferentiate between different workflow runs', required=True)
    parser.add_argument('-p', '--mc_processes', help='MC processes to be analyzed: Radions, TT, ...', required=True)
    args = parser.parse_args()

    drawTriggerSF(args) 

import os
import sys
import functools
import h5py
from copy import copy

import ROOT
from ROOT import TFile
from ROOT import TCanvas
from ROOT import TLegend
from ROOT import TLatex

from luigi_conf import _extensions
from utils import utils

def plotDist(args, channel, variable, trig, save_names, binedges, nbins):
  _name_join = lambda l: functools.reduce(lambda x,y: x + y, l)
  _name_data = _name_join([args.targetsPrefix, args.data_name, args.target_suffix, args.subtag])
  name_data = os.path.join(args.indir, _name_data + '.root')
  file_data = TFile( name_data, 'READ');

  files_mc, names_mc = ([] for _ in range(2))
  _tbase1 = _name_join([args.targetsPrefix, args.mc_name])
  _tbase2 = _name_join([args.target_suffix, args.subtag])
  for proc in args.mc_processes:
    names_mc.append( os.path.join(args.indir, _name_join(_tbase1 + '_' + proc + _tbase2)) )
    files_mc.append( TFile( names_mc[-1] + '.root', 'READ') )
  # the last MC to be added is the full one (merging all MC subsamples)
  names_mc.append( os.path.join(args.indir, _name_join(_tbase1 + _tbase2)) )
  files_mc.append( TFile( names_mc[-1] + '.root', 'READ') )

  if args.debug:
    print('[=debug=] Open files:')
    print('[=debug=]  - Data: {}'.format(name_data))
    print('[=debug=]  - MC Join: {}'.format(names_mc[0]))
    for n in names_mc[1:]:
      print('[=debug=]  - MC: {}'.format(n))
    print('[=debug=]  - Args: channel={channel}, variable={variable}, trig={trig}'
          .format(channel=channel, variable=variable, trig=trig))

  # histo_names = { 'ref': 'Ref_{}_{}'.format(channel, variable),
  #                 'trig': 'Trig_{}_{}_{}'.format(channel, variable, trig),
  #                 'noref' : 'NoRef_{}_{}_{}'.format(channel, variable, trig)
  #                }
  if trig == 'Reference':
    histo_name = 'Ref_{}_{}'.format(channel, variable)
  else:
    histo_name = 'Trig_{}_{}_{}'.format(channel, variable, trig)
  histo_data = utils.getROOTObject(histo_name, file_data)
  histos_mc = [ utils.getROOTObject(histo_name, f)  for f in files_mc ]
    
  if args.debug:
    print('[=debug=] Plotting...')
    
  canvas = TCanvas( os.path.basename(save_names[0]).split('.')[0], 'canvas', 600, 600 )
  ROOT.gStyle.SetOptStat(0)
  ROOT.gStyle.SetOptTitle(0)
  canvas.cd()

  histo_data.SetLineColor(1)
  histo_data.SetLineWidth(2)
  histo_data.SetMarkerColor(1)
  histo_data.SetMarkerSize(1.5)
  histo_data.SetMarkerStyle(20)
  histo_data.Draw('p0 e')
  histo_data.GetXaxis().SetNdivisions(nbins)
  histo_data.GetYaxis().SetLabelSize(0.04)
  histo_data.GetXaxis().SetLabelSize(0.04)
  histo_data.SetTitleSize(0.04,'X')
  histo_data.SetTitleSize(0.04,'Y')
  histo_data.GetXaxis().SetTitleOffset(1.)
  histo_data.GetYaxis().SetTitleOffset(1.20)
  histo_data.GetYaxis().SetTitle('Counts')
  histo_data.GetXaxis().SetTitle(variable)

  mc_colors = (ROOT.kRed, 14, 24, 34, 44)
  for h in histos_mc:
    h.SetLineColor( mc_colors[histos_mc.index(h)] )
    h.SetLineWidth(2)
    h.SetMarkerColor( mc_colors[histos_mc.index(h)] )
    h.SetMarkerSize(1.4)
    h.SetMarkerStyle(22)
    h.Draw('same p0 l e')      

  leg = TLegend(0.62, 0.70, 0.9, 0.88)
  leg.SetFillColor(0)
  leg.SetShadowColor(0)
  leg.SetBorderSize(0)
  leg.SetTextSize(0.04)
  leg.SetFillStyle(0)
  leg.SetTextFont(42)
  
  leg.AddEntry(histo_data, 'Data', 'p')
  leg.AddEntry(histos_mc[0], 'Full MC', 'p')
  for proc,h in zip(args.mc_processes,histos_mc[1:]):
    leg.AddEntry(h, proc, 'p')
  leg.Draw('same')
  
  utils.redrawBorder()
  
  lX, lY, lYstep = 0.11, 0.87, 0.03
  l = TLatex()
  l.SetNDC()
  l.SetTextFont(72)
  l.SetTextColor(1)
  l.SetTextSize(0.035)
  latexChannel = copy(channel)
  latexChannel.replace('mu','#mu')
  latexChannel.replace('tau','#tau_{h}')
  latexChannel.replace('Tau','#tau_{h}')
  
  l.DrawLatex( lX, lY,        'Channel: '+latexChannel)
  l.DrawLatex( lX, lY-lYstep, 'Trigger: '+trig)
  
  for aname in save_names:
    canvas.SaveAs( aname )

@utils.set_pure_input_namespace
def drawDistributions_outputs(args):
  def _save_figures(base, figname, outputs):
      """Saves the output names, modifying the list in-place"""
      utils.create_single_dir( base )   
      for ext,out in zip(_extensions, outputs):
          out.append( os.path.join( base, figname + '.' + ext ) )

  outputs = [[] for _ in range(len(_extensions))]
  for ch in args.channels:
    for var in args.variables:
        figname = 'distRef_' + ch + '_' + var + args.subtag
        thisbase = os.path.join(args.outdir, ch, var, '')
        _save_figures(thisbase, figname, outputs)
        for trig in args.triggers:
            figname = 'distTrig_' + ch + '_' + var + '_' + trig + args.subtag
            _save_figures(thisbase, figname, outputs)

  #join all outputs in the same list
  return sum(outputs, []), _extensions

@utils.set_pure_input_namespace
def drawDistributions(args):
  outputs, extensions = drawDistributions_outputs(args)

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

  dt = len(args.triggers) + 1 #triggers + reference trigger
  dv = len(args.variables) * dt
  dc = len(args.channels) * dv
  for ic,ch in enumerate(args.channels):
      for iv,var in enumerate(args.variables):
        index = ic*dv + iv*dt
        names = [ outputs[index + dc*x] for x in range(len(extensions)) ]
        
        if args.debug:
          for name in names:
            print('[=debug=] {}'.format(name))
            print("channel={}, variable={}, trigger=Reference".format(ch, var))
            print()

        plotDist( args, ch, var, 'Reference', names,
                  binedges[var][chn], nbins[var][chn] )

        for it,trig in enumerate(args.triggers):
          index = ic*dv + iv*dt + it + 1
          names = [ outputs[index + dc*x] for x in range(len(extensions)) ]
            
          if args.debug:
            for name in names:
              print('[=debug=] {}'.format(name))
              print("channel={}, variable={}, trigger={}".format(ch, var, trig))
              print()
                  
          plotDist( args, ch, var, trig, names,
                    binedges[var][chn], nbins[var][chn] )

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Draw variables distributions')

    parser.add_argument('-i', '--indir', help='Inputs directory', required=True)
    parser.add_argument('--targetsPrefix', help='prefix to the names of the produced outputs (targets in luigi lingo)',
                        required=True)
    parser.add_argument('-t', '--tag', help='string to differentiate between different workflow runs', required=True)
    parser.add_argument('-d', '--data', help='dataset to be analyzed/plotted', required=True)
    parser.add_argument('-p', '--mc_processes', help='MC processes to be analyzed: Radions, TT, ...', required=True)
    parser.add_argument('--binedges_filename', dest='binedges_filename', required=True, help='in directory')
    parser.add_argument('--debug', action='store_true', help='debug verbosity')
    args = parser.parse_args()

    drawDistributions(args)

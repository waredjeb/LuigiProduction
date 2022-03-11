import os
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

import sys
sys.path.append( os.path.join(os.environ['CMSSW_BASE'], 'src', 'METTriggerStudies'))

from utils.utils import (
  create_single_dir,
  getKeyList,
  get_root_object,
  get_histo_names,
  redrawBorder,
  rewriteCutString,
  restoreBinning,
)

from luigi_conf import (
  _extensions,
  _placeholder_cuts,
)

def drawSingleEff(indir, channel, var, trig,
                  eff_names, sprof_names, subtag, debug):
    _name = lambda a,b,c,d : a + b + c + d + '.root'
    name_mc = os.path.join(indir, _name( tprefix, mc_name, '_Sum', subtag ))
    file_mc = TFile.Open(name_mc)
  
    if debug:
        print('[=debug=] Open files:')
        #print('[=debug=]  - Data: {}'.format(name_data))
        print('[=debug=]  - MC: {}'.format(name_mc))
        print( '[=debug=]  - Args: proc={proc}, '.format(proc=proc)
               + 'channel={channel}, '.format(channel=channel)
               + 'variable={variable}, '.format(variable=variable)
               + 'trig={trig} '.format(trig=trig) )

    hname_mc = get_histo_names('Ref1D')(channel, variable)
      
    eff2D_mc = get_root_object(hname_mc, file_mc).GetPaintedHistogram()
    
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

    lX, lY, lYstep = 0.25, 0.84, 0.1
    l = TLatex()
    l.SetNDC()
    l.SetTextFont(72)
    l.SetTextColor(1)
    l.DrawLatex( lX, lY,        'Channel: '+channel)
    l.DrawLatex( lX, lY-lYstep, 'Trigger: '+trig)

    for aname in eff_names:
      eff_canvas.SaveAs( aname )

    #################################################################
    ############## DRAW PROFILES ####################################
    #################################################################
    prof_canvas = TCanvas( prof_canvas_name, prof_canvas_name, 600, 600 )
    prof_canvas.cd()
    eff2D_mc.ProfileX()

    for aname in prof_names:
      eff_prof.SaveAs( aname )

def _get_plot_name(chn, var, trig, subtag, isprofile):
    """
    A 'XXX' placeholder is added for later replacement by all cuts considered
      for the same channel, variable and trigger combination.
    Without the placeholder one would have to additionally calculate the number
      of cuts beforehand, which adds complexity with no major benefit.
    """
    add = chn + '_' + var + '_' + trig
    prefix = 'ClosureEff_' if not isprofile else 'ClosureProf_'
    n = prefix + '_' + add + subtag
    n += _placeholder_cuts
    return n

#@setPureInputNamespace
def runClosure_outputs(outdir, channel, variables, triggers, subtag):
  outputs = [[] for _ in range(len(_extensions[:-1]))]
  outdict = {} #redundant but convenient
  
  for var in variables:
      outdict[var] = {}
      for trig in triggers:
          outdict[var][trig] = {}
          eff_name = _get_plot_name(channel, var, trig, subtag, isprofile=False)
          prof_name = _get_plot_name(channel, var, trig, subtag, isprofile=True)
          thisbase = os.path.join(outdir, ch, var, '')
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

def runClosure(indir, outdir,
               channel,
               variables,
               triggers,
               subtag,
               debug):
    ROOT.gStyle.SetOptStat(0)
    ROOT.gStyle.SetOptTitle(0)

    _, outs = runClosure_outputs(outdir, channel, variables, triggers, subtag)
  
    for ivar,var in enumerate(_variables_unionweights):
        for itrig,trig in enumerate(triggers):
            eff_names  = [ outs[var][trig][x]['eff']  for x in extensions[:-1] ]
            prof_names = [ outs[var][trig][x]['prof'] for x in extensions[:-1] ]

            drawSingleEff( indir, channel, var, trig,
                           eff_names, prof_names,
                           subtag, debug)

parser = argparse.ArgumentParser(description='Draw trigger scale factors')

parser.add_argument('--indir_ref', required=True,
                    help='Input directory for data reference efficiencies')
parser.add_argument('--indir_union', required=True,
                    help='Input directory for MC corrected efficiencies')
parser.add_argument('--outdir', help='Output directory', required=True, )
parser.add_argument('--channel', dest='channel', required=True,
                    help='Select the channels over which the workflow will be run.' )
parser.add_argument('--variables', dest='variables', required=True,
                    help='Select the variables over which the workflow will be run.' )
parser.add_argument('--triggers', dest='triggers', required=True,
                    help='Select the triggers over which the workflow will be run.' )
parser.add_argument('--subtag', dest='subtag', required=True, help='subtag')
parser.add_argument('--debug', action='store_true', help='debug verbosity')
args = parser.parse_args()

def runClosure(args.indir, args.outdir,
               args.channel, args.variables, args.triggers,
               args.subtag, args.debug):

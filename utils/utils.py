
import os
import re
import operator
import argparse
import functools
import itertools as it
import numpy as np
import h5py
from types import SimpleNamespace

import ROOT
from ROOT import TLine

from luigi_conf import (
  _placeholder_cuts,
  _sel,
  _triggers_map,
  _triggers_custom,
)
  
def addSlash(s):
  """Adds single slash to path if absent"""
  s = s if s[-1] == '/' else s + '/'
  return s

def checkBit(number, bitpos):
    bitdigit = 1
    res = bool(number&(bitdigit<<bitpos))
    return res
            
def createSingleDir(p):
  """Creates a directory if it does not exist"""
  try:
    if not os.path.exists(p): 
      os.makedirs(p)
  except PermissionError:
    m = ( "You tried to create folder {}. Make sure you have the rights!"
          "Are you sure the path is correct?".format(p) )
    print(m)
    raise
    
def createSingleFile(f):
  """Creates a dummy file if it does not exist"""
  try:
    os.remove(f)
  except OSError as e:
    if e.errno != errno.ENOENT: # errno.ENOENT = no such file or directory
      raise
  with open(f, 'x') as newf:
    newf.write('[utils] Dummy text.')

def debug(message, flag=True):
  decorator = ' ============ '
  if flag:
    print( decorator + message + decorator )

class dotDict(dict):
  """dot.notation access to dictionary attributes"""
  __getattr__ = dict.get
  __setattr__ = dict.__setitem__
  __delattr__ = dict.__delitem__

def generateTriggerCombinations(trigs):
    """Set all possible trigger combinations of intersections with any number of elements"""
    return list( it.chain.from_iterable(it.combinations(trigs, x)
                                        for x in range(1,len(trigs)+1)) )

def getKeyList(afile, inherits=['TH1']):
  tmp = []
  keylist = ROOT.TIter(afile.GetListOfKeys())
  for key in keylist:
    cl = ROOT.gROOT.GetClass(key.GetClassName())

    inherited = functools.reduce( lambda x, y: x or y,
                                  [ cl.InheritsFrom(x) for x in inherits ] )
    if not inherited:
      continue
    
    h = key.ReadObj()
    tmp.append( h.GetName() )
  return tmp

def getHistoNames(opt):
  if opt == 'Ref1D':
    return lambda a,b : 'Ref_{}_{}'.format(a,b)
  elif opt == 'Trig1D':
    return lambda a,b,c : 'Trig_{}_{}_{}{}'.format(a,b,c,_placeholder_cuts)
  else:
    import inspect
    currentFunction = inspect.getframeinfo(frame).function
    raise ValueError('[{}] option not supported.'.format(currentFunction))

def getROOTInputFiles(proc, args):
    #### Check input folder
    inputfiles = [ os.path.join(idir, proc + '/goodfiles.txt') for idir in args.indir ]
    fexists = [ os.path.exists( inpf ) for inpf in inputfiles ]
    if sum(fexists) != 1: #check one and only one is True
        raise ValueError('The process {} could be found.'.format(proc))
    inputdir = args.indir[ fexists.index(True) ] #this is the only correct input directory

    inputfiles = os.path.join(inputdir, proc + '/goodfiles.txt')

    #### Parse input list
    filelist=[]
    with open(inputfiles) as fIn:
        for line in fIn:
            if '.root' in line:
                filelist.append(line)

    return filelist, inputdir

def getROOTObject(name, afile):
  _keys = afile.GetListOfKeys()
  if name not in _keys:
    msg =  'Wrong ROOT object name!\n'
    msg += 'File name: {}\n'.format(afile.GetName())
    msg += 'Object name: {}\n'.format(name)
    msg += 'Keys: {}\n'.format([n.GetName() for n in _keys])
    raise ValueError(msg)
  return afile.Get(name)

def getTriggerBit(trigger_name, isData):
  """
  Returns the trigger bit corresponding to '_triggers_map'
  """
  s = 'data' if isData else 'mc'
  return _triggers_map[trigger_name][s]

def isChannelConsistent(chn, pairtype):
  opdict = { '<':  operator.lt,
             '>':  operator.gt,
             '==': operator.eq }

  op, val = _sel[chn]['pairType']
  return opdict[op](pairtype, val)
  
def joinNameTriggerIntersection(tuple_element):
    inters = '_PLUS_'
    return inters.join(tuple_element)

class LeafManager():
    """
    Class to manage TTree branch leafs, making sure they exist.
    """
    def __init__(self, fname, t_in):
        self.fname = fname
        self.tree = t_in
        self.absent_leaves = set()
        self.error_prefix = '[LeafManager]: '
        
    def getLeaf(self, leaf):
        if not isinstance(leaf, str):
            m = 'The leaf must be a string.'
            raise TypeError(self.error_prefix + m)
        try:
            obj = self.tree.GetListOfBranches().FindObject(leaf)
            name = obj.GetName()
            getAttr = lambda x : getattr(self.tree, x)
            return getAttr(leaf)
        except ReferenceError:
            if leaf not in self.absent_leaves:
                m = 'WARNING: leaf ' + leaf + ' does not exist in file ' + self.fname
                print(self.error_prefix + m)
                self.absent_leaves.add(leaf)
            return 0.
          
def loadBinning(afile, key, variables, channels):
  """
  Load the Binning stored in a previous task.
  """
  binedges, nbins = ({} for _ in range(2))
  with h5py.File(afile, 'r') as f:
    try:
      group = f[key]
    except KeyError:
      print('{} does not have key {}.'.format(afile, key))
      print('Available keys: {}'.format(f.keys()))
      raise
          
    for var in variables:
      subgroup = group[var]
      binedges[var], nbins[var] = ({} for _ in range(2))
      for chn in channels:
        binedges[var][chn] = np.array(subgroup[chn][:])
        nbins[var][chn] = len(binedges[var][chn]) - 1

  return binedges, nbins

def redrawBorder():
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

def remove(f):
  if os.path.exists( f ):
    os.remove( f )

def rewriteCutString(oldstr, newstr, regex=False):
  if regex:
    _regex = re.findall(r'^.*CUTS_(.+)$', newstr)
    print(_regex)
    assert(len(_regex)==1)
    _regex = _regex[0]
    newstr = _regex.replace('>', 'L').replace('<', 'S').replace('.', 'p')
    
  res = oldstr.replace(_placeholder_cuts, '_CUTS_'+newstr)
  return res

def restoreBinning(afile, channels, variables, subtag):
  """Restore the binning saved by the worflow"""
  binedges, nbins = ({} for _ in range(2))
  with h5py.File(afile, 'r') as f:
    group = f[subtag]
    for var in variables:
      subgroup = group[var]
      binedges[var], nbins[var] = ({} for _ in range(2))
      for chn in channels:
        binedges[var][chn] = subgroup[chn][:]
        nbins[var][chn] = len(binedges[var][chn]) - 1
  return binedges, nbins

def setPureInputNamespace(func):
  """
  Decorator which forces the input namespace to be a "bare" one.
  Used when luigi calls a function with a luigi.DictParameter().
  'param' can be used to pass additional arguments to the function being decorated.
  """
  def wrapper(args, param=None):
    if not isinstance(args, (argparse.Namespace, SimpleNamespace)):
      args = SimpleNamespace(**args)
    if param:
      return func(args, param)
    else:
      return func(args)

  return wrapper

def setCustomTriggerBit(trigger, trigBit, run, isData):
  """
  The VBF trigger was updated during data taking, adding HPS
  https://twiki.cern.ch/twiki/bin/viewauth/CMS/TauTrigger
  """
  if trigger not in _triggers_custom:
    import inspect
    currentFunction = inspect.getframeinfo(frame).function
    raise ValueError('[{}] option not supported.'.format(currentFunction))

  if run < 317509 and isData:
    if trigger == 'VBFTauCustom':
      bits = checkBit(trigBit, _triggers_map[trigger]['VBFTau']['data'])
    elif trigger == 'IsoTauCustom':
      bits = ( checkBit(trigBit, _triggers_map[trigger]['IsoTau']['data'][0]) or
               checkBit(trigBit, _triggers_map[trigger]['IsoTau']['data'][1]) or
               checkBit(trigBit, _triggers_map[trigger]['IsoTau']['data'][2]) )
    elif trigger == 'IsoMuIsoTauCustom':
      bits = checkBit(trigBit, _triggers_map[trigger]['IsoMuIsoTau']['data'])
    elif trigger == 'EleIsoTauCustom':
      bits = checkBit(trigBit, _triggers_map[trigger]['EleIsoTau']['data'])

  else:
    s = 'data' if isData else 'mc'
    if trigger == 'VBFTauCustom':
      bits = checkBit(trigBit, _triggers_map[trigger]['VBFTauHPS'][s])
    elif trigger == 'IsoTauCustom':
      bits = checkBit(trigBit, _triggers_map[trigger]['IsoTauHPS'][s])
    elif trigger == 'IsoMuIsoTauCustom':
      bits = checkBit(trigBit, _triggers_map[trigger]['IsoMuIsoTauHPS'][s])
    elif trigger == 'EleIsoTauCustom':
      bits = checkBit(trigBit, _triggers_map[trigger]['EleIsoTauHPS'][s])

  return bits

def slashToUnderscoreAndKeep(s, n=4):
  """Replaces slashes by underscores, keeping only the last 'n' slash-separated strings"""
  return '_'.join( s.split('/')[-n:] )

def upify(s):
  """capitalizes the first letter of the passed string"""
  return s[0].upper() + s[1:]

def writeDummyFile(f):
  try:
    with open(fname, 'x') as f:
      f.write('Dummy text.')
  except FileExistsError:
    pass

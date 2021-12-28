import os
import argparse
from types import SimpleNamespace

import ROOT
from ROOT import TLine

from luigi_conf import _triggers_map
  
def add_slash(s):
  """Adds single slash to path if absent"""
  s = s if s[-1] == '/' else s + '/'
  return s

class dotDict(dict):
    """dot.notation access to dictionary attributes"""
    __getattr__ = dict.get
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__
            
def create_single_dir(p):
  """Creates a directory if it does not exist"""
  try:
    if not os.path.exists(p): 
      os.makedirs(p)
  except PermissionError:
    m = ( "You tried to create folder {}. Make sure you have the rights!"
          "Are you sure the path is correct?".format(p) )
    print(m)
    raise
    
def create_single_file(f):
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

def slash_to_underscore_and_keep(s, n=4):
  """Replaces slashes by underscores, keeping only the last 'n' slash-separated strings"""
  return '_'.join( s.split('/')[-n:] )

def upify(s):
  """capitalizes the first letter of the passed string"""
  return s[0].upper() + s[1:]

def write_dummy_file(f):
  try:
    with open(fname, 'x') as f:
      f.write('Dummy text.')
  except FileExistsError:
    pass

def set_pure_input_namespace(func):
  """
  Decorator which forces the input namespace to be a "bare" one.
  Used when luigi calls a function with a luigi.DictParameter().
  It can however be made more ganeral.
  """
  def wrapper(args):
    if not isinstance(args, (argparse.Namespace, SimpleNamespace)):
      args = SimpleNamespace(**args)
    return func(args)

  return wrapper



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

def check_bit(number, bitpos):
    bitdigit = 1
    res = bool(number&(bitdigit<<bitpos))
    return res
            
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

def find_bin(edges, value, var):
    """
    Find the bin id corresponding to one value, given the bin edges.
    Bin 0 stands for underflow.
    """
    binid = np.digitize(value, edges)
    if binid == len(edges):
        binid -= 1 # include overflow

    # check for out-of-bounds
    if binid==0:
        print(binid, value, var)
        print(edges, len(edges))
        print( '[{}] WARNING: This should never happen in production code. '
               'Are you sure the binning is well defined?'.format(os.path.basename(__file__)) )
        binid = 1
    elif binid>len(edges):
        raise ValueError('[{}] WARNING: This cannot happen in production code.')

    return binid

def generate_trigger_combinations(trigs):
    """Set all possible trigger combinations of intersections with any number of elements"""
    complete_list = list( it.chain.from_iterable(it.combinations(trigs, x)
                                                 for x in range(1,len(trigs)+1)) )
    length1_list = list( it.chain.from_iterable(it.combinations(trigs, 1)) )
    for elem in length1_list:
        if elem not in _triggers_map.keys():
            raise ValueError('[utils.generate_trigger_combinations] Trigger {} is not supported'.format(elem))

    # remove combinations that are necessarily orthogonal to save computating time and reduce number of plots
    trigger_combinations_to_remove = []
    for elem in complete_list:        
        if ( # mixing muon and electron channels
                ('IsoMu24' in elem and 'Ele32' in elem) or
                ('IsoMu27' in elem and 'Ele32' in elem) or
                ('IsoMu24' in elem and 'Ele35' in elem) or
                ('IsoMu27' in elem and 'Ele35' in elem) or
                # mixing muon and double tau channels
                ('IsoMu24' in elem and 'IsoDoubleTauCustom' in elem) or
                ('IsoMu27' in elem and 'IsoDoubleTauCustom' in elem) or
                # mixing electron and double tau channels
                ('Ele32' in elem and 'IsoDoubleTauCustom' in elem) or
                ('Ele35' in elem and 'IsoDoubleTauCustom' in elem)
        ):
            trigger_combinations_to_remove.append( elem )


    return list(set(complete_list) - set(trigger_combinations_to_remove))

def get_key_list(afile, inherits=['TH1']):
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

def get_histo_names(opt):
    if opt == 'Ref1D':
        return lambda a,b : 'Ref_{}_{}'.format(a,b)
    elif opt == 'Trig1D':
        return lambda a,b,c : 'Trig_{}_{}_{}{}'.format(a,b,c,_placeholder_cuts)
    elif opt == 'Closure':
        return lambda a,b,c,d : 'Closure{}_{}_{}_{}'.format(a,b,c,d)
    else:
        import inspect
        currentFunction = inspect.getframeinfo(frame).function
        raise ValueError('[{}] option not supported.'.format(currentFunction))

def get_root_input_files(proc, indir):
    #### Check input folder
    inputfiles = [ os.path.join(idir, proc + '/goodfiles.txt') for idir in indir ]
    fexists = [ os.path.exists( inpf ) for inpf in inputfiles ]
    if sum(fexists) != 1: #check one and only one is True
        raise ValueError('The process {} could be found.'.format(proc))
    inputdir = indir[ fexists.index(True) ] #this is the only correct input directory

    inputfiles = os.path.join(inputdir, proc + '/goodfiles.txt')

    #### Parse input list
    filelist=[]
    with open(inputfiles) as fIn:
        for line in fIn:
            if '.root' in line:
                filelist.append(line)

    return filelist, inputdir

def get_root_object(name, afile):
    try:
        _keys = afile.GetListOfKeys()
    except ReferenceError:
        print('File {} does not exist!'.format(afile))
        raise
    if name not in _keys:
        msg =  'Wrong ROOT object name!\n'
        msg += 'File name: {}\n'.format(afile.GetName())
        msg += 'Object name: {}\n'.format(name)
        msg += 'Keys: {}\n'.format([n.GetName() for n in _keys])
        raise ValueError(msg)
    return afile.Get(name)

def get_trigger_bit(trigger_name, isdata):
    """
    Returns the trigger bit corresponding to '_triggers_map'
    """
    s = 'data' if isdata else 'mc'
    res = _triggers_map[trigger_name]
    try:
        res = res[s]
    except KeyError:
        print('You likely forgot to add your custom trigger to _triggers_custom.')
        raise
    return res

def is_channel_consistent(chn, pairtype):
    opdict = { '<':  operator.lt,
               '>':  operator.gt,
               '==': operator.eq }

    op, val = _sel[chn]['pairType']
    return opdict[op](pairtype, val)
  
def join_name_trigger_intersection(tuple_element):
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

def load_binning(afile, key, variables, channels):
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

def pass_selection_cuts(leaf_manager, invert_mass_cut=True):
    """
    Applies selection cut per TTree entry.
    Returns `True` only if all selection is passed.
    """
    mhh = leaf_manager.getLeaf( 'HHKin_mass' )
    if mhh<1:
        return False

    pairtype = leaf_manager.getLeaf( 'pairType' )
    dau1_eleiso = leaf_manager.getLeaf( 'dau1_eleMVAiso'    )
    dau1_muiso  = leaf_manager.getLeaf( 'dau1_iso'          )
    dau1_tauiso = leaf_manager.getLeaf( 'dau1_deepTauVsJet' )
    dau2_tauiso = leaf_manager.getLeaf( 'dau2_deepTauVsJet' )

    # Loose / Medium / Tight
    bool0 = pairtype==0 and (dau1_muiso>=0.15 or dau2_tauiso<5)
    bool1 = pairtype==1 and (dau1_eleiso!=1 or dau2_tauiso<5)
    bool2 = pairtype==2 and (dau1_tauiso<5 or dau2_tauiso<5)
    if bool0 or bool1 or bool2:
        return False

    #((tauH_SVFIT_mass-116.)*(tauH_SVFIT_mass-116.))/(35.*35.) + ((bH_mass_raw-111.)*(bH_mass_raw-111.))/(45.*45.) <  1.0
    svfit_mass = leaf_manager.getLeaf('tauH_SVFIT_mass')
    bh_mass    = leaf_manager.getLeaf('bH_mass_raw')

    mcut = ( (svfit_mass-129.)*(svfit_mass-129.) / (53.*53.) +
             (bh_mass-169.)*(bh_mass-169.) / (145.*145.) ) <  1.0
    if mcut and invert_mass_cut: # inverted elliptical mass cut (-> ttCR)
        return False

    #pass_met = leaf_manager.getLeaf('isMETtrigger')
    #pass_tau = leaf_manager.getLeaf('isSingleTautrigger')
    #pass_taumet = leaf_manager.getLeaf('isTauMETtrigger')
    pass_lep = leaf_manager.getLeaf('isLeptrigger')
    if not pass_lep:
        return False

    return True

def redraw_border():
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
        assert(len(_regex)==1)
        _regex = _regex[0]
        newstr = _regex.replace('>', 'L').replace('<', 'S').replace('.', 'p')
    
    res = oldstr.replace(_placeholder_cuts, '_CUTS_'+newstr)
    return res

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

def set_custom_trigger_bit(trigger, trigBit, run, isData):
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
            bits = check_bit(trigBit, _triggers_map[trigger]['VBFTau']['data'])
        elif trigger == 'IsoDoubleTauCustom':
            bits = ( check_bit(trigBit, _triggers_map[trigger]['IsoDoubleTau']['data'][0]) or
                     check_bit(trigBit, _triggers_map[trigger]['IsoDoubleTau']['data'][1]) or
                     check_bit(trigBit, _triggers_map[trigger]['IsoDoubleTau']['data'][2]) )
        elif trigger == 'IsoMuIsoTauCustom':
            bits = check_bit(trigBit, _triggers_map[trigger]['IsoMuIsoTau']['data'])
        elif trigger == 'EleIsoTauCustom':
            bits = check_bit(trigBit, _triggers_map[trigger]['EleIsoTau']['data'])

    else:
        s = 'data' if isData else 'mc'
        if trigger == 'VBFTauCustom':
            bits = check_bit(trigBit, _triggers_map[trigger]['VBFTauHPS'][s])
        elif trigger == 'IsoDoubleTauCustom':
            bits = check_bit(trigBit, _triggers_map[trigger]['IsoDoubleTauHPS'][s])
        elif trigger == 'IsoMuIsoTauCustom':
            bits = check_bit(trigBit, _triggers_map[trigger]['IsoMuIsoTauHPS'][s])
        elif trigger == 'EleIsoTauCustom':
            bits = check_bit(trigBit, _triggers_map[trigger]['EleIsoTauHPS'][s])

    return bits

def pass_any_trigger(trigs, bit, run, isdata):
    # checks that at least one trigger was fired
    flag = False
    for trig in trigs:
        if trig in _triggers_custom:
            flag = set_custom_trigger_bit(trig, bit, run, isdata)
        else:
            flag = check_bit(bit, get_trigger_bit(trig, isdata))
        if flag:
            return True
    return False

def pass_trigger_bits(trig, trig_bit, run, isdata):
    if trig in _triggers_custom:
        return set_custom_trigger_bit(trig, trig_bit, run, isdata)
    else:
        return check_bit(trig_bit, get_trigger_bit(trig, isdata))

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

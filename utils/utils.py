import os
import glob
import re
import operator
import argparse
import functools
import itertools as it
import numpy as np
import h5py
from types import SimpleNamespace

import ROOT
from ROOT import (
    TGraphAsymmErrors,
    TLine,
)

from luigi_conf import (
    _placeholder_cuts,
)
  
def addSlash(s):
    """Adds single slash to path if absent"""
    s = s if s[-1] == '/' else s + '/'
    return s

def build_prog_path(base, script_name):
    script = os.path.join(base, 'scripts')
    script = os.path.join(script, script_name)
    return 'python3 {}'.format(script)
                
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
    except FileExistsError:
        pass
    
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

def remove(f):
    if os.path.exists( f ):
        os.remove( f )

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

def print_configuration(parse_args):
    print('----------------------------------------')
    print('Script configuration:')
    options = vars(parse_args)
    maxlkey = max(len(x) for x in options.keys())
    for k,v in options.items():
        k = '--' + k
        if isinstance(v, (tuple,list)):
            v = ' '.join(v)
        print('{0:>{d1}}   {1}'.format(k, v, d1=maxlkey+3))
    print('----------------------------------------')

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

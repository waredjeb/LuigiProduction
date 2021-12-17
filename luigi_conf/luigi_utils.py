import os
import time
import luigi
import re
import functools
import inspect
from luigi.util import inherits

class ForceParameter(luigi.Task):
    force = luigi.BoolParameter(significant=False, default=False)

@inherits(ForceParameter)
class ForceableEnsureRecentTarget(luigi.Task):
    def __init__(self, *args, **kwargs):
        """Force the task to run by deleting its targets if force=True"""
        super().__init__(*args, **kwargs)
        if self.force:
            outputs = luigi.task.flatten(self.output())
            for out in outputs:
                if out.exists():
                    os.remove(out.path)

    def complete(self):
        """Flag this task as incomplete if any requirement is incomplete or has been updated more recently than this task."""
        def to_list(obj):
            if type(obj) in (list, tuple): return obj
            else: return [obj]
            
        def mtime(path):
            return os.path.getmtime(path)

        outputs = to_list(self.output())
        if len(outputs)!=0 and not isinstance(outputs[0], luigi.local_target.LocalTarget):
            raise TypeError("The targets must be luigi.LocalTarget's!")
        
        for out in outputs:
            if not os.path.exists(out.path):
                print("[luigi_utils] FORCEABLE_ENSURE_RECENT_TARGET: Marked as incomplete because file '" + out.path + "' was not found.")
                return False

        self_mtime = min(mtime(out.path) for out in outputs)

        # the below assumes a list of requirements, each with a list of outputs
        for el in to_list(self.requires()):
            #if not el.complete(): return False
            for output in to_list(el.output()):
                if mtime(output.path) > self_mtime:
                    print('[luigi_utils] FORCEABLE_ENSURE_RECENT_TARGET: target ' + output.path 
                          + ' was produced after at least one target of its parent task.')
                    return False

        return True

class WorkflowDebugger():
    """Implementation of a decorator with arguments for debugging."""
    #Note: the same can be achieved with a triply-nested function,
    #but in that case the return type cannot be the one from the 
    #original method, which is required when using luigi.
    
    def __init__(self, flag=False):
        """If there are decorator arguments, the function
        to be decorated is not passed to the constructor!"""
        self.flag = flag

    def __call__(self, func):
        """If there are decorator arguments, __call__() is only called
        once, as part of the decoration process! You can only give
        it a single argument, which is the function object."""
        def wrapper(*args, **kwargs):
            if self.flag:
                stack = inspect.stack()
                c = stack[1][0].f_locals['self'].__class__.__name__
                m = stack[1][0].f_code.co_name
                s = 'Class: {}, Method: {}, Internal luigi method: {}'.format(c, func.__name__, m)
                print('=========Workflow Debugger========== ' + s)
            return func(*args, **kwargs)
        return wrapper

def is_force_mistake(f):
    """
    The parameter '--force' may delete a lot of data.
    This function acts as a barrier to a potential running mistake.
    Currently not being used ('boundaries' and 'alt_str' must be adapted).
    """
    def yes_no(s):
        if s == 'y': return False
        elif s == 'n': return True
        else: raise ValueError()

    def yes_no_loop_wait():
        while True:
            try:
                answer = yes_no( input() )
                return answer
            except ValueError:
                print("Please type either 'y' or 'n'.")
                pass

    base_str = 'Are you sure you want to delete the '
    boundaries = (3, 4)
    alt_str = ('<fill one>', '<fill two>')
    for b,s in zip(boundaries,alt_str):
        if f > b:
            print(base_str + s + ' [y/n] ')
            if yes_no_loop_wait():
                return True

    return False

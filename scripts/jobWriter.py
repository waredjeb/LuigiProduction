import os

class JobWriter:
    """
    Help writing shell and condor job files.
    Writes one file at a time.
    """
    def __init__(self):
        self.filenames = []
        self.exts = ('sh', 'condor')
        self.endl = '\n'

    def write_init(self, filename, *args, **kwargs):
        self.filenames.append( filename )
        with open(filename, 'w') as self.f:
            extension = filename.split('.')[-1]
            self.launch_write(extension, *args, **kwargs)
        os.system('chmod u+rwx '+ filename)

    def write_queue(self, qvars=(), qlines=[]):
        """
        Works for any number variables in the queue.
        It is up to the user to guarantee compatibility between queue variables and lines.
        """
        extension = self.filenames[-1].split('.')[-1]
        if extension != self.exts[1]:
            self.extension_exception()
            
        with open(self.filenames[-1], 'a') as self.f:
            if len(qvars) > 0:
                argstr = 'Arguments = '
                for qvar in qvars:
                    argstr += '$(' + qvar + ') '
                argstr += self.endl
                self.f.write(argstr)
                qstr = 'queue '
                for qvar in qvars:
                    qstr += ('' if qvar == qvars[0] else ',') + qvar
                qstr += ' from ('
                self.f.write(qstr + self.endl)
                for ql in qlines:
                    self.f.write(ql + self.endl)
                self.f.write(')' + self.endl)
            else:
                self.f.write('queue' + self.endl)

    def extension_exception(self):
        raise ValueError('Wrong extension.')
    
    def launch_write(self, extension, *args, **kwargs):
        if extension == self.exts[0]:
            self.write_shell_(*args, **kwargs)
        elif extension == self.exts[1]:
            self.write_condor_(*args, **kwargs)
        else:
            self.extension_exception()
    
    def check_mode_(self, mode):
        if mode not in self.exts:
            raise ValueError('The mode {} is not supported.'.format(mode))

    def write_shell_(self, command, localdir):
        m = ( '#!/bin/bash' +
              self.endl + 'export X509_USER_PROXY=~/.t3/proxy.cert' +
              self.endl + 'export EXTRA_CLING_ARGS=-O2' +
              self.endl + 'source /cvmfs/cms.cern.ch/cmsset_default.sh' +
              self.endl + 'cd {}/'.format(localdir) +
              self.endl + 'eval `scramv1 runtime -sh`' +
              self.endl + command +
              self.endl )
        self.f.write(m)

    def llr_condor_specific_content(self, queue):
        m = ( self.endl + 'T3Queue = {}'.format(queue) +
              self.endl + 'WNTag=el7' +
              self.endl + '+SingularityCmd = ""' +
              self.endl + 'include : /opt/exp_soft/cms/t3_tst/t3queue |' +
              self.endl )
        return m

    def write_condor_(self, executable, outfile, queue):
        m = ( 'Universe = vanilla' +
              self.endl + 'Executable = {}'.format(executable) +
              self.endl + 'input = /dev/null' +
              self.endl + 'output = {}'.format(outfile) +
              self.endl + 'error  = {}'.format(outfile.replace('.o', '.e')) +
              self.endl + 'getenv = true' +
              self.llr_condor_specific_content(queue) +
             self.endl )
        self.f.write(m)

    def add_string(self, string):
        with open(self.filenames[-1], 'a') as self.f:
            self.f.write(string + self.endl)

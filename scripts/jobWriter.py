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

    def write_shell_(self, command, localdir, cmsswdir = ""):
        m = ( '#!/bin/bash' +
              self.endl + 'source /cvmfs/cms.cern.ch/cmsset_default.sh' +
              self.endl + "tar -xzvf " + cmsswdir + ".tgz" +
            #   self.endl + "rm " + cmsswdir + ".tgz" + 
              self.endl + "cd " + cmsswdir + "/src/" +
              self.endl + 'eval `scramv1 runtime -sh`' +
              self.endl + "echo $CMSSW_BASE is the CMSSW we have on the local worker node" +
              self.endl + "cd ../../" + 
              self.endl + "date" +
              self.endl + command +
              self.endl + "date" + 
              self.endl )
        self.f.write(m)

    def llr_condor_specific_content(self, queue):
        m = ( self.endl + 'queue 1' +
              self.endl )
        return m

    def write_condor_(self, executable, transfer_files, output_step_file, outfile, outpath, queue):
        transfer_file_string_list = [f + ", " for f in transfer_files]
        transfer_file_string = ""
        for s in transfer_file_string_list:
            transfer_file_string += s
        transfer_file_string = transfer_file_string[:-2]
        if(transfer_files != ""):
            do_transfer_files = "should_transfer_files = YES" + self.endl + "transfer_input_files = " + transfer_file_string
            do_transfer_output_remaps = "transfer_output_remaps = \"" + output_step_file + " = " + outpath + outfile.replace(".root", "") + "$(proc)_$(Cluster).root\""
        else:
            do_transfer_files = ""
            do_transfer_output_remaps = ""

        m = ( 'Universe = vanilla' +
              self.endl + 'Executable = {}'.format(executable) +
              self.endl + do_transfer_files + 
              self.endl + do_transfer_output_remaps + 
              self.endl + 'error  = {}'.format(outfile.replace('.o', '.e')) +
              self.endl + 'getenv = true' +
            #   self.llr_condor_specific_content(queue) +
             self.endl )
        self.f.write(m)

    def add_string(self, string):
        with open(self.filenames[-1], 'a') as self.f:
            self.f.write(string + self.endl)

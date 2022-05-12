import sys
sys.path.append("..")

import os
import atexit # https://stackoverflow.com/questions/865115/how-do-i-correctly-clean-up-a-python-object

from utils.utils import (
  setPureInputNamespace,
)

@setPureInputNamespace
def writeHTCondorDAGFiles_outputs(args):
    """
    Outputs are guaranteed to have the same length.
    Returns all separate paths to avoid code duplication.
    """
    outSubmDir = 'submission'
    submDir = os.path.join(args.localdir, 'jobs', args.tag, outSubmDir)
    os.system('mkdir -p {}'.format(submDir))
    # outCheckDir = 'outputs'
    # checkDir = os.path.join(args.localdir, 'jobs', args.tag, outCheckDir)
    # os.system('mkdir -p {}'.format(checkDir))

    name = 'workflow.dag'
    return os.path.join(submDir, name)

class WriteDAGManager:
    def __init__(self, localdir, tag, data_name, jobs):
        self.data_name = data_name
        
        out = writeHTCondorDAGFiles_outputs( {'localdir': localdir, 'tag': tag} )
        self.this_file = open(out, 'w')
        atexit.register(self.cleanup)
        
        self.rem_ext = lambda x : os.path.basename(x).split('.')[0]

        self.write_configuration()

        self.jobs = jobs
        self.define_all_job_names(self.jobs)

    def cleanup(self):
        self.this_file.close()
            
    def write_string(self, string):
        self.this_file.write(string)
            
    def new_line(self):
        self.this_file.write('\n')

    def define_all_job_names(self, jobs):
        for _,values in jobs.items():
            print(f"Values {values}")
            self.define_job_names(values)
        
    def define_job_names(self, jobs):
        """First step to build a DAG"""
        for job in jobs:
            self.write_string('JOB {} {}\n'.format(self.rem_ext(job), job))
        self.new_line()

    def write_parent_child_hierarchy(self, parents, childs):
        if not isinstance(parents, (list,tuple)):
            m = '[writeHTCondorDAGFiles] Please pass lists to the '
            m += ' `write_parent_child_hierarchy method.'
            raise TypeError(m)
        
        self.write_string('PARENT ')
        for par in parents:
            self.write_string('{} '.format(self.rem_ext(par)))
        self.write_string('CHILD ')
        for cld in childs:
            self.write_string('{} '.format(self.rem_ext(cld)))
        self.new_line()

    def write_configuration(self):
        # https://research.cs.wisc.edu/htcondor/manual/v7.6/2_10DAGMan_Applications.html#SECTION003109000000000000000
        # dot -Tps dag.dot -o dag.ps
        self.this_file.write('DOT dag.dot\n')
        #self.this_file.write('DAGMAN_HOLD_CLAIM_TIME=30\n')
        self.new_line()

    def write_all(self):
        # histos to hadd for data
        self.write_parent_child_hierarchy( parents=[x for x in self.jobs['jobsStep1'] if self.data_name in os.path.basename(x)],
                                            childs=[self.jobs["jobsStep2"]])
        self.new_line()
        

        # hadd aggregation for Data




# condor_submit_dag -no_submit diamond.dag
# condor_submit diamond.dag.condor.sub
# https://htcondor.readthedocs.io/en/latest/users-manual/dagman-workflows.html#optimization-of-submission-time


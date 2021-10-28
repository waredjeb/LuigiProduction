#!/usr/bin/env python

import os,sys
import optparse
import fileinput
import time
import glob
import subprocess
from os.path import basename
import ROOT


def isGoodFile (fileName) :
	ff = ROOT.TFile (fname)
	if ff.IsZombie() : return False
	if ff.TestBit(ROOT.TFile.kRecovered) : return False
	return True


# ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

def parseInputFileList (fileName) :
	filelist = []
	with open (fileName) as fIn:
		for line in fIn:
			line = (line.split("#")[0]).strip()
			if line:
				filelist.append(line)
		return filelist


# ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

if __name__ == "__main__":

	# -- Parse options
	parser = optparse.OptionParser('')
	parser.add_option('-a',  '--indir',   dest='indir',   help='in directory')
	parser.add_option('-o', '--outdir',  dest='outdir',  help='out directory')
	parser.add_option('-p',   '--proc',    dest='proc',    help='process name')
	(opt, args) = parser.parse_args()

	# -- Job command
	prog = 'python get_trigger_eff_sig.py '

	# -- Check input folder
	if not os.path.exists (opt.indir) :
		print('input folder', opt.indir, 'not existing, exiting')
		sys.exit (1)


	# -- Input list
	inputfiles = opt.indir+'/SKIM_'+opt.proc+'/goodfiles.txt'

	# -- Job steering files
	currFolder = os.getcwd ()
	jobsDir = currFolder+'/jobs/'
	n = int (0)
	commandFile = open (jobsDir + '/submit.sh', 'w')

	# -- Parse input list
	filelist=[]
	with open(inputfiles) as fIn:
		for line in fIn:
			if '.root' in line:
				filelist.append(line)

	# -- Write steering files
	for listname in filelist:

		# - Setup
		jobFile = jobsDir+'/job_'+opt.proc+'_{}.sh'.format(n)
		scriptFile = open (jobFile, 'w')
		scriptFile.write ('#!/bin/bash\n')
		scriptFile.write ('export X509_USER_PROXY=~/.t3/proxy.cert\n')
		scriptFile.write ('source /cvmfs/cms.cern.ch/cmsset_default.sh\n')
		scriptFile.write ('cd /home/llr/cms/portales/hhbbtautau/CMSSW_11_1_0_pre6/src\n')
		scriptFile.write ('eval `scram r -sh`\n')
		scriptFile.write ('cd %s\n'%currFolder)

		# - Define job command
		command = (prog
		           + ' --indir='   + opt.indir
		           + ' --outdir='  + opt.outdir
		           + ' --proc='    + opt.proc
		           + ' --file='    + listname.replace(opt.indir,'').replace('/','').replace('SKIM_','').replace(opt.proc,'')
                           )

		scriptFile.write (command)
		scriptFile.write ('echo "Done for job %d" \n'%n)
		scriptFile.close()

		# - Submit job (test)
		os.system ('chmod u+rwx '+ jobFile)
		os.system('/opt/exp_soft/cms/t3/t3submit -short ' + jobFile)
		n = n + 1

		# - Save job command
		commandFile.write (command + '\n')

	commandFile.close ()

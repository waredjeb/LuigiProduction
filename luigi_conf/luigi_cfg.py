import os
import argparse
from argparse import RawTextHelpFormatter
import luigi
from luigi.util import inherits

######################################################################## 
### ARGUMENT PARSING ###################################################
########################################################################
parser = argparse.ArgumentParser(formatter_class=RawTextHelpFormatter)
parser.add_argument(
    '--sample_name',
    type=str,
    default="SingleElectron",
    help="Number of histogram bins. If fine-grained control is required modify the variable `_bins` in the luigi configuration file."
)
parser.add_argument(
    '--sample_file',
    type=str,
    default="CE_E_Front_300um_cfi_GEN_SIM.py",
    help="Number of histogram bins. If fine-grained control is required modify the variable `_bins` in the luigi configuration file."
)
parser.add_argument(
    '--localdir',
    type=str,
    default="./",
    help="Number of histogram bins. If fine-grained control is required modify the variable `_bins` in the luigi configuration file."
)

parser.add_argument(
    '--scheduler',
    type=str,
    choices=['local', 'central'],
    default='local',
    help='Select the scheduler for luigi.'
)

parser.add_argument(
    '--tag',
    type=str,
    default='TEST',
    help='Specifies a tag to differentiate workflow runs.'
)

parser.add_argument(
    '--subtag',
    type=str,
    default='TEST',
    help='Specifies a tag to differentiate workflow runs.'
)

parser.add_argument(
    '--debug_workflow',
    action='store_true',
    help="Explicitly print the functions being run for each task, for workflow debugging purposes."
)

FLAGS, _ = parser.parse_known_args()

########################################################################
### LUIGI CONFIGURATION ################################################
########################################################################
class cfg(luigi.Config):
    # auxiliary, not used by scripts
    base_name = 'SampleProduction'
    data_base = os.path.join( './data/')
    user = os.environ['USER']
    tag = FLAGS.tag
    
    _storage = os.path.join(data_base, user, base_name, tag)
    data_storage = os.path.join(_storage, 'Data')
    out_storage = os.path.join(_storage, 'Outputs')

    local_home = os.environ['HOME']
    local_cmssw = os.path.join(os.environ['CMSSW_VERSION'], 'src')
    local_analysis_folder = 'TESTProduction'

    subtag = ( FLAGS.subtag if FLAGS.subtag==''
               else ( '_' + FLAGS.subtag if FLAGS.subtag[0] != '_' else FLAGS.subtag ) )
    local_folder = os.path.join(local_home, local_cmssw, local_analysis_folder)
    localdir_folder = os.path.join(data_base, local_analysis_folder)
    targets_folder = os.path.join(data_storage, 'targets')
    targets_default_name = 'DefaultTarget.txt'

    ####
    #### Step1
    ####


    step1_params = luigi.DictParameter(
        default={ 'indir': data_base,
                  'outdir': data_base + "output",
                  'localdir': './',
                  "sample_name" : "SingleElectron",
                  "sample_file" : "CE_E_Front_300um_cfi_GEN_SIM.py"} )

    write_params = { 'data_name': FLAGS.sample_name,
                     'localdir': localdir_folder,
                     'tag': tag }

    for k, v in write_params.items():
        print(k, v)

   
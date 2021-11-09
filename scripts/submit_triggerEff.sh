#!/usr/bin/env bash

h_description="prints this help message"
n_description="dry-run: prints the commands but does not run them"
i_description="input data directory"
o_description="output data directory"
p_description="mc / data to process"
    
function print_usage {
    usage=" $(basename $0) [-h] [-i -o -p]: 

    where:
    -h  ${h_description}
    -n  ${n_description}
    -i  ${i_description}
    -o  ${o_description}
    -p  ${p_description}

    Description: 
    submits condor jobs for trigger scale factors

    Run example: 
    bash submit_triggerEff.sh -n -i /data_CMS/cms/portales/HHresonant_SKIMS/SKIMS_Radion_2018_fixedMETtriggers_mht_16Jun2021/ -o /data_CMS/cms/alves/FRAMEWORKTEST/ -p MET2018
"
    printf "${usage}"
}

###############################################################
############### SET INDEPENDENT VARIABLES #####################
###############################################################
THISPATH="${HOME}/METTriggerStudies/scripts"
DRY_RUN=0

# list of arguments expected in the input
optstring=":hni:o:p:"
while getopts ${optstring} arg; do
  case ${arg} in
    h)
      print_usage
      exit 1;
      ;;
    n)
      DRY_RUN=1
      ;;
    i)
        i_arg="${OPTARG}"
        ;;
    o)
        o_arg="${OPTARG}"
        ;;
    p)
        p_arg="${OPTARG}"
        ;;
    :)
      echo "$0: Must supply an argument to -$OPTARG." >&2
      exit 1
      ;;
    ?)
      echo "Invalid option: -${OPTARG}."
      exit 2
      ;;
  esac
done
shift $((OPTIND-1))

###############################################################
############### CHECK UNSET ARGUMENTS #########################
###############################################################
if [ -z "${i_arg+x}" ]; then
    echo "Option -i (${i_description}) is unset";
    exit 1;
fi
if [ -z "${o_arg+x}" ]; then
    echo "Option -o (${o_description}) is unset";
    ext 1;
fi
if [ -z "${p_arg+x}" ]; then
    echo "Option -p (${p_description}) is unset";
    ext 1;
fi

###############################################################
############### SET DEPENDENT VARIABLES #######################
###############################################################
if [ "${p_arg}" == "MET2018" ]; then
    #PROC=("MET2018A" "MET2018B" "MET2018C" "MET2018D")
    PROC=("MET2018A")
elif [ "${p_arg}" == "Radions" ]; then
    PROC=("Radion_m300" "Radion_m400" "Radion_m500" "Radion_m600" "Radion_m700" "Radion_m800 Radion_m900")
fi

###############################################################
############### DEFINE SUBMISSION COMMAND #####################
###############################################################
function mycommand() {
    local options="${THISPATH}/submit_triggerEff.py --indir ${i_arg} --outdir ${o_arg} --proc ${1} --channels all etau mutau tautau mumu"
    if [ "$2" -eq 0 ]; then
	echo python3 ${options} 
    elif [ "$2" -eq 1 ]; then
	python3 ${options}
    else
	echo ":mycommand: received a wrong argument."
	return 1;
    fi
}

###############################################################
############### RUN PYTHON SCRIPT #############################
###############################################################
if [ "${DRY_RUN}" -eq 1 ]; then
    echo "---- DRY RUN ----"
fi

for proc in ${PROC[@]}
do
    if [ "${DRY_RUN}" -eq 1 ]; then
	mycommand $proc 0;
    else
	mycommand $proc 1;
    fi
done

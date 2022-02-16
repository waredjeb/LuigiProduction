#!/usr/bin/env bash

######### CONFIGURABLE PARAMETERS ################
BASE_PATH="/data_CMS/cms/alves/TriggerScaleFactors"
EOS_PATH="/eos/home-b/bfontana"
##################################################

help_description="prints this help message"
tag_description="select tag"
full_description="also removes HTCondor outputs"
debug_description="debug: prints everything, runs nothing"
function print_usage_workflowClean {
    usage=" $(basename "$0") [-h] [-t -d]: removes folders
where:
    -h / --help  [ ${help_description} ]
    -t / --tag   [ ${tag_description} ]
    -f / --full  [ ${full_description} ]
    -d / --debug [ ${debug_description} ]

    Run example: $(basename "$0") -t v1
"
    printf "${usage}"
}

DEBUG=false
FULL=false
while [[ $# -gt 0 ]]; do
    key="$1"

    case $key in
	-h|--help)
	    print_usage_workflowClean
	    exit 1
	    ;;	
	-t|--tag)
	    TAG="$2"
	    shift # past argument
	    shift # past value
	    ;;
	-f|--full)
	    FULL=true
	    shift # past argument
	    ;;
	-d|--debug)
	    DEBUG=true
	    shift # past argument
	    ;;
	*)    # unknown option
	    echo "Wrong parameter ${1}."
	    exit 1
	    ;;
    esac
done

if [[ -z "${TAG}" ]]; then
    echo "Select the tag via the '--tag' option."
    declare -a tags=( $(/bin/ls -1 "${BASE_PATH}") )
    if [ ${#tags[@]} -ne 0 ]; then
	echo "The following tags are currently available:"
	for tag in "${tags[@]}"; do
	    echo "- ${tag}"
	done
    else
	echo "No tags are currently available. Everything looks clean!"
    fi
    exit 1;
fi

echo "####### Arguments #####################"
echo "TAG   = ${TAG}"
echo "DEBUG = ${DEBUG}"
echo "FULL = ${DEBUG}"
echo "#######################################"

##### FUNCTION DEFINITION #####
declare -a COMMANDS=( "rm -rf ${EOS_PATH}/www/TriggerScaleFactors/${TAG}/"
		      "rm -rf ${BASE_PATH}/${TAG}/*root" #hadd outputs
		      "rm -rf ${BASE_PATH}/${TAG}/targets/DefaultTarget_hadd.txt"
		      "rm -rf ${BASE_PATH}/${TAG}/targets/DefaultTarget_drawsf.txt"
		      "rm -rf jobs/${TAG}/" )

if $FULL; then
    COMMANDS+=( "rm -rf ${BASE_PATH}/${TAG}/" )
fi

for comm in "${COMMANDS[@]}"; do
    if $DEBUG; then
	echo "${comm}";
    else
	${comm};
    fi
done;

if "${DEBUG}"; then
    echo "Debug mode: the files were not removed."
else
    echo "Files removed."
fi

###############################


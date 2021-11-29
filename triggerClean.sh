#!/usr/bin/env bash

help_description="prints this help message"
tag_description="select tag"
debug_description="debug: prints everything, runs nothing"
function print_usage_triggerClean {
    usage=" $(basename "$0") [-h] [-t -d]: removes folders
where:
    -h / --help  [ ${help_description} ]
    -t / --tag   [ ${tag_description} ]
    -d / --debug [ ${debug_description} ]

    Run example: $(basename "$0") -t v1
"
    printf "${usage}"
}

DEBUG=false
while [[ $# -gt 0 ]]; do
    key="$1"

    case $key in
	-h|--help)
	    print_usage_triggerClean
	    exit 1
	    ;;	
	-t|--tag)
	    TAG="$2"
	    shift # past argument
	    shift # past value
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
    exit 1;
fi
echo "TAG   = ${TAG}"
echo "DEBUG = ${DEBUG}"

##### FUNCTION DEFINITION #####
declare -a COMMANDS=( "rm -rf /eos/home-b/bfontana/www/TriggerScaleFactors/${TAG}/*"
		      "rm -rf /data_CMS/cms/alves/TriggerScaleFactors/${TAG}/*"
		      "rm -rf jobs/outputs/*"
		      "rm -rf jobs/submission/*" )

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


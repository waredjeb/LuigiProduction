
TAG="CountsTest"
# condor_submit jobs/"${TAG}"/submission/jobHistos_SKIM_MET.condor
# condor_submit jobs/"${TAG}"/submission/jobHistos_SKIM_TT_fullyLep.condor
# condor_submit jobs/"${TAG}"/submission/jobHistos_SKIM_TT_semiLep.condor
# condor_submit jobs/"${TAG}"/submission/jobHistos_SKIM_TT_fullyHad.condor

# condor_submit jobs/"${TAG}"/submission/jobCounts_SKIM_MET.condor
# condor_submit jobs/"${TAG}"/submission/jobCounts_SKIM_TT_fullyLep.condor
# condor_submit jobs/"${TAG}"/submission/jobCounts_SKIM_TT_semiLep.condor
# condor_submit jobs/"${TAG}"/submission/jobCounts_SKIM_TT_fullyHad.condor

# condor_submit jobs/"${TAG}"/submission/jobHaddHisto_MET.condor
# condor_submit jobs/"${TAG}"/submission/jobHaddHisto_TT.condor

# condor_submit jobs/"${TAG}"/submission/jobHaddCounts_MET.condor
# condor_submit jobs/"${TAG}"/submission/jobHaddCounts_TT.condor

# condor_submit jobs/"${TAG}"/submission/jobHaddHistoAgg_MET.condor
# condor_submit jobs/"${TAG}"/submission/jobHaddHistoAgg_TT.condor

condor_submit jobs/"${TAG}"/submission/jobHaddCountsAgg_MET.condor
condor_submit jobs/"${TAG}"/submission/jobHaddCountsAgg_TT.condor

# condor_submit jobs/"${TAG}"/submission/jobEfficienciesAndSF.condor

# for sub in jobs/"${TAG}"/submission/jobDiscriminator_*.condor; do
# 	condor_submit "${sub}"
# done

# for sub in jobs/"${TAG}"/submission/jobUnionWeightsCalculator_*.condor;do
# 	condor_submit "${sub}"
# done

# REMOVED condor_submit jobs/"${TAG}"/submission/jobHaddEff.condor
# REMOVED condor_submit jobs/"${TAG}"/submission/jobHaddEffAgg.condor

# condor_submit jobs/"${TAG}"/submission/jobClosure.condor

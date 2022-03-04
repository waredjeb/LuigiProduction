
# condor_submit jobs/TEST/submission/jobHistos_SKIM_MET2018.condor
# condor_submit jobs/TEST/submission/jobHistos_SKIM_TT_fullyLep.condor
# condor_submit jobs/TEST/submission/jobHistos_SKIM_TT_semiLep.condor
# condor_submit jobs/TEST/submission/jobHistos_SKIM_TT_fullyHad.condor

# condor_submit jobs/TEST/submission/jobHadd_MET2018.condor
# condor_submit jobs/TEST/submission/jobHadd_TT.condor

# condor_submit jobs/TEST/submission/jobHaddAgg_MET2018.condor
# condor_submit jobs/TEST/submission/jobHaddAgg_TT.condor

# condor_submit jobs/TEST/submission/jobEfficienciesAndSF.condor

for sub in jobs/TEST/submission/jobDiscriminator_*.condor;do
	condor_submit "${sub}"
done

# for sub in jobs/TEST/submission/jobUnionWeightsCalculator_*.condor;do
# 	condor_submit "${sub}"
# done


# condor_submit jobs/TEST/submission/jobHistos_SKIM_MET2018.condor
# condor_submit jobs/TEST/submission/jobHistos_SKIM_TT_fullyLep.condor
# condor_submit jobs/TEST/submission/jobHistos_SKIM_TT_semiLep.condor
# condor_submit jobs/TEST/submission/jobHistos_SKIM_TT_fullyHad.condor

# condor_submit jobs/TEST/submission/jobHaddHisto_MET2018.condor
# condor_submit jobs/TEST/submission/jobHaddHisto_TT.condor

# condor_submit jobs/TEST/submission/jobHaddHistoAgg_MET2018.condor
# condor_submit jobs/TEST/submission/jobHaddHistoAgg_TT.condor

# condor_submit jobs/TEST/submission/jobEfficienciesAndSF.condor

# for sub in jobs/TEST/submission/jobDiscriminator_*.condor; do
# 	condor_submit "${sub}"
# done

for sub in jobs/TEST/submission/jobUnionWeightsCalculator_*.condor;do
	condor_submit "${sub}"
done

# REMOVED condor_submit jobs/TEST/submission/jobHaddEff.condor
# REMOVED condor_submit jobs/TEST/submission/jobHaddEffAgg.condor

# condor_submit jobs/TEST/submission/jobClosure.condor

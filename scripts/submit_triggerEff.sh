#!/usr/bin/env bash

#INDIR="/data_CMS/cms/portales/HHresonant_SKIMS/SKIMS_Radion_2018_resDNN_ALLMETtrigger_test_28Apr2021/"
#INDIR="/data_CMS/cms/portales/HHresonant_SKIMS/SKIMS_Radion_2018_fixedMETtriggers_04Jun2021/"
#INDIR="/data_CMS/cms/portales/HHresonant_SKIMS/SKIMS_Radion_2018_fixedMETtriggers_mht_16Jun2021/"
INDIR="/data_CMS/cms/portales/HHresonant_SKIMS/SKIMS_Radion_2018_metTrg_tauTrg_taumetsplit_dr04_05Jul2021/"

#OUTDIR="/data_CMS/cms/portales/HHresonant_SKIMS/SKIMS_Radion_2018_resDNN_ALLMETtrigger_test_28Apr2021/output_trigEffBkg_TTCR_fixedtrig/"
#OUTDIR="/data_CMS/cms/portales/HHresonant_SKIMS/SKIMS_Radion_2018_fixedMETtriggers_mht_16Jun2021/output_trigEffBkg_TTCR_fixedtrig/"
OUTDIR="/data_CMS/cms/alves/FRAMEWORKTEST/"

#PROC="DY DY_lowMass Radion_m300 Radion_m400 Radion_m500 Radion_m600 Radion_m700 Radion_m800 Radion_m900 TT_fullyHad TT_fullyLep TT_semiLep SingleMuon2018A SingleMuon2018B SingleMuon2018C SingleMuon2018D MET2018A MET2018B MET2018C MET2018D"
#PROC="SingleMuon2018A SingleMuon2018B SingleMuon2018C SingleMuon2018D"
#PROC="MET2018A MET2018B MET2018C"
#PROC="TT_fullyHad TT_fullyLep TT_semiLep MET2018A MET2018B MET2018C MET2018D"
#PROC="MET2018C MET2018D"
PROC="Radion_m300 Radion_m400 Radion_m500 Radion_m600 Radion_m700 Radion_m800 Radion_m900"

for proc in $PROC
do
    python3 submit_triggerEff.py --indir=$INDIR --outdir=$OUTDIR --proc=$proc
done

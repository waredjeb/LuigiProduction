/***********************************************************************************************************/
Bool_t passBaseline(TTree *theTree, int ievt){
/***********************************************************************************************************/

  theTree->GetEntry(ievt);
  
  int pairType   = theTree->GetLeaf( "pairType"   )->GetValue(0);
  int nleps      = theTree->GetLeaf( "nleps"      )->GetValue(0);
  int isOS       = theTree->GetLeaf( "isOS"       )->GetValue(0);
  int nbjetscand = theTree->GetLeaf( "nbjetscand" )->GetValue(0);
  
  float dau1_eleiso = theTree->GetLeaf( "dau1_eleMVAiso"     )->GetValue(0);
  float dau1_iso    = theTree->GetLeaf( "dau1_iso"           )->GetValue(0);
  float dau1_tauiso = theTree->GetLeaf( "dau1_deepTauVsJet"  )->GetValue(0);
  float dau2_tauiso = theTree->GetLeaf( "dau2_deepTauVsJet"  )->GetValue(0);
 
  bool evtPass = true;
  if(nleps!=0 || nbjetscand<2 || isOS==0) evtPass = false;
  
  //if(pairType>2) evtPass = false;
  if(pairType!=2) evtPass = false;
  if(dau2_tauiso<5) evtPass = false;

  if(pairType==0 && dau1_iso>=0.15) evtPass = false;
  if(pairType==1 && dau1_eleiso!=1.) evtPass = false;
  if(pairType==2 && dau1_tauiso<5) evtPass = false;

  return evtPass;
}

/***********************************************************************************************************/
Bool_t passTriggerCuts(Bool_t is_VBF, Bool_t is_Lep, Bool_t is_MET, Bool_t is_SingleTau, Bool_t is_TauMET,
		       Double_t pt_dau2,Double_t MET){
/***********************************************************************************************************/

  Bool_t passTrg = false;
  if(is_Lep || is_VBF) {

    passTrg = true;

  } else {
    // may need some priority ordering here
    if(is_SingleTau) passTrg = true;
    if(is_TauMET && MET>100.) passTrg = true;
    if(is_MET && MET>200.) passTrg = true;
  }
  return passTrg;
}

/***********************************************************************************************************/
Double_t getEvtWeight(TTree *theTree, int ievt){
/***********************************************************************************************************/
  Double_t weight = 0;
  theTree->GetEntry(ievt);
  // TO DO !
  return weight;
}


/***********************************************************************************************************/
/***********************************************************************************************************/
void comp_all_trig_fast(){
/***********************************************************************************************************/
/***********************************************************************************************************/


  TFile *f = new TFile("/data_CMS/cms/portales/HHresonant_SKIMS/SKIMS_Radion_2018_metTrg_tauTrg_taumetsplit_dr04_05Jul2021/SKIM_Radion_m900.root","read");
  TTree *t = (TTree*)f->Get("HTauTauTree");

  //TH2D *h = new TH2D("h","Spin 0 - all - m_{X} = 900 GeV",7,0.5,7.5,5,0.5,5.5);
  TH2D *h = new TH2D("h","Spin 0 - #tau_{h}#tau_{h} - m_{X} = 900 GeV",7,0.5,7.5,5,0.5,5.5);

  Int_t nevt=0;

  //1D histos
  
  


  for(int i=0; i<t->GetEntries();++i)
    {
      t->GetEntry(i);
     
      int pairType   = t->GetLeaf( "pairType"   )->GetValue(0);
      int nleps      = t->GetLeaf( "nleps"      )->GetValue(0);
      int isOS       = t->GetLeaf( "isOS"       )->GetValue(0);
      int nbjetscand = t->GetLeaf( "nbjetscand" )->GetValue(0);
      
      float dau1_eleiso = t->GetLeaf( "dau1_eleMVAiso"     )->GetValue(0);
      float dau1_iso    = t->GetLeaf( "dau1_iso"           )->GetValue(0);
      float dau1_tauiso = t->GetLeaf( "dau1_deepTauVsJet"  )->GetValue(0);
      float dau2_tauiso = t->GetLeaf( "dau2_deepTauVsJet"  )->GetValue(0);

      if(nleps!=0 || nbjetscand<2 || isOS==0) continue;
  
      //if(pairType>2) continue;
      if(pairType!=2) continue;
      if(dau2_tauiso<5) continue;
      
      if(pairType==0 && dau1_iso>=0.15) continue;
      if(pairType==1 && dau1_eleiso!=1.) continue;
      if(pairType==2 && dau1_tauiso<5) continue;

      //if(!passBaseline(t,i)) continue;



   
      Bool_t is_VBF       = t->GetLeaf( "isVBFtrigger"       )->GetValue(0);
      //Bool_t is_VBF       = t->GetLeaf( "isVBF"       )->GetValue(0);
      Bool_t is_Lep       = t->GetLeaf( "isLeptrigger"       )->GetValue(0);
      Bool_t is_MET       = t->GetLeaf( "isMETtrigger"       )->GetValue(0);
      Bool_t is_SingleTau = t->GetLeaf( "isSingleTautrigger" )->GetValue(0);
      Bool_t is_TauMET    = t->GetLeaf( "isTauMETtrigger"    )->GetValue(0);
      if(!is_VBF && !is_Lep && !is_MET && !is_SingleTau && !is_TauMET) continue;

      Double_t pt_dau2 = t->GetLeaf( "dau2_pt" )->GetValue(0);
      Double_t MET     = t->GetLeaf( "met_et"  )->GetValue(0);
      if(!passTriggerCuts(is_VBF,is_Lep,is_MET,is_SingleTau,is_TauMET,pt_dau2,MET)) continue;
      //is_TauMET = is_TauMET && (MET>100.);
      //is_MET = is_MET && (MET>200.);
      
      nevt++;

      Bool_t isOnly_VBF       = is_VBF       && !( is_Lep || is_MET || is_SingleTau || is_TauMET );
      Bool_t isOnly_Lep       = is_Lep       && !( is_VBF || is_MET || is_SingleTau || is_TauMET );
      Bool_t isOnly_MET       = is_MET       && !( is_Lep || is_VBF || is_SingleTau || is_TauMET );
      Bool_t isOnly_SingleTau = is_SingleTau && !( is_Lep || is_MET || is_VBF       || is_TauMET );
      Bool_t isOnly_TauMET    = is_TauMET    && !( is_Lep || is_MET || is_SingleTau || is_VBF    );

      Bool_t is_LepOrMET = ( is_Lep || is_MET || is_VBF); 
      
      // Pass only "that" trigger
      h->Fill(1,1,isOnly_Lep);
      h->Fill(1,2,isOnly_VBF);
      h->Fill(1,3,isOnly_MET);
      h->Fill(1,4,isOnly_TauMET);
      h->Fill(1,5,isOnly_SingleTau);

      // pass Lep & MET & trigger
      h->Fill(2,1,!is_LepOrMET && is_Lep);
      h->Fill(2,2,!is_LepOrMET && is_VBF);
      h->Fill(2,3,!is_LepOrMET && is_MET);
      h->Fill(2,4,!is_LepOrMET && is_TauMET);
      h->Fill(2,5,!is_LepOrMET && is_SingleTau);

      // pass Lep & trigger
      h->Fill(3,1,is_Lep && is_Lep);
      h->Fill(3,2,is_Lep && is_VBF);
      h->Fill(3,3,is_Lep && is_MET);
      h->Fill(3,4,is_Lep && is_TauMET);
      h->Fill(3,5,is_Lep && is_SingleTau);

      // pass VBF & trigger
      h->Fill(4,1,is_VBF && is_Lep);
      h->Fill(4,2,is_VBF && is_VBF);
      h->Fill(4,3,is_VBF && is_MET);
      h->Fill(4,4,is_VBF && is_TauMET);
      h->Fill(4,5,is_VBF && is_SingleTau);

      // pass MET & trigger
      h->Fill(5,1,is_MET && is_Lep);
      h->Fill(5,2,is_MET && is_VBF);
      h->Fill(5,3,is_MET && is_MET);
      h->Fill(5,4,is_MET && is_TauMET);
      h->Fill(5,5,is_MET && is_SingleTau);

      // pass TauMET & trigger
      h->Fill(6,1,is_TauMET && is_Lep);
      h->Fill(6,2,is_TauMET && is_VBF);
      h->Fill(6,3,is_TauMET && is_MET);
      h->Fill(6,4,is_TauMET && is_TauMET);
      h->Fill(6,5,is_TauMET && is_SingleTau);

      // pass SingleTau & trigger
      h->Fill(7,1,is_SingleTau && is_Lep);
      h->Fill(7,2,is_SingleTau && is_VBF);
      h->Fill(7,3,is_SingleTau && is_MET);
      h->Fill(7,4,is_SingleTau && is_TauMET);
      h->Fill(7,5,is_SingleTau && is_SingleTau);


    }

  h->GetXaxis()->SetBinLabel(1,"none");
  h->GetXaxis()->SetBinLabel(2,"notLEPMETVBF");
  h->GetXaxis()->SetBinLabel(3,"isLEP");
  h->GetXaxis()->SetBinLabel(4,"isVBF");
  h->GetXaxis()->SetBinLabel(5,"isMET");
  h->GetXaxis()->SetBinLabel(6,"isTauMET");
  h->GetXaxis()->SetBinLabel(7,"isSingleTau");

  h->GetYaxis()->SetBinLabel(1,"isLEP");
  h->GetYaxis()->SetBinLabel(2,"isVBF");
  h->GetYaxis()->SetBinLabel(3,"isMET");
  h->GetYaxis()->SetBinLabel(4,"isTauMET");
  h->GetYaxis()->SetBinLabel(5,"isSingleTau");
  
  gStyle->SetOptStat(0);

  h->Draw("colz text");
  cout << "Nevts = " << nevt << endl;

}

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
		       Double_t MET){
/***********************************************************************************************************/

  Bool_t passTrg = false;
  if(is_Lep || is_VBF || is_SingleTau){

    passTrg = true;

  } else {
    if(is_TauMET && MET>100.) passTrg = true;
    if(is_MET && MET>200.) passTrg = true;
  }
  return passTrg;
}


void get_histos(TString file_in){

  TFile *f = new TFile(file_in,"read");
  TTree *t = (TTree*)f->Get("HTauTauTree");
 
  TH2D *h = new TH2D("trigger_overlaps","trigger overlaps",7,0.5,7.5,5,0.5,5.5);

  TH1D *h_MET_any    = new TH1D("h_MET_any",   "MET - any trigger",    20, 0, 600);
  TH1D *h_MET_VBF    = new TH1D("h_MET_any",   "MET - VBF trigger",    20, 0, 600);
  TH1D *h_MET_LEP    = new TH1D("h_MET_LEP",   "MET - LEP trigger",    20, 0, 600);
  TH1D *h_MET_MET    = new TH1D("h_MET_MET",   "MET - MET trigger",    20, 0, 600);
  TH1D *h_MET_TAU    = new TH1D("h_MET_TAU",   "MET - TAU trigger",    20, 0, 600);
  TH1D *h_MET_TAUMET = new TH1D("h_MET_TAUMET","MET - TAUMET trigger", 20, 0, 600);

  TH1D *h_ptdau1_any    = new TH1D("h_ptdau1_any",   "ptdau1 - any trigger",    20, 0, 600);
  TH1D *h_ptdau1_VBF    = new TH1D("h_ptdau1_any",   "ptdau1 - VBF trigger",    20, 0, 600);
  TH1D *h_ptdau1_LEP    = new TH1D("h_ptdau1_LEP",   "ptdau1 - LEP trigger",    20, 0, 600);
  TH1D *h_ptdau1_MET    = new TH1D("h_ptdau1_MET",   "ptdau1 - MET trigger",    20, 0, 600);
  TH1D *h_ptdau1_TAU    = new TH1D("h_ptdau1_TAU",   "ptdau1 - TAU trigger",    20, 0, 600);
  TH1D *h_ptdau1_TAUMET = new TH1D("h_ptdau1_TAUMET","ptdau1 - TAUMET trigger", 20, 0, 600);

  TH1D *h_ptdau2_any    = new TH1D("h_ptdau2_any",   "ptdau2 - any trigger",    20, 0, 600);
  TH1D *h_ptdau2_VBF    = new TH1D("h_ptdau2_any",   "ptdau2 - VBF trigger",    20, 0, 600);
  TH1D *h_ptdau2_LEP    = new TH1D("h_ptdau2_LEP",   "ptdau2 - LEP trigger",    20, 0, 600);
  TH1D *h_ptdau2_MET    = new TH1D("h_ptdau2_MET",   "ptdau2 - MET trigger",    20, 0, 600);
  TH1D *h_ptdau2_TAU    = new TH1D("h_ptdau2_TAU",   "ptdau2 - TAU trigger",    20, 0, 600);
  TH1D *h_ptdau2_TAUMET = new TH1D("h_ptdau2_TAUMET","ptdau2 - TAUMET trigger", 20, 0, 600);
  

  for(int i=0; i<t->GetEntries();++i)
    {
      t->GetEntry(i);
     
      // apply baseline selection
      if(!passBaseline(t,i)) continue;

      // retrieve trigger decisions
      Bool_t is_VBF       = t->GetLeaf( "isVBFtrigger"       )->GetValue(0);
      Bool_t is_Lep       = t->GetLeaf( "isLeptrigger"       )->GetValue(0);
      Bool_t is_MET       = t->GetLeaf( "isMETtrigger"       )->GetValue(0);
      Bool_t is_SingleTau = t->GetLeaf( "isSingleTautrigger" )->GetValue(0);
      Bool_t is_TauMET    = t->GetLeaf( "isTauMETtrigger"    )->GetValue(0);
      if(!is_VBF && !is_Lep && !is_MET && !is_SingleTau && !is_TauMET) continue;

      // apply trigger-matching cuts (for MET/tau+MET triggers)
      Double_t MET     = t->GetLeaf( "met_et"  )->GetValue(0);
      if(!passTriggerCuts(is_VBF,is_Lep,is_MET,is_SingleTau,is_TauMET, MET)) continue;

      Bool_t isOnly_VBF       = is_VBF       && !( is_Lep || is_MET || is_SingleTau || is_TauMET );
      Bool_t isOnly_Lep       = is_Lep       && !( is_VBF || is_MET || is_SingleTau || is_TauMET );
      Bool_t isOnly_MET       = is_MET       && !( is_Lep || is_VBF || is_SingleTau || is_TauMET );
      Bool_t isOnly_SingleTau = is_SingleTau && !( is_Lep || is_MET || is_VBF       || is_TauMET );
      Bool_t isOnly_TauMET    = is_TauMET    && !( is_Lep || is_MET || is_SingleTau || is_VBF    );
      
      Bool_t is_LepOrMET = ( is_Lep || is_MET || is_VBF); 
      
      // -- FILL TRIGGER OVERLAP PLOT
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

bool CheckBit (Long64_t number, int bitpos)
{
  /*
    TODO
    0 - HLT_IsoMu24_v
    1 - HLT_IsoMu27_v
    2 - HLT_Ele32_WPTight_Gsf_v
    3 - HLT_Ele35_WPTight_Gsf_v
    4 - HLT_DoubleMediumChargedIsoPFTauHPS35_Trk1_eta2p1_Reg_v
    5 - HLT_IsoMu20_eta2p1_LooseChargedIsoPFTauHPS27_eta2p1_CrossL1_v
    6 - HLT_Ele24_eta2p1_WPTight_Gsf_LooseChargedIsoPFTauHPS30_eta2p1_CrossL1_v
    7 - HLT_VBF_DoubleLooseChargedIsoPFTau20_Trk1_eta2p1_v
    8 - HLT_VBF_DoubleLooseChargedIsoPFTauHPS20_Trk1_eta2p1_v

    9 - HLT_PFHT500_PFMET100_PFMHT100_IDTight_v
    10 - HLT_PFMET100_PFMHT100_IDTight_PFHT60_v
    11 - HLT_PFMET110_PFMHT110_IDTight_v
    12 - HLT_PFMET200_HBHECleaned_v
    13 - HLT_PFMETNoMu100_PFMHTNoMu100_IDTight_PFHT60_v
    14 - HLT_PFMETNoMu110_PFMHTNoMu110_IDTight_v
  */
  Long64_t bitdigit = 1;
  bool res = number & (bitdigit << bitpos);
  return res;
}

void check_trigger(TString proc, TString trig, TString channel, bool save){

  TString dir_in = "/data_CMS/cms/portales/HHresonant_SKIMS/SKIMS_Radion_2018_resDNN_ALLMETtrigger_test_28Apr2021/output_trigEffBkg_TTCR_10bins/";
  TString fname = "hist_"+proc+".root";
  if(strstr(proc,"2018")) fname = "hist_"+proc+"_sum.root";
  
    
  TFile *f_in = new TFile(dir_in+fname,"read");
  
  cout << "Opening file: " << dir_in+fname << endl;
  
  TString hname_all     = "passALL_"     + channel;
  TString hname_met     = "passMET_"     + channel + "_" + trig;
  TString hname_metonly = "passMETonly_" + channel + "_" + trig;

  cout << "Getting hist: " << hname_met << endl;
  TH1D *h_passMET     = (TH1D*)f_in->Get( hname_met     );
  TH1D *h_passALL     = (TH1D*)f_in->Get( hname_all     );
  TH1D *h_passMETonly = (TH1D*)f_in->Get( hname_metonly );


  TH1D *h_eff = (TH1D*)h_passMET->Clone("h_eff");
  h_eff->Divide(h_passALL);  

  //DRAW MET & TRIG EFF ON SAME CANVAS
  TCanvas *c1 = new TCanvas("c1","",600,400);
 
  //create/fill draw h1
  gStyle->SetOptStat(kFALSE);
  h_eff->SetAxisRange(0,1.1,"Y");
  h_eff->SetLineColor(1);
  h_eff->SetLineWidth(2);
  h_eff->GetXaxis()->SetTitle("MET [GeV]");
  h_eff->GetYaxis()->SetTitle("Trigger Efficiency");
  h_eff->GetXaxis()->SetTitleSize(0.045);
  h_eff->GetYaxis()->SetTitleSize(0.045);
  h_eff->Draw("");

  c1->Update();
  
  cout << h_passMETonly->Integral() << endl
       << h_passALL->Integral() << endl
       << h_passMET->Integral() << endl<<endl;
 

  //create hint1 filled with the bins integral of h1
 
  h_passMETonly->Add(h_passALL);
  cout << h_passMETonly->Integral() << endl
       << h_passALL->Integral() << endl
       << h_passMET->Integral() << endl<<endl;

  h_passALL->Scale(1./h_passMETonly->Integral());
  h_passMET->Scale(1./h_passMETonly->Integral());
  h_passMETonly->Scale(1./h_passMETonly->Integral());
  
  //scale hint1 to the pad coordinates
  Float_t rightmax = 1.5*h_passALL->GetMaximum();
  Float_t scale = gPad->GetUymax()/rightmax;
  h_passALL->Scale(scale);
  h_passMET->Scale(scale);
  h_passMETonly->Scale(scale);
  cout << h_passMETonly->Integral() << endl
       << h_passALL->Integral() << endl
       << h_passMET->Integral() << endl;

  h_passMETonly->SetLineWidth(2);
  h_passMETonly->SetLineColor(9);
  h_passMETonly->SetFillColor(9);
  h_passMETonly->Draw("samehist");
  h_passALL->SetLineWidth(2);
  h_passALL->SetLineColor(2);
  h_passALL->SetFillColor(2);
  h_passALL->Draw("samehist");
  h_passMET->SetLineWidth(3);
  h_passMET->SetLineColor(3);
  h_passMET->SetFillColor(3);
  h_passMET->Draw("samehist");
  
  h_eff->Draw("same");
  
  TLegend *leg = new TLegend(0.65,0.65,0.85,0.85);
  leg->AddEntry(h_eff,"Trigger eff.","l");
  leg->AddEntry(h_passMETonly,"passMET, !passLEP","f");
  leg->AddEntry(h_passALL,"!passMET, passLEP","f");
  leg->AddEntry(h_passMET,"passMET, passLEP","f");
  leg->Draw("same");

  TString pair = "";
  if( strstr(channel, "all"    ) ) pair = "All channels";
  if( strstr(channel, "etau"   ) ) pair = "#mu#tau_{h} channel";
  if( strstr(channel, "mutau"  ) ) pair = "e#tau_{h} channel";
  if( strstr(channel, "tautau" ) ) pair = "#tau_{h}#tau_{h} channel";
  if( strstr(channel, "mumu"   ) ) pair = "#mu#mu channel";
  if( strstr(channel, "ee"     ) ) pair = "ee channel";

  TLatex *l = new TLatex();
  l->DrawLatex(338,0.6, pair                   );
  if(strstr(proc,"Radion")){
    TString mass = proc.ReplaceAll("Radion_m","");
    l->DrawLatex(338,0.5, "m_{X} = "+mass+" GeV" );
    l->DrawLatex(338,0.4, "TRIGGER(S): "+trig      );
  } else   {
    l->DrawLatex(338,0.4, "TRIGGER(S): "+trig      );
  }
  
  gPad->SetTickx();

  //draw an axis on the right side
  TGaxis *axis = new TGaxis(gPad->GetUxmax(),gPad->GetUymin(),
			    gPad->GetUxmax(), gPad->GetUymax(),0,rightmax,510,"+L");
  axis->SetTitle("Events / bin (normalised)");
  gPad->RedrawAxis();
  axis->Draw();
  
  TString strtrig = trig; strtrig.ReplaceAll(" ","_");
  
  
  gPad->RedrawAxis();

  if(save){
    //"/data_CMS/cms/portales/HHresonant_SKIMS/SKIMS_Radion_2018_resDNN_ALLMETtrigger_test_28Apr2021/output_trigEffBkg/fig/"
    c1->SaveAs(dir_in+"fig/"+channel+"/png/triggerEff_"+proc+"_trig_"+trig+".png");
    c1->SaveAs(dir_in+"fig/"+channel+"/pdf/triggerEff_"+proc+"_trig_"+trig+".pdf");
  }
  
  
}

int main(){

  const int nproc = 10;
  TString proc[nproc] = {
    "DYall",
    "TTall",

    "Radion_m300",
    "Radion_m400",
    "Radion_m500",
    "Radion_m600",
    "Radion_m700",
    "Radion_m800",
    "Radion_m900",

    "SingleMuon2018"
  };
  
  const int ntrig = 7;
  TString trig[ntrig] = {
    //all trig
    "all",
    //single trig
    "9", "10", "11", "12", "13", "14"
  };
  
  for(int i=0; i<nproc; ++i){
    for(int j=0; j<ntrig; ++j){
      check_trigger( proc[i], trig[j], "all"     , true); //comb
      check_trigger( proc[i], trig[j],  "etau"   , true); //mutau
      check_trigger( proc[i], trig[j],  "mutau"  , true); //etau
      check_trigger( proc[i], trig[j],  "tautau" , true); //tautau
      check_trigger( proc[i], trig[j],  "mumu"   , true); //tautau
      check_trigger( proc[i], trig[j],  "tautau" , true); //tautau
    }
  }

}
  
//void draw_trigger_eff(){main();}

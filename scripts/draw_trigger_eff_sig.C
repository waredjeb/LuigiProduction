#include <iostream>
#include <vector>

#include "TCanvas.h"
#include "TPad.h"
#include "TStyle.h"
#include "TFile.h"
#include "TEfficiency.h"
#include "TGraphAsymmErrors.h"
#include "TH1D.h"
#include "TH2D.h"
#include "TLatex.h"
#include "TLine.h"
#include "TGaxis.h"
#include "TLegend.h"
#include "TString.h"

void debug( std::string s, bool kill=false, bool flag=true ) {
  if(flag) {
    std::cout << "[draw_trigger_eff_sig.C]: " << s << std::endl;
  }
  if(kill) {
    std::cout << "Debug force exit." << std::endl;
    std::exit(0);
  }
}

void findAndReplaceAll(std::string & data, std::string toSearch, std::string replaceStr)
{
    // Get the first occurrence
    size_t pos = data.find(toSearch);
    // Repeat till end is reached
    while( pos != std::string::npos)
    {
        // Replace this occurrence of Sub String
        data.replace(pos, toSearch.size(), replaceStr);
        // Get the next occurrence from the current position
        pos =data.find(toSearch, pos + replaceStr.size());
    }
}

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

void check_trigger(std::string proc, std::string trig, std::string channel, std::string var, bool save){

  std::string dir_in = "/data_CMS/cms/alves/FRAMEWORKTEST/";
  std::string fname = "hist_" + proc + "." + "metnomu200cut" + ".root";
  
  TFile *f_in = new TFile( (dir_in+fname).c_str(), "read");
  if(f_in == nullptr)
    debug( "File pointer " + dir_in + fname + " is null" );
  
  
  std::string hname_all     = "passALL_" + channel + "_" + var;
  std::string hname_trg     = "pass" + trig + "_"     + channel + "_" + var;
  std::string hname_trgonly = "pass" + trig + "only_" + channel + "_" + var;

  TH1D *h_passTRG     = (TH1D*)f_in->Get( hname_trg.c_str() );
  if(h_passTRG == nullptr)
    debug( "Histo pointer " + hname_trg + " is null", true );
  TH1D *h_passALL     = (TH1D*)f_in->Get( hname_all.c_str() );
  if(h_passALL == nullptr)
    debug( "Histo pointer " + hname_all + " is null", true );
  TH1D *h_passTRGonly = (TH1D*)f_in->Get( hname_trgonly.c_str() );
  if(h_passTRGonly == nullptr)
    debug( "Histo pointer " + hname_trgonly + " is null", true );

  TH1D *h_eff = (TH1D*)h_passTRG->Clone("h_eff");
  if(h_eff == nullptr)
    debug( "Histo pointer is null", true );
  h_eff->Divide(h_passALL);  

  //DRAW MET & TRIG EFF ON SAME CANVAS
  TCanvas *c1 = new TCanvas("c1","",600,400);
 
  //create/fill draw h1
  gStyle->SetOptStat(kFALSE);
  h_eff->SetAxisRange(0,1.1,"Y");
  h_eff->SetLineColor(1);
  h_eff->SetLineWidth(2);
  h_eff->GetXaxis()->SetTitle( var.c_str() );
  h_eff->GetYaxis()->SetTitle("Trigger Efficiency");
  h_eff->GetXaxis()->SetTitleSize(0.045);
  h_eff->GetYaxis()->SetTitleSize(0.045);
  h_eff->Draw("");

  c1->Update();
  
  std::cout << h_passTRGonly->Integral() << std::endl
       << h_passALL->Integral() << std::endl
       << h_passTRG->Integral() << std::endl<<std::endl;
 

  //create hint1 filled with the bins integral of h1
 
  h_passTRGonly->Add(h_passALL);
  std::cout << h_passTRGonly->Integral() << std::endl
       << h_passALL->Integral() << std::endl
       << h_passTRG->Integral() << std::endl<<std::endl;

  h_passALL->Scale(1./h_passTRGonly->Integral());
  h_passTRG->Scale(1./h_passTRGonly->Integral());
  h_passTRGonly->Scale(1./h_passTRGonly->Integral());
  
  //scale hint1 to the pad coordinates
  Float_t rightmax = 1.5*h_passALL->GetMaximum();
  Float_t scale = gPad->GetUymax()/rightmax;
  h_passALL->Scale(scale);
  h_passTRG->Scale(scale);
  h_passTRGonly->Scale(scale);
  std::cout << h_passTRGonly->Integral() << std::endl
       << h_passALL->Integral() << std::endl
       << h_passTRG->Integral() << std::endl;

  h_passTRGonly->SetLineWidth(2);
  h_passTRGonly->SetLineColor(9);
  h_passTRGonly->SetFillColor(9);
  h_passTRGonly->Draw("samehist");
  h_passALL->SetLineWidth(2);
  h_passALL->SetLineColor(2);
  h_passALL->SetFillColor(2);
  h_passALL->Draw("samehist");
  h_passTRG->SetLineWidth(3);
  h_passTRG->SetLineColor(3);
  h_passTRG->SetFillColor(3);
  h_passTRG->Draw("samehist");
  
  h_eff->Draw("same");
  
  TLegend *leg = new TLegend(0.65,0.65,0.85,0.85);
  leg->AddEntry(h_eff,"Trigger eff.","l");
  leg->AddEntry(h_passTRGonly,"passTrg AND !passLEP","f");
  leg->AddEntry(h_passALL,"!passTrg AND passLEP","f");
  leg->AddEntry(h_passTRG,"passTrg AND passLEP","f");
  leg->Draw("same");

  std::string pair = "";
  if( strstr(channel.c_str(), "all"    ) ) pair = "All channel.c_str()s";
  if( strstr(channel.c_str(), "etau"   ) ) pair = "#mu#tau_{h} channel.c_str()";
  if( strstr(channel.c_str(), "mutau"  ) ) pair = "e#tau_{h} channel.c_str()";
  if( strstr(channel.c_str(), "tautau" ) ) pair = "#tau_{h}#tau_{h} channel.c_str()";
  if( strstr(channel.c_str(), "mumu"   ) ) pair = "#mu#mu channel.c_str()";
  if( strstr(channel.c_str(), "ee"     ) ) pair = "ee channel.c_str()";

  TLatex *l = new TLatex();
  l->DrawLatex(338,0.6, pair.c_str() );
  if(strstr(proc.c_str(),"Radion")){
    std::string mass(proc);
    findAndReplaceAll(mass, "Radion_m", "");
    
    l->DrawLatex(338,0.5, ("m_{X} = "+mass+" GeV").c_str() );
    l->DrawLatex(338,0.4, ("TRIGGER: "+trig).c_str() );
  } else   {
    l->DrawLatex(338,0.4, ("TRIGGER: "+trig).c_str() );
  }
  
  gPad->SetTickx();

  //draw an axis on the right side
  TGaxis *axis = new TGaxis(gPad->GetUxmax(),gPad->GetUymin(),
			    gPad->GetUxmax(), gPad->GetUymax(),0,rightmax,510,"+L");
  axis->SetTitle("Events / bin (normalised)");
  gPad->RedrawAxis();
  axis->Draw();
  
  std::string strtrig = trig;
  findAndReplaceAll(strtrig, " ", "_");
  
  gPad->RedrawAxis();

  if(save){
    //"/data_CMS/cms/portales/HHresonant_SKIMS/SKIMS_Radion_2018_resDNN_ALLMETtrigger_test_28Apr2021/output_trigEffBkg/fig/"
    std::string path = dir_in+"fig/"+channel+"/png/";
    system( ("mkdir -p " + path).c_str() );
    c1->SaveAs( (path+"triggerEff_"+proc+"_trig_"+trig+"_"+var+".png").c_str() );
    c1->SaveAs( (path+"triggerEff_"+proc+"_trig_"+trig+"_"+var+".pdf").c_str() );
  }  
  
}

int main(){

  const int nproc = 7;
  std::string proc[nproc] = {
    //"DYall",
    //"TTall",

    "Radion_m300",
    "Radion_m400",
    "Radion_m500",
    "Radion_m600",
    "Radion_m700",
    "Radion_m800",
    "Radion_m900"

    //"SingleMuon2018"
  };
  
  std::vector< std::string > trig = {
    //all trig
    "MET","Tau","TauMET"
    //single trig
    //"9", "10", "11", "12", "13", "14"
  };

  std::vector< std::string > var = {
    "dau1_pt",
    "dau2_pt",
    "met_et",
    "mht_et",
    "HT20",
    "metnomu_et",
    "mhtnomu_et"
  };
  
  for(int i=0; i<nproc; ++i){
    for(int j=0; j<trig.size(); ++j){
      for(int k=0; k<var.size(); ++k){
	check_trigger( proc[i], trig[j].c_str(), "all"     , var[k].c_str(), true); //comb
	check_trigger( proc[i], trig[j].c_str(),  "etau"   , var[k].c_str(), true); //mutau
	check_trigger( proc[i], trig[j].c_str(),  "mutau"  , var[k].c_str(), true); //etau
	check_trigger( proc[i], trig[j].c_str(),  "tautau" , var[k].c_str(), true); //tautau
	check_trigger( proc[i], trig[j].c_str(),  "mumu"   , var[k].c_str(), true); //tautau
	check_trigger( proc[i], trig[j].c_str(),  "tautau" , var[k].c_str(), true); //tautau
      }
    }
  }

}
  
//void draw_trigger_eff(){main();}

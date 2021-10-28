#include <iostream>

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
#include "TLegend.h"
#include "TString.h"


///////////////////
void RedrawBorder()
///////////////////
{
  // this little macro redraws the axis tick marks and the pad border lines.
  gPad->Update();
  gPad->RedrawAxis();
  TLine l;
  l.SetLineWidth(2);

  /*top border   */l.DrawLine(gPad->GetUxmin(), gPad->GetUymax(), gPad->GetUxmax(), gPad->GetUymax());
  /*right border */l.DrawLine(gPad->GetUxmax(), gPad->GetUymin(), gPad->GetUxmax(), gPad->GetUymax());
  /*left border  */l.DrawLine(gPad->GetUxmin(), gPad->GetUymin(), gPad->GetUxmin(), gPad->GetUymax());
  /*bottom border*/l.DrawLine(gPad->GetUxmin(), gPad->GetUymin(), gPad->GetUxmax(), gPad->GetUymin());
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

void check_trigger(TString data, TString proc, TString htcut, TString trig, TString channel, bool save){

  //TString dir_in = "/data_CMS/cms/portales/HHresonant_SKIMS/SKIMS_Radion_2018_resDNN_ALLMETtrigger_test_28Apr2021/output_trigEffBkg_TTCR_fixedtrig/";
  TString dir_in = "/data_CMS/cms/alves/FRAMEWORKTEST/";
  TString fname      = "hist_"+proc+"."+htcut+".root";
  //TString fname_data = "hist_"+data+"_sum.HTcut600.root";
  TString fname_data = "hist_"+data+"."+htcut+".root";

  TFile *f_data = new TFile(dir_in+fname_data,"read");

  TFile *f_in   = new TFile(dir_in+fname,"read");

  std::cout << "Opening file: " << dir_in+fname << std::endl;

  TString hname_all     = "passALL_"     + channel;
  TString hname_met     = "passMET_"     + channel + "_" + trig;
  TString hname_metonly = "passMETonly_" + channel + "_" + trig;
  hname_met.ReplaceAll("_all_all","_all");
  hname_metonly.ReplaceAll("_all_all","_all");
  std::cout << "Getting hist: " << hname_met << std::endl;
  TH1D *h_passMET     = (TH1D*)f_in->Get( hname_met     );
  TH1D *h_passALL     = (TH1D*)f_in->Get( hname_all     );
  TH1D *h_passMETonly = (TH1D*)f_in->Get( hname_metonly );

  TH1D *d_passMET     = (TH1D*)f_data->Get( hname_met     );
  TH1D *d_passALL     = (TH1D*)f_data->Get( hname_all     );
  TH1D *d_passMETonly = (TH1D*)f_data->Get( hname_metonly );

//  h_passMET     ->Rebin(3);
//  h_passALL     ->Rebin(3);
//  h_passMETonly ->Rebin(3);
//  d_passMET     ->Rebin(3);
//  d_passALL     ->Rebin(3);
//  d_passMETonly ->Rebin(3);


  std::cout << "effmc" <<std::endl;
  TEfficiency *effmc = new TEfficiency(*h_passMET,*h_passALL);
  TH1D *heffmc = (TH1D*)h_passMET->Clone("h_eff");
  TGraphAsymmErrors *geffmc = new TGraphAsymmErrors();
  geffmc->Divide(heffmc,h_passALL,"cp");
  heffmc->Divide(heffmc,h_passALL,1,1,"B");

  std::cout << "effdata"<<std::endl;
  TEfficiency *eff = new TEfficiency(*d_passMET,*d_passALL);
  TH1D *heff = (TH1D*)d_passMET->Clone("d_eff");
  TGraphAsymmErrors *geff = new TGraphAsymmErrors();
  //heff->Divide(heff,d_passALL,1,1,"B");
  geff->Divide(heff,d_passALL,"cp");
  heff->Divide(heff,d_passALL,1,1,"B");

  // loop on tgraphs to get errors
  const int npoint = geffmc->GetN();
  Double_t xp[6],yp[6];
  Double_t ye_up[6],ye_down[6];
  for (int i=0;i<6;i++) {
    geffmc->GetPoint(i,xp[i],yp[i]);
    ye_up[i] = geffmc->GetErrorYhigh(i);
    ye_down[i] = geffmc->GetErrorYlow(i);
    printf("xp[%d] = %g - yp[%d] = %g +%g/-%g\n",i,xp[i],i,yp[i],ye_up[i],ye_down[i]);
  }
  std::cout << "=======" << std::endl;
  Double_t dxp[6],dyp[6];
  Double_t dye_up[6],dye_down[6];

  Double_t sf_xp[6],sf_yp[6];
  Double_t sf_ye_up[6],sf_ye_down[6];
  for (int i=0;i<6;i++) {
    geff->GetPoint(i,dxp[i],dyp[i]);
    dye_up[i] = geff->GetErrorYhigh(i);
    dye_down[i] = geff->GetErrorYlow(i);
    printf("xp[%d] = %g - yp[%d] = %g +%g/-%g\n",i,dxp[i],i,dyp[i],dye_up[i],dye_down[i]);

    sf_xp[i] = xp[i];
    sf_yp[i] = dyp[i]/yp[i];
    sf_ye_up[i]   = sqrt(pow(ye_up[i],2) + pow(dye_up[i],2));
    sf_ye_down[i] = sqrt(pow(ye_down[i],2) + pow(dye_down[i],2));
  }


  std::cout << "=== SF ====" << std::endl;
  for(int i=0;i<6;i++) printf("xp[%d] = %g - yp[%d] = %g +%g/-%g\n",i,sf_xp[i],i,sf_yp[i],sf_ye_up[i],sf_ye_down[i]);

  std::cout << "all good" << std::endl;
  //TEfficiency *effmc = new TEfficiency(h_passMET,h_passALL);
  TH1D * sf1 = (TH1D*) heff->Clone("sf");
  Double_t a[6] = {50,50,50,50,50,50};
  TGraphAsymmErrors *sf = new TGraphAsymmErrors(6,sf_xp,sf_yp,a,a,sf_ye_down,sf_ye_up);
  //sf->Divide(heffmc);
  //sf->Divide(sf1,heffmc,"cp");
  std::cout << "all good 2" << std::endl;

  //DRAW MET & TRIG EFF ON SAME CANVAS
  //TCanvas *c1 = new TCanvas("c1","",600,400);

  TCanvas *canvas = new TCanvas("canvas","",600,600);
  gStyle->SetOptStat(0);
  gStyle->SetOptTitle(0);
  canvas->cd();


  eff  -> SetLineColor(1);
  eff  -> SetLineWidth(2);
  eff  -> SetMarkerColor(1);
  eff  -> SetMarkerSize(1.5);
  eff  -> SetMarkerStyle(20);

  effmc  -> SetLineColor(kRed);
  effmc  -> SetLineWidth(2);
  effmc  -> SetMarkerColor(kRed);
  effmc  -> SetMarkerSize(1.4);
  effmc  -> SetMarkerStyle(22);

  sf  -> SetLineColor(kRed);
  sf  -> SetLineWidth(2);
  sf  -> SetMarkerColor(kRed);
  sf  -> SetMarkerSize(1.4);
  sf  -> SetMarkerStyle(22);

//  TH1D *line_sf      = (TH1D*)sf      -> Clone( "line_sf     " );
//  TH1D *line_eff     = (TH1D*)eff     -> Clone( "line_eff    " );
//  TH1D *line_effmc   = (TH1D*)effmc   -> Clone( "line_effmc  " );
//  for(int i = 1; i<11; ++i){
//    line_sf      -> SetBinError(i, 0.000000001 );
//    line_eff     -> SetBinError(i, 0.000000001 );
//    line_effmc   -> SetBinError(i, 0.000000001 );
//  }


  std::cout << "did it break" << std::endl;


  TPad *pad1 = new TPad("pad1","pad1",0,0.35,1,1);
  pad1->SetBottomMargin(0.005);
  pad1->SetLeftMargin(0.2);
  pad1->Draw();
  pad1->cd();

  TH2D *axor = new TH2D("axor","axor",4,0,600,100,-0.1,1.7);
  axor->GetYaxis()->SetTitle("Efficiency");
  axor->GetXaxis()->SetLabelOffset(1);
  axor->GetXaxis()->SetLabelOffset(1.);
  axor->GetYaxis()->SetTitleSize(0.08);
  axor->GetYaxis()->SetTitleOffset(.85);
  axor->GetXaxis()->SetLabelSize(0.07);
  axor->GetYaxis()->SetLabelSize(0.07);
  axor->Draw();

  //  line_eff->Draw("SAME ");
  //  line_effmc->Draw("SAME ");

  eff    ->Draw("SAME p0 e ");
  effmc  ->Draw("SAME p0 ");
  pad1->RedrawAxis();

  TLegend *leg = new TLegend(0.25, 0.55, 0.47, 0.75);
  leg->SetFillColor(0);
  leg->SetShadowColor(0);
  leg->SetBorderSize(0);
  leg->SetTextSize(0.06);
  leg->SetFillStyle(0);
  leg->SetTextFont(42);
  char message[100];
  sprintf(message,"Data");
  leg->AddEntry(eff,message,"p");
  sprintf(message,proc);
  leg->AddEntry(effmc,message,"p");
  leg->Draw("same");

  RedrawBorder();


    Double_t lX = 0.25;
    Double_t lY = 0.84;
    Double_t lYstep = 0.045;
    TLatex l;
    l.SetNDC();
    l.SetTextFont(72);
    l.SetTextColor(1);

    TString latexChannel = channel;
    latexChannel.ReplaceAll("mu","#mu");
    latexChannel.ReplaceAll("tau","#tau_{h}");
    latexChannel.ReplaceAll("Tau","#tau_{h}");

    l.DrawLatex( lX, lY,        "Channel: "+latexChannel);
    l.DrawLatex( lX, lY-lYstep, "Trigger(s): "+trig);


  canvas->cd();
  TPad *pad2 = new TPad("pad2","pad2",0,0.0,1,0.35);
  pad2->SetTopMargin(0.005);
  pad2->SetBottomMargin(0.4);
  pad2->SetLeftMargin(0.2);
  pad2->Draw();
  pad2->cd();
  pad2->SetGridy();

  TH2D *axor2 = new TH2D("axor2","axor2",4,0,600,100,0.8,1.2);
  axor2->GetYaxis()->SetNdivisions(507);
  axor2->GetYaxis()->SetLabelSize(0.13);
  axor2->GetXaxis()->SetLabelSize(0.13);
  axor2->SetTitleSize(0.15,"X");
  axor2->SetTitleSize(0.15,"Y");
  axor2->GetXaxis()->SetTitleOffset(1.);
  axor2->GetYaxis()->SetTitleOffset(0.45);
  axor2->GetYaxis()->SetTitle("Data/MC");
  axor2->GetXaxis()->SetTitle("MET [GeV]");
  if(strstr(htcut,"MET")) axor2->GetXaxis()->SetTitle("H_{T} [GeV]");

  axor2->Draw();
  TLine *line = new TLine(20,1,120,1);
  line->SetLineColor(1);
  line->SetLineWidth(2);

  sf->GetYaxis()->SetNdivisions(507);
  sf->GetYaxis()->SetLabelSize(0.13);
  sf->GetXaxis()->SetLabelSize(0.13);
  sf->GetXaxis()->SetTitleSize(0.15);
  sf->GetYaxis()->SetTitleSize(0.15);
  sf->GetXaxis()->SetTitleOffset(1.);
  sf->GetYaxis()->SetTitleOffset(0.45);
  sf->GetYaxis()->SetTitle("Data/MC");
  sf->GetXaxis()->SetTitle("MET [GeV]");

  //line->Draw();
  sf->Draw("same P0");
  RedrawBorder();

  if(save){
    //"/data_CMS/cms/portales/HHresonant_SKIMS/SKIMS_Radion_2018_resDNN_ALLMETtrigger_test_28Apr2021/output_trigEffBkg/fig/"
    canvas->SaveAs(dir_in+"fig/"+channel+"/png/triggerSF_"+data+"_"+proc+"_trig_"+trig+"."+htcut+".png");
    canvas->SaveAs(dir_in+"fig/"+channel+"/pdf/triggerSF_"+data+"_"+proc+"_trig_"+trig+"."+htcut+".pdf");
  }


}

//run with: 'g++ -o draw draw_trigger_sf.C `root-config --cflags --ldflags --evelibs`' followed by './draw'
int main(){

  std::vector< std::string > proc = {
    "Radion_m300",
    "Radion_m400",
    "Radion_m500",
    "Radion_m600",
    "Radion_m700",
    "Radion_m800",
    "Radion_m900"
    // "DYall",
    // "TTall",
    // "SingleMuon2018"
  };

  std::vector< std::string > trig = {
    //all trig
    "all"
    //single trig
    //"9", "10", "11"//, "12",
    //"13", "14"
  };

  for(int i=0; i<proc.size(); ++i){
    for(int j=0; j<trig.size(); ++j){
      //check_trigger( "SingleMuon2018", proc[i], trig[j].c_str(), "all"     , true); //comb
      //std::cout << 1 << std::endl;
      //check_trigger( "SingleMuon2018", proc[i], trig[j].c_str(),  "etau"   , true); //mutau
      //std::cout << 2 << std::endl;
      //check_trigger( "SingleMuon2018", proc[i], trig[j].c_str(),  "mutau"  , true); //etau
      //std::cout << 3 << std::endl;
      //check_trigger( "SingleMuon2018", proc[i], trig[j].c_str(),  "tautau" , true); //tautau
      //std::cout << 4 << std::endl;
      //check_trigger( "SingleMuon2018", proc[i], trig[j].c_str(),  "mumu"   , true); //tautau
      //std::cout << 5 << std::endl;
      //check_trigger( "SingleMuon2018", proc[i], trig[j].c_str(),  "tautau" , true); //tautau
      //std::cout << 6 << std::endl;
      // check_trigger( "MET2018_partial", proc[i].c_str(), "HTcut600", trig[j].c_str(), "all"     , true); //comb
      // check_trigger( "MET2018_partial", proc[i].c_str(), "HTcut600", trig[j].c_str(),  "etau"   , true); //mutau
      // check_trigger( "MET2018_partial", proc[i].c_str(), "HTcut600", trig[j].c_str(),  "mutau"  , true); //etau
      // check_trigger( "MET2018_partial", proc[i].c_str(), "HTcut600", trig[j].c_str(),  "tautau" , true); //tautau
      // check_trigger( "MET2018_partial", proc[i].c_str(), "HTcut600", trig[j].c_str(),  "mumu"   , true); //tautau
      // check_trigger( "MET2018_partial", proc[i].c_str(), "HTcut600", trig[j].c_str(),  "tautau" , true); //tautau
      // check_trigger( "MET2018_sum", proc[i].c_str(), "HTcut100", trig[j].c_str(), "all"     , true); //comb
      // check_trigger( "MET2018_sum", proc[i].c_str(), "HTcut100", trig[j].c_str(),  "etau"   , true); //mutau
      // check_trigger( "MET2018_sum", proc[i].c_str(), "HTcut100", trig[j].c_str(),  "mutau"  , true); //etau
      // check_trigger( "MET2018_sum", proc[i].c_str(), "HTcut100", trig[j].c_str(),  "tautau" , true); //tautau
      // check_trigger( "MET2018_sum", proc[i].c_str(), "HTcut100", trig[j].c_str(),  "mumu"   , true); //tautau
      // check_trigger( "MET2018_sum", proc[i].c_str(), "HTcut100", trig[j].c_str(),  "tautau" , true); //tautau
      // check_trigger( "MET2018_sum", proc[i].c_str(), "noHTcut", trig[j].c_str(), "all"     , true); //comb
      // check_trigger( "MET2018_sum", proc[i].c_str(), "noHTcut", trig[j].c_str(),  "etau"   , true); //mutau
      // check_trigger( "MET2018_sum", proc[i].c_str(), "noHTcut", trig[j].c_str(),  "mutau"  , true); //etau
      // check_trigger( "MET2018_sum", proc[i].c_str(), "noHTcut", trig[j].c_str(),  "tautau" , true); //tautau
      // check_trigger( "MET2018_sum", proc[i].c_str(), "noHTcut", trig[j].c_str(),  "mumu"   , true); //tautau
      // check_trigger( "MET2018_sum", proc[i].c_str(), "noHTcut", trig[j].c_str(),  "tautau" , true); //tautau
      // check_trigger( "MET2018_sum", proc[i].c_str(), "METcut200", trig[j].c_str(), "all"     , true); //comb
      // check_trigger( "MET2018_sum", proc[i].c_str(), "METcut200", trig[j].c_str(),  "etau"   , true); //mutau
      // check_trigger( "MET2018_sum", proc[i].c_str(), "METcut200", trig[j].c_str(),  "mutau"  , true); //etau
      // check_trigger( "MET2018_sum", proc[i].c_str(), "METcut200", trig[j].c_str(),  "tautau" , true); //tautau
      // check_trigger( "MET2018_sum", proc[i].c_str(), "METcut200", trig[j].c_str(),  "mumu"   , true); //tautau
      // check_trigger( "MET2018_sum", proc[i].c_str(), "METcut200", trig[j].c_str(),  "tautau" , true); //tautau


      check_trigger( "MET2018_sum", proc[i].c_str(), "metnomu200cut", trig[j].c_str(),  "all"     , true); //comb
      check_trigger( "MET2018_sum", proc[i].c_str(), "metnomu200cut", trig[j].c_str(),  "etau"   , true); //mutau
      check_trigger( "MET2018_sum", proc[i].c_str(), "metnomu200cut", trig[j].c_str(),  "mutau"  , true); //etau
      check_trigger( "MET2018_sum", proc[i].c_str(), "metnomu200cut", trig[j].c_str(),  "tautau" , true); //tautau
      check_trigger( "MET2018_sum", proc[i].c_str(), "metnomu200cut", trig[j].c_str(),  "mumu"   , true); //tautau
      check_trigger( "MET2018_sum", proc[i].c_str(), "metnomu200cut", trig[j].c_str(),  "tautau" , true); //tautau

    }
  }

}

//void draw_trigger_eff(){main();}

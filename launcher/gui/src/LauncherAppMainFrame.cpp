#include "LauncherAppMainFrame.h"

#include <iostream>
#include <wx/fileconf.h>

#include "Exception.h"
#include "LibjvmWrapper.hpp"

LauncherAppMainFrame::LauncherAppMainFrame(wxWindow* parent)
    : MainFrame(parent) {
    m_javaHomeDirPicker->SetPath(wxFileConfig::Get()->Read(kJavaHomeSettingsKey));
    m_mountPathDirPicker->SetPath(wxFileConfig::Get()->Read(kMountPointSettingsKey));
    wxGridSizer* bSizer4;
    bSizer4 = new wxGridSizer(1, 0, 0);

    m_panel5->SetSizer(bSizer4);
    m_panel5->Layout();
    bSizer4->Fit(m_panel5);
    m_webView = wxWebView::New(m_panel5, wxID_ANY);
    bSizer4->Add(m_webView, 0, wxALL | wxEXPAND);
    m_webView->LoadURL("http://localhost:8080");
}

void LauncherAppMainFrame::OnStartStopButtonClick(wxCommandEvent& event) {
    switch (_dhfsInstance.state()) {
        case DhfsInstanceState::RUNNING:
            m_statusText->SetLabel("Stopped");
            m_startStopButton->SetLabel("Start");
            m_statusBar1->SetStatusText("Stopped", 0);
            _dhfsInstance.stop();
            break;
        case DhfsInstanceState::STOPPED:
            LibjvmWrapper::instance().setJavaHome(wxFileConfig::Get()->Read(kJavaHomeSettingsKey).ToStdString());
            m_statusText->SetLabel("Running");
            m_startStopButton->SetLabel("Stop");
            m_statusBar1->SetStatusText("Running", 0);
            _dhfsInstance.start(wxFileConfig::Get()->Read(kMountPointSettingsKey).ToStdString(), {

                                });
            break;
        default:
            throw Exception("Unhandled switch case");
    }
}

void LauncherAppMainFrame::OnJavaHomeChanged(wxFileDirPickerEvent& event) {
    wxFileConfig::Get()->Write(kJavaHomeSettingsKey, event.GetPath());
}

void LauncherAppMainFrame::OnMountPathChanged(wxFileDirPickerEvent& event) {
    wxFileConfig::Get()->Write(kMountPointSettingsKey, event.GetPath());
}

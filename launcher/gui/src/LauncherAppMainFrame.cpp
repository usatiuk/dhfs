#include "LauncherAppMainFrame.h"

#include <iostream>
#include <wx/fileconf.h>

#include "Exception.h"

wxDEFINE_EVENT(NEW_LINE_OUTPUT_EVENT, wxCommandEvent);
wxDEFINE_EVENT(SHUTDOWN_EVENT, wxCommandEvent);

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

    Bind(NEW_LINE_OUTPUT_EVENT, &LauncherAppMainFrame::onNewLineOutput, this);
    Bind(SHUTDOWN_EVENT, &LauncherAppMainFrame::onShutdown, this);
    wxFont font = wxFont(wxSize(16, 16),
                         wxFontFamily::wxFONTFAMILY_TELETYPE,
                         wxFontStyle::wxFONTSTYLE_NORMAL,
                         wxFontWeight::wxFONTWEIGHT_NORMAL);
    m_logOutputTextCtrl->SetFont(font);
    updateState();
}

void LauncherAppMainFrame::updateState() {
    switch (_dhfsInstance.state()) {
        case DhfsInstanceState::RUNNING:
            m_statusText->SetLabel("Running");
            m_startStopButton->SetLabel("Stop");
            m_statusBar1->SetStatusText("Running", 0);
            break;
        case DhfsInstanceState::STOPPED: {
            // wxFileConfig::Get()->Read(kJavaHomeSettingsKey).ToStdString();
            m_statusText->SetLabel("Stopped");
            m_startStopButton->SetLabel("Start");
            m_statusBar1->SetStatusText("Stopped", 0);
            break;
        }
        default:
            throw Exception("Unhandled switch case");
    }
}

void LauncherAppMainFrame::OnStartStopButtonClick(wxCommandEvent& event) {
    switch (_dhfsInstance.state()) {
        case DhfsInstanceState::RUNNING:
            _dhfsInstance.stop();
            break;
        case DhfsInstanceState::STOPPED: {
            DhfsStartOptions options;
            options.java_home = wxFileConfig::Get()->Read(kJavaHomeSettingsKey);
            options.xmx = "512m"; // Default memory allocation, can be changed
            options.mount_path = wxFileConfig::Get()->Read(kMountPointSettingsKey);
            options.data_path = "/Users/stepus53/dhfs_test/launcher/data";
            options.jar_path = "/Users/stepus53/projects/dhfs/dhfs-parent/dhfs-fuse/target/quarkus-app/quarkus-run.jar";
            options.webui_path = "/Users/stepus53/projects/dhfs/webui/dist";

            _dhfsInstance.start(options);
            break;
        }
        default:
            throw Exception("Unhandled switch case");
    }
    updateState();
}

void LauncherAppMainFrame::OnJavaHomeChanged(wxFileDirPickerEvent& event) {
    wxFileConfig::Get()->Write(kJavaHomeSettingsKey, event.GetPath());
}

void LauncherAppMainFrame::OnMountPathChanged(wxFileDirPickerEvent& event) {
    wxFileConfig::Get()->Write(kMountPointSettingsKey, event.GetPath());
}

void LauncherAppMainFrame::onNewLineOutput(wxCommandEvent& event) {
    m_logOutputTextCtrl->AppendText(event.GetString());
}

void LauncherAppMainFrame::onShutdown(wxCommandEvent& event) {
    m_logOutputTextCtrl->AppendText("Shutdown");
    updateState();
}

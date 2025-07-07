#include "LauncherAppMainFrame.h"

#include <iostream>
#include <wx/fileconf.h>
#include <wx/stdpaths.h>
#include <filesystem>

#ifdef __APPLE__
#include "macos/utils.h"
#endif

#include "LauncherApp.h"
#include "Exception.h"

std::string getBundlePath() {
    if (wxGetenv("DHFS_BUNDLE_PATH") == NULL)
        return std::filesystem::path(wxStandardPaths::Get().GetExecutablePath().ToStdString())
#ifndef __APPLE__
    .parent_path()
#endif
                .parent_path().string();
    return wxGetenv("DHFS_BUNDLE_PATH");
}

LauncherAppMainFrame::LauncherAppMainFrame(wxWindow* parent)
    : MainFrame(parent) {
    m_javaHomeDirPicker->SetPath(wxFileConfig::Get()->Read(kJavaHomeSettingsKey));
    m_mountPathDirPicker->SetPath(wxFileConfig::Get()->Read(kMountPointSettingsKey));
    m_dataPathDirPicker->SetPath(wxFileConfig::Get()->Read(kDataDirSettingsKey));

    m_webViewSizer = new wxGridSizer(1, 0, 0);
    m_panel5->SetSizer(m_webViewSizer);
    m_panel5->Layout();
    m_webViewSizer->Fit(m_panel5);

    Bind(NEW_LINE_OUTPUT_EVENT, &LauncherAppMainFrame::onNewLineOutput, this);
    Bind(DHFS_STATE_CHANGE_EVENT, &LauncherAppMainFrame::onDhfsInstanceStateChange, this);
    wxFont font = wxFont(wxSize(16, 16),
                         wxFontFamily::wxFONTFAMILY_TELETYPE,
                         wxFontStyle::wxFONTSTYLE_NORMAL,
                         wxFontWeight::wxFONTWEIGHT_NORMAL);
    m_logOutputTextCtrl->SetFont(font);
    updateState();
#ifdef __APPLE__
    SetAppAsRegular();
#endif
}

static DhfsInstance& getDhfsInstance() {
    return wxGetApp().m_dhfsInstance;
}

void LauncherAppMainFrame::updateState() {
    switch (getDhfsInstance().state()) {
        case DhfsInstanceState::RUNNING: {
            m_statusText->SetLabel("Running");
            m_startStopButton->SetLabel("Stop");
            m_statusBar1->SetStatusText("Running", 0);

            if (m_notebook1->GetSelection() == 4) {
                if (m_webView != nullptr)
                    m_webView->LoadURL("http://localhost:8080");
            }

            wxGetApp().SetExitOnFrameDelete(false);
            break;
        }
        case DhfsInstanceState::STARTING: {
            m_statusText->SetLabel("Starting");
            m_startStopButton->SetLabel("Stop");
            m_statusBar1->SetStatusText("Starting", 0);
            wxGetApp().SetExitOnFrameDelete(false);
            break;
        }
        case DhfsInstanceState::STOPPED: {
            m_statusText->SetLabel("Stopped");
            m_startStopButton->SetLabel("Start");
            m_statusBar1->SetStatusText("Stopped", 0);
            wxGetApp().SetExitOnFrameDelete(true);
            break;
        }
        case DhfsInstanceState::STOPPING: {
            m_statusText->SetLabel("Stopping");
            m_startStopButton->SetLabel("Kill");
            m_statusBar1->SetStatusText("Stopping", 0);
            wxGetApp().SetExitOnFrameDelete(false);
            break;
        }
        default:
            throw Exception("Unhandled switch case");
    }
}

void LauncherAppMainFrame::OnStartStopButtonClick(wxCommandEvent& event) {
    switch (getDhfsInstance().state()) {
        case DhfsInstanceState::RUNNING:
        case DhfsInstanceState::STARTING: {
            getDhfsInstance().stop();
            break;
        }
        case DhfsInstanceState::STOPPED: {
            DhfsStartOptions options;
            options.java_home = wxFileConfig::Get()->Read(kJavaHomeSettingsKey);
            options.xmx = "512m";
            options.mount_path = wxFileConfig::Get()->Read(kMountPointSettingsKey);
            options.data_path = wxFileConfig::Get()->Read(kDataDirSettingsKey);
            options.jar_path = getBundlePath() + "/app/Server/quarkus-run.jar";
            options.webui_path = getBundlePath() + "/app/Webui";

            getDhfsInstance().start(options);
            break;
        }
        case DhfsInstanceState::STOPPING: {
            // TODO:
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

void LauncherAppMainFrame::OnDataPathChanged(wxFileDirPickerEvent& event) {
    wxFileConfig::Get()->Write(kDataDirSettingsKey, event.GetPath());
}

void LauncherAppMainFrame::onNewLineOutput(wxCommandEvent& event) {
    m_logOutputTextCtrl->AppendText(event.GetString());
}

void LauncherAppMainFrame::OnNotebookPageChanged(wxBookCtrlEvent& event) {
    if (event.GetSelection() == 4) prepareWebview();
    else unloadWebview();
}

void LauncherAppMainFrame::OnNotebookPageChanging(wxBookCtrlEvent& event) {
}

void LauncherAppMainFrame::onDhfsInstanceStateChange(wxCommandEvent& event) {
    updateState();
}

void LauncherAppMainFrame::unloadWebview() {
    if (m_webView != nullptr) {
        m_webViewSizer->Detach(m_webView);
        m_webView->Destroy();
        m_webView = nullptr;
    }
}

void LauncherAppMainFrame::prepareWebview() {
    m_webView = wxWebView::New(m_panel5, wxID_ANY);
    m_webViewSizer->Add(m_webView, 0, wxALL | wxEXPAND);
    m_webView->LoadURL("http://localhost:8080");
    m_panel5->Layout();
}


void LauncherAppMainFrame::OnActivate(wxActivateEvent& event) {
    MainFrame::OnActivate(event);
}

void LauncherAppMainFrame::OnActivateApp(wxActivateEvent& event) {
    MainFrame::OnActivateApp(event);
}

void LauncherAppMainFrame::OnClose(wxCloseEvent& event) {
#ifdef __APPLE__
    SetAppAsAccessory();
#endif
    wxGetApp().OnTopFrameClose(event);
    MainFrame::OnClose(event);
}

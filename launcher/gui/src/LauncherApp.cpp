//
// Created by Stepan Usatiuk on 11.07.2024.
//

#include "wx/notebook.h"

#include "LauncherApp.h"

#include <filesystem>

#include "LauncherAppMainFrame.h"
#include "wx/taskbar.h"
#include <wx/fileconf.h>
#include <wx/stdpaths.h>

#include "wx/snglinst.h"

IMPLEMENT_APP(LauncherApp)

static std::string getServerSocket() {
#ifdef __WIN32__
    return "dhfs-sock-" + wxGetUserId().ToStdString();
#else
    return wxStandardPaths::Get().GetUserLocalDataDir().ToStdString()
           + "/" + "dhfs-sock-" + wxGetUserId().ToStdString();
#endif
}

// This is executed upon startup, like 'main()' in non-wxWidgets programs.
bool LauncherApp::OnInit() {
    m_checker = new wxSingleInstanceChecker;
    if (!std::filesystem::is_directory(wxStandardPaths::Get().GetUserLocalDataDir().ToStdString())
        && !std::filesystem::create_directories(wxStandardPaths::Get().GetUserLocalDataDir().ToStdString())) {
        wxLogError("Couldn't create data directory: %s", wxStandardPaths::Get().GetUserLocalDataDir());
        return false;
    }
    if (m_checker->IsAnotherRunning()) {
        // wxLogError(_("Another program instance is already running, aborting."));

        delete m_checker; // OnExit() won't be called if we return false
        m_checker = NULL;

        auto clinet = new wxClient();
        auto conn = clinet->MakeConnection("dhfs", getServerSocket(), "dhfs");
        conn->Execute("wakeup");

        return false;
    }

    if (!m_server.Create(getServerSocket())) {
        wxLogError("Couldn't create server!");
        return false;
    }

    wxFrame* frame = new LauncherAppMainFrame(NULL);
    frame->Show(true);
    SetTopWindow(frame);

    Bind(NEW_LINE_OUTPUT_EVENT, &LauncherApp::forwardEventToTopWindow, this);
    Bind(DHFS_STATE_CHANGE_EVENT, &LauncherApp::forwardEventToTopWindow, this);

    return true;
}

bool LauncherApp::OnExceptionInMainLoop() {
    try {
        std::rethrow_exception(std::current_exception());
    } catch (const std::exception& e) {
        wxMessageBox(e.what(), "Error", wxOK | wxICON_ERROR | wxCENTRE, GetTopWindow());
    }
    return true;
}

int LauncherApp::OnExit() {
    delete m_checker;

    return wxApp::OnExit();
}

void LauncherApp::Open() {
    if (m_topWindow) {
        m_topWindow->SetFocus();
        m_topWindow->Raise();
        if (auto frame = dynamic_cast<wxFrame*>(m_topWindow)) {
            frame->RequestUserAttention();
        }
    } else {
        wxFrame* frame = new LauncherAppMainFrame(NULL);
        frame->Show(true);
        SetTopWindow(frame);
    }
}

void LauncherApp::OnTopFrameClose(wxCloseEvent& event) {
    SetTopWindow(nullptr);
}

void LauncherApp::forwardEventToTopWindow(wxCommandEvent& event) {
    if (m_topWindow) {
        m_topWindow->GetEventHandler()->ProcessEvent(event);
    } else {
        event.Skip();
    }
}

#ifdef __APPLE__
void LauncherApp::MacReopenApp() {
    this->Open();
}
#endif

//
// Created by Stepan Usatiuk on 11.07.2024.
//

// For compilers that don't support precompilation, include "wx/wx.h"
#include "wx/notebook.h"

#include "LauncherApp.h"

#include "LauncherAppMainFrame.h"
#include "wx/taskbar.h"
#include <wx/fileconf.h>
#include "wx/snglinst.h"

IMPLEMENT_APP(LauncherApp)

// This is executed upon startup, like 'main()' in non-wxWidgets programs.
bool LauncherApp::OnInit() {
    m_checker = new wxSingleInstanceChecker;
    if (m_checker->IsAnotherRunning()) {
        // wxLogError(_("Another program instance is already running, aborting."));

        delete m_checker; // OnExit() won't be called if we return false
        m_checker = NULL;

        auto clinet = new wxClient();
        auto conn = clinet->MakeConnection("dhfs", "/Users/stepus53/dhfs-sock", "dhfs");
        conn->Execute("wakeup");

        return false;
    }

    m_server.Create("/Users/stepus53/dhfs-sock");

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

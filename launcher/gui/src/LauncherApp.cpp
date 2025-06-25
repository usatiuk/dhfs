//
// Created by Stepan Usatiuk on 11.07.2024.
//

// For compilers that don't support precompilation, include "wx/wx.h"
#include "wx/wxprec.h"

#ifndef WX_PRECOMP
#	include "wx/wx.h"
#endif

#include "wx/notebook.h"

#include "LauncherApp.h"

#include "LauncherAppMainFrame.h"
#include "wx/taskbar.h"
#include <wx/fileconf.h>
IMPLEMENT_APP(LauncherApp)

// This is executed upon startup, like 'main()' in non-wxWidgets programs.
bool LauncherApp::OnInit() {
    wxFileConfig::Get()->SetAppName("DHFS");

    wxFrame* frame = new LauncherAppMainFrame(NULL);
    frame->Show(true);
    SetTopWindow(frame);

    // wxTaskBarIcon* tb = new wxTaskBarIcon();
    // auto img = new wxImage(32, 32, false);
    // img->Clear(128);
    // tb->SetIcon(*(new wxBitmapBundle(*(new wxBitmap(*img)))), "e");

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

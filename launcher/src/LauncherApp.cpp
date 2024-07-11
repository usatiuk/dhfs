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

#include "wx/taskbar.h"

IMPLEMENT_APP(LauncherApp)

// This is executed upon startup, like 'main()' in non-wxWidgets programs.
bool LauncherApp::OnInit() {
    wxFrame* frame = new MainFrame(_T("DHFS Launcher"), wxDefaultPosition);
    frame->CreateStatusBar();
    frame->SetStatusText(_T("Hello World"));
    frame->Show(true);
    SetTopWindow(frame);

    wxTaskBarIcon* tb = new wxTaskBarIcon();
    auto img = new wxImage(32, 32, false);
    img->Clear(128);
    tb->SetIcon(*(new wxBitmapBundle(*(new wxBitmap(*img)))), "e");

    return true;
}

BEGIN_EVENT_TABLE(MainFrame, wxFrame)
    EVT_BUTTON(BUTTON_Hello, MainFrame::OnExit) // Tell the OS to run MainFrame::OnExit when
END_EVENT_TABLE() // The button is pressed

MainFrame::MainFrame(const wxString& title, const wxPoint& pos)
    : wxFrame((wxFrame*) NULL, -1, title, pos) {
    Notebook = new wxNotebook(this, NOTEBOOK_Main);

    Panel = new wxPanel(Notebook);
    Panel2 = new wxPanel(Notebook);
    Notebook->AddPage(Panel, "Hello");
    Notebook->AddPage(Panel2, "Hello2");

    Panel->SetBackgroundColour(wxColour(0xFF0000));

    HelloWorld = new wxButton(Panel, BUTTON_Hello, _T("Hello World"),
                              // shows a button on this window
                              wxDefaultPosition, wxDefaultSize, 0); // with the text "hello World"
}

void MainFrame::OnExit(wxCommandEvent& event) {
    Close(TRUE);
}

//
// Created by Stepan Usatiuk on 11.07.2024.
//

#ifndef HELLOWORLDAPP_H
#define HELLOWORLDAPP_H

#include "DhfsGuiInstance.hpp"
#include "wx/wx.h"

#include "DhfsWxServer.hpp"

class wxSingleInstanceChecker;

// The HelloWorldApp class. This class shows a window
// containing a statusbar with the text "Hello World"
class LauncherApp : public wxApp {
public:
    virtual bool OnInit() override;

    virtual int OnExit() override;

    virtual bool OnExceptionInMainLoop() override;

#ifdef __APPLE__
    void MacReopenApp() override;
#endif

    void Open();

    void OnTopFrameClose(wxCloseEvent& event);

private:
    wxSingleInstanceChecker* m_checker = nullptr;
    DhfsWxServer m_server;
    void forwardEventToTopWindow(wxCommandEvent& event);

public:
    DhfsGuiInstance m_dhfsInstance{*this};
};

DECLARE_APP(LauncherApp)

#endif //HELLOWORLDAPP_H

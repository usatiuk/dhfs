//
// Created by Stepan Usatiuk on 11.07.2024.
//

#ifndef HELLOWORLDAPP_H
#define HELLOWORLDAPP_H

// The HelloWorldApp class. This class shows a window
// containing a statusbar with the text "Hello World"
class LauncherApp : public wxApp {
public:
    virtual bool OnInit();
};

class MainFrame : public wxFrame // MainFrame is the class for our window,
{
    // It contains the window and all objects in it
public:
    MainFrame(const wxString& title, const wxPoint& pos);

    wxButton* HelloWorld;
    wxNotebook* Notebook;
    wxPanel *Panel;
    wxPanel *Panel2;

    void OnExit(wxCommandEvent& event);

    DECLARE_EVENT_TABLE()
};

enum {
    BUTTON_Hello = wxID_HIGHEST + 1, // declares an id which will be used to call our button
    NOTEBOOK_Main = wxID_HIGHEST + 2 // declares an id which will be used to call our button
};

DECLARE_APP(LauncherApp)

#endif //HELLOWORLDAPP_H

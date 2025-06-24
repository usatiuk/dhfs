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

DECLARE_APP(LauncherApp)

#endif //HELLOWORLDAPP_H

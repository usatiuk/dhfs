#include "LauncherAppMainFrame.h"

#include <iostream>

LauncherAppMainFrame::LauncherAppMainFrame(wxWindow* parent)
    : MainFrame(parent) {
}

void LauncherAppMainFrame::OnStartStopButtonClick(wxCommandEvent& event) {
    std::cout << "Hi!" << std::endl;
    _dhfsInstance.start();
    // TODO: Implement OnStartStopButtonClick
}

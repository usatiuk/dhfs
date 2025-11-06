//
// Created by Stepan Usatiuk on 25.06.2025.
//

#include "DhfsGuiInstance.hpp"

#include "LauncherAppMainFrame.h"

wxDEFINE_EVENT(NEW_LINE_OUTPUT_EVENT, wxCommandEvent);
wxDEFINE_EVENT(DHFS_STATE_CHANGE_EVENT, wxCommandEvent);

DhfsGuiInstance::DhfsGuiInstance(wxEvtHandler& parent): _evtHandler(parent) {
}

void DhfsGuiInstance::OnStateChange() {
    wxCommandEvent* event = new wxCommandEvent(DHFS_STATE_CHANGE_EVENT);
    _evtHandler.QueueEvent(event);
}

void DhfsGuiInstance::OnRead(std::string s) {
    wxCommandEvent* event = new wxCommandEvent(NEW_LINE_OUTPUT_EVENT);
    event->SetString(std::move(s));
    _evtHandler.QueueEvent(event);
}

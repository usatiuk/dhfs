//
// Created by Stepan Usatiuk on 25.06.2025.
//

#include "DhfsGuiInstance.hpp"

#include "LauncherAppMainFrame.h"

DhfsGuiInstance::DhfsGuiInstance(LauncherAppMainFrame& parent): _parent(parent) {
}

void DhfsGuiInstance::OnStateChange() {
    wxCommandEvent* event = new wxCommandEvent(DHFS_STATE_CHANGE_EVENT, _parent.GetId());
    event->SetEventObject(&_parent);
    _parent.GetEventHandler()->QueueEvent(event);
}

void DhfsGuiInstance::OnRead(std::string s) {
    wxCommandEvent* event = new wxCommandEvent(NEW_LINE_OUTPUT_EVENT, _parent.GetId());
    event->SetEventObject(&_parent);
    event->SetString(std::move(s));
    _parent.GetEventHandler()->QueueEvent(event);
}

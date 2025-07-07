//
// Created by Stepan Usatiuk on 25.06.2025.
//

#ifndef DHFSGUIINSTANCE_HPP
#define DHFSGUIINSTANCE_HPP
#include "DhfsInstance.hpp"

wxDECLARE_EVENT(NEW_LINE_OUTPUT_EVENT, wxCommandEvent);
wxDECLARE_EVENT(DHFS_STATE_CHANGE_EVENT, wxCommandEvent);

class DhfsGuiInstance : public DhfsInstance {
public:
    DhfsGuiInstance(wxEvtHandler& parent);

    void OnStateChange() override;

    void OnRead(std::string s) override;

protected:
    wxEvtHandler& _evtHandler;
};


#endif //DHFSGUIINSTANCE_HPP

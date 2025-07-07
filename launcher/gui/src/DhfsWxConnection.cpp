//
// Created by Stepan Usatiuk on 29.06.2025.
//

#include "DhfsWxConnection.hpp"

#include <iostream>

#include "wx/app.h"
#include "LauncherApp.h"

DhfsWxConnection::DhfsWxConnection() : wxConnection() {
}

bool DhfsWxConnection::OnExec(const wxString& wx_uni_char_refs, const wxString& wx_uni_chars) {
    std::cout << "DhfsWxConnection::OnExec called with topic: " << wx_uni_char_refs << " and item: " << wx_uni_chars <<
            std::endl;

    wxGetApp().Open();
    return true;
}

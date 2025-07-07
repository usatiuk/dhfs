//
// Created by Stepan Usatiuk on 29.06.2025.
//

#ifndef DHFSWXCONNECTION_HPP
#define DHFSWXCONNECTION_HPP
#include "wx/ipc.h"


class DhfsWxConnection : public wxConnection {
public:
    DhfsWxConnection();

    bool OnExec(const wxString&, const wxString&) override;
};


#endif //DHFSWXCONNECTION_HPP

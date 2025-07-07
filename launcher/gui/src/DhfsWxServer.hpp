//
// Created by Stepan Usatiuk on 29.06.2025.
//

#ifndef DHFSWXSERVER_HPP
#define DHFSWXSERVER_HPP

#include "wx/ipc.h"

class DhfsWxServer : public wxServer {
public:
    wxConnectionBase* OnAcceptConnection(const wxString& topic) override;
};


#endif //DHFSWXSERVER_HPP

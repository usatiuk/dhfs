//
// Created by Stepan Usatiuk on 29.06.2025.
//

#include "DhfsWxServer.hpp"

#include "DhfsWxConnection.hpp"

wxConnectionBase* DhfsWxServer::OnAcceptConnection(const wxString& topic) {
    return new DhfsWxConnection();
}

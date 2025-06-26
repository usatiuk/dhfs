//
// Created by Stepan Usatiuk on 25.06.2025.
//

#include "DhfsWxProcess.hpp"

#include "DhfsInstance.hpp"

DhfsWxProcess::DhfsWxProcess(DhfsInstance& parent): wxProcess(wxPROCESS_REDIRECT), _instance(parent) {
}

void DhfsWxProcess::OnTerminate(int pid, int status) {
    _instance.OnTerminateInternal(pid, status);
}

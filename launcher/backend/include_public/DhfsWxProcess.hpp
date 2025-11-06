//
// Created by Stepan Usatiuk on 25.06.2025.
//

#ifndef DHFSWXPROCESS_HPP
#define DHFSWXPROCESS_HPP
#include <wx/process.h>


class DhfsInstance;

class DhfsWxProcess : public wxProcess {
public:
    DhfsWxProcess(DhfsInstance& parent);

protected:
    void OnTerminate(int pid, int status) override;

private:
    DhfsInstance& _instance;
};


#endif //DHFSWXPROCESS_HPP

//
// Created by stepus53 on 24.6.25.
//

#ifndef DHFSINSTANCE_HPP
#define DHFSINSTANCE_HPP

#include <wx/process.h>
#include <string>
#include <vector>
#include <thread>
#include<mutex>

#include "DhfsStartOptions.hpp"
#include "DhfsWxProcess.hpp"

enum class DhfsInstanceState {
    RUNNING,
    STOPPED,
};

class DhfsInstance {
public:
    DhfsInstance();

    virtual ~DhfsInstance();

    DhfsInstanceState state();

    void start(DhfsStartOptions start_options);

    void stop();

    virtual void OnTerminate(int pid, int status) = 0;

    virtual void OnRead(std::string s) = 0;

protected:
    std::unique_ptr<wxProcess> process = std::make_unique<DhfsWxProcess>(*this);

private:
    DhfsInstanceState _state = DhfsInstanceState::STOPPED;
    std::thread _readThread;
    std::mutex _mutex;
};


#endif //DHFSINSTANCE_HPP

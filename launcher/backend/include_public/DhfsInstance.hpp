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
    STARTING,
    RUNNING,
    STOPPING,
    STOPPED,
};

class DhfsInstance {
    friend DhfsWxProcess;

public:
    DhfsInstance();

    virtual ~DhfsInstance();

    DhfsInstanceState state();

    void start(DhfsStartOptions start_options);

    void stop();

protected:
    virtual void OnStateChange() = 0;

    virtual void OnRead(std::string s) = 0;

private:
    std::unique_ptr<DhfsWxProcess> process = std::make_unique<DhfsWxProcess>(*this);

    void OnTerminateInternal(int pid, int status);

    DhfsInstanceState _state = DhfsInstanceState::STOPPED;
    std::thread _readThread;
    std::thread _readThreadErr;
    std::mutex _mutex;
};


#endif //DHFSINSTANCE_HPP

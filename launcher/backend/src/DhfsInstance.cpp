//
// Created by stepus53 on 24.6.25.
//

#include "DhfsInstance.hpp"

#include <stdexcept>
#include <vector>

#include "Exception.h"

DhfsInstance::DhfsInstance() {
}

DhfsInstance::~DhfsInstance() {
    stop();
}

DhfsInstanceState DhfsInstance::state() {
    std::lock_guard<std::mutex> lock(_mutex);
    return _state;
}

void DhfsInstance::start(DhfsStartOptions options) {
    std::lock_guard<std::mutex> lock(_mutex);
    switch (_state) {
        case DhfsInstanceState::RUNNING:
        case DhfsInstanceState::STARTING:
        case DhfsInstanceState::STOPPING:
            return;
        case DhfsInstanceState::STOPPED:
            break;
        default:
            throw std::runtime_error("Unknown DhfsInstanceState");
    }

    _state = DhfsInstanceState::STARTING;
    OnStateChange();

    std::vector<char*> args;
    auto readyOptions = options.getOptions();
    for (const auto& option: readyOptions) {
        args.push_back(const_cast<char*>(option.c_str()));
    }

    long ret = wxExecute(args.data(), wxEXEC_ASYNC | wxEXEC_HIDE_CONSOLE | wxEXEC_MAKE_GROUP_LEADER, process.get(),
                         nullptr);
    if (ret == 0) {
        _state = DhfsInstanceState::STOPPED;
        throw Exception("Failed to start DHFS");
    }

    OnRead("Started! PID: " + std::to_string(process->GetPid()) + "\n");

    _readThread = std::thread([&]() {
        auto stream = process->GetInputStream();

        bool searching = true;
        std::string lastLine;

        while (!stream->Eof() || stream->CanRead()) {
            char buffer[1024];
            size_t bytesRead = stream->Read(buffer, sizeof(buffer) - 1).LastRead();
            if (bytesRead > 0) {
                buffer[bytesRead] = '\0';
                if (searching) {
                    for (size_t i = 0; i < bytesRead; i++) {
                        lastLine += buffer[i];
                        if (buffer[i] == '\n') {
                            if (lastLine.find("Listening on:") != std::string::npos) {
                                searching = false;
                                std::lock_guard<std::mutex> lock(_mutex);
                                if (_state == DhfsInstanceState::STARTING) {
                                    _state = DhfsInstanceState::RUNNING;
                                    OnStateChange();
                                }
                            }
                            lastLine = "";
                        }
                    }
                }
                OnRead(std::string(buffer));
            }
        }
    });
    _readThreadErr = std::thread([&]() {
        auto stream = process->GetErrorStream();
        while (!stream->Eof() || stream->CanRead()) {
            char buffer[1024];
            size_t bytesRead = stream->Read(buffer, sizeof(buffer) - 1).LastRead();
            if (bytesRead > 0) {
                buffer[bytesRead] = '\0';
                OnRead(std::string(buffer));
            }
        }
    });
}

void DhfsInstance::stop() {
    std::lock_guard<std::mutex> lock(_mutex);
    switch (_state) {
        case DhfsInstanceState::RUNNING:
        case DhfsInstanceState::STARTING:
            break;
        case DhfsInstanceState::STOPPED:
        case DhfsInstanceState::STOPPING:
            return;
        default:
            throw std::runtime_error("Unknown DhfsInstanceState");
    }

    _state = DhfsInstanceState::STOPPING;
    OnStateChange();

    int err = wxProcess::Kill(process->GetPid(), wxSIGTERM, wxKILL_CHILDREN);
    if (err != wxKILL_OK) {
        OnRead("Failed to stop DHFS: " + std::to_string(err) + "\n");
    }
}

void DhfsInstance::OnTerminateInternal(int pid, int status) {
    std::lock_guard<std::mutex> lock(_mutex);
    _state = DhfsInstanceState::STOPPED;

    _readThread.join();
    _readThreadErr.join();
    OnRead("Stopped!\n");
    OnStateChange();
}

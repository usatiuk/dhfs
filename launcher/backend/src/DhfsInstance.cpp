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
            return;
        case DhfsInstanceState::STOPPED:
            break;
        default:
            throw std::runtime_error("Unknown DhfsInstanceState");
    }
    _state = DhfsInstanceState::RUNNING;

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

    OnRead("Started! " + std::to_string(ret) + " PID: " + std::to_string(process->GetPid()) + "\n");

    _readThread = std::thread([&]() {
        auto stream = process->GetInputStream();
        while (!stream->Eof() || stream->CanRead()) {
            char buffer[1024];
            size_t bytesRead = stream->Read(buffer, sizeof(buffer) - 1).LastRead();
            if (bytesRead > 0) {
                buffer[bytesRead] = '\0'; // Null-terminate the string
                OnRead(std::string(buffer));
            } else if (bytesRead == 0) {
                break; // EOF reached
            }
        }
    });
    _readThreadErr = std::thread([&]() {
        auto stream = process->GetErrorStream();
        while (!stream->Eof() || stream->CanRead()) {
            char buffer[1024];
            size_t bytesRead = stream->Read(buffer, sizeof(buffer) - 1).LastRead();
            if (bytesRead > 0) {
                buffer[bytesRead] = '\0'; // Null-terminate the string
                OnRead(std::string(buffer));
            } else if (bytesRead == 0) {
                break; // EOF reached
            }
        }
    });
}

void DhfsInstance::stop() {
    std::lock_guard<std::mutex> lock(_mutex);
    switch (_state) {
        case DhfsInstanceState::RUNNING:
            break;
        case DhfsInstanceState::STOPPED:
            return;
        default:
            throw std::runtime_error("Unknown DhfsInstanceState");
    }

    _state = DhfsInstanceState::STOPPED;

    int err = wxProcess::Kill(process->GetPid(), wxSIGTERM, wxKILL_CHILDREN);
    _readThread.join();
    _readThreadErr.join();
    OnRead("Stopped!\n");
    if (err != wxKILL_OK) {
        OnRead("Failed to stop DHFS: " + std::to_string(err) + "\n");
    }
    OnTerminate(0, 0);
}

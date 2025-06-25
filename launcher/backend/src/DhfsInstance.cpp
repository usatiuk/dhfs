//
// Created by stepus53 on 24.6.25.
//

#include "DhfsInstance.hpp"

#include <stdexcept>
#include <vector>

#include "Exception.h"
#include "LibjvmWrapper.hpp"

DhfsInstance::DhfsInstance() {
}

DhfsInstance::~DhfsInstance() {
}

DhfsInstanceState DhfsInstance::state() {
    return _state;
}

void DhfsInstance::start(const std::string& mount_path, const std::vector<std::string>& extra_options) {
    switch (_state) {
        case DhfsInstanceState::RUNNING:
            return;
        case DhfsInstanceState::STOPPED:
            break;
        default:
            throw std::runtime_error("Unknown DhfsInstanceState");
    }
    _state = DhfsInstanceState::RUNNING;

    JavaVMInitArgs args;
    std::vector<JavaVMOption> options;
    for (const auto& option: extra_options) {
        options.emplace_back((char*) option.c_str(), nullptr);
    }
    std::string mount_option = "-Ddhfs.fuse.root=";
    mount_option += mount_path;
    options.emplace_back((char*) mount_option.c_str(), nullptr);
    args.version = JNI_VERSION_21;
    args.nOptions = options.size();
    args.options = options.data();
    args.ignoreUnrecognized = false;

    LibjvmWrapper::instance().get_JNI_CreateJavaVM()(&_jvm, (void**) &_env, &args);
}

void DhfsInstance::stop() {
    switch (_state) {
        case DhfsInstanceState::RUNNING:
            break;
        case DhfsInstanceState::STOPPED:
            return;
        default:
            throw std::runtime_error("Unknown DhfsInstanceState");
    }

    if (_jvm == nullptr)
        throw Exception("JVM not running");

    JNIEnv* env;
    _jvm->AttachCurrentThread((void**) &env, nullptr);
    _jvm->DestroyJavaVM();
    _jvm = nullptr;
    _state = DhfsInstanceState::STOPPED;
}

//
// Created by stepus53 on 24.6.25.
//

#include "DhfsInstance.hpp"

#include <stdexcept>
#include <vector>

#include "LibjvmWrapper.hpp"

DhfsInstance::DhfsInstance() {
}

DhfsInstance::~DhfsInstance() {
}

DhfsInstanceState DhfsInstance::state() {
}

void DhfsInstance::start() {
    switch (_state) {
        case DhfsInstanceState::RUNNING:
            return;
        case DhfsInstanceState::STOPPED:
            break;
        default:
            throw std::runtime_error("Unknown DhfsInstanceState");
    }

    JavaVMInitArgs args;
    std::vector<JavaVMOption> options;
    args.version = JNI_VERSION_21;
    args.nOptions = 0;
    args.options = options.data();
    args.ignoreUnrecognized = false;

    LibjvmWrapper::instance().WJNI_CreateJavaVM(&_jvm, (void**) &_env, &args);
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
}

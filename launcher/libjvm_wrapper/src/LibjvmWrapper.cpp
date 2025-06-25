//
// Created by Stepan Usatiuk on 24.06.2025.
//

#include "LibjvmWrapper.hpp"
#include <dlfcn.h>
#include <jni.h>

#include "Exception.h"

LibjvmWrapper& LibjvmWrapper::instance() {
    static LibjvmWrapper instance;
    return instance;
}

LibjvmWrapper::LibjvmWrapper() {
}

void LibjvmWrapper::load() {
    if (_java_home == "")
        throw Exception("Java home not set");
    if (_lib_handle != nullptr)
        throw Exception("load() called when already loaded");

    std::string javaHomeAppended;
    javaHomeAppended = _java_home + "/lib/server/libjvm.so";

    _lib_handle = dlopen(javaHomeAppended.c_str(), RTLD_NOW | RTLD_GLOBAL);
    if (_lib_handle == nullptr)
        throw Exception(dlerror());
    WJNI_CreateJavaVM = reinterpret_cast<decltype(WJNI_CreateJavaVM)>(
        dlsym(_lib_handle, "JNI_CreateJavaVM"));
    if (WJNI_CreateJavaVM == nullptr)
        throw Exception(dlerror());
}

void LibjvmWrapper::unload() {
    if (_lib_handle != nullptr) {
        dlclose(_lib_handle);
        _lib_handle = nullptr;
        WJNI_CreateJavaVM = nullptr;
    }
}

decltype(JNI_CreateJavaVM)* LibjvmWrapper::get_JNI_CreateJavaVM() {
    if (WJNI_CreateJavaVM == nullptr) {
        load();
    }
    return WJNI_CreateJavaVM;
}

LibjvmWrapper::~LibjvmWrapper() {
    unload();
}

void LibjvmWrapper::setJavaHome(const std::string& javaHome) {
    unload();
    _java_home = javaHome;
}

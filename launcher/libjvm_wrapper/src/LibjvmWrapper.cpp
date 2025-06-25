//
// Created by Stepan Usatiuk on 24.06.2025.
//

#include "LibjvmWrapper.hpp"
#include <dlfcn.h>
#include <jni.h>

#include "Exception.h"

static constexpr auto LIBJVM_PATH =
        "/Library/Java/JavaVirtualMachines/zulu-21.jdk/Contents/Home/lib/server/libjvm.dylib";

LibjvmWrapper& LibjvmWrapper::instance() {
    static LibjvmWrapper instance;
    return instance;
}

LibjvmWrapper::LibjvmWrapper() {
    _lib_handle = dlopen(LIBJVM_PATH, RTLD_NOW | RTLD_GLOBAL);
    if (_lib_handle == nullptr)
        throw Exception(dlerror());
    WJNI_CreateJavaVM = reinterpret_cast<decltype(WJNI_CreateJavaVM)>(
        dlsym(_lib_handle, "JNI_CreateJavaVM"));
    if (WJNI_CreateJavaVM == nullptr)
        throw Exception(dlerror());
}

LibjvmWrapper::~LibjvmWrapper() {
    dlclose(_lib_handle);
}

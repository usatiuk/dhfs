//
// Created by Stepan Usatiuk on 24.06.2025.
//

#ifndef LIBJVMWRAPPER_HPP
#define LIBJVMWRAPPER_HPP

#include <jni.h>
#include <string>

class LibjvmWrapper {
public:
    static LibjvmWrapper& instance();

    void setJavaHome(const std::string& javaHome);

    decltype(JNI_CreateJavaVM)* get_JNI_CreateJavaVM();

private:
    void load();

    void unload();

    LibjvmWrapper();

    ~LibjvmWrapper();

    void* _lib_handle = nullptr;
    decltype(JNI_CreateJavaVM)* WJNI_CreateJavaVM = nullptr;
    std::string _java_home;
};


#endif //LIBJVMWRAPPER_HPP

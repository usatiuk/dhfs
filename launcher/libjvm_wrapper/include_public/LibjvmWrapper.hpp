//
// Created by Stepan Usatiuk on 24.06.2025.
//

#ifndef LIBJVMWRAPPER_HPP
#define LIBJVMWRAPPER_HPP
#include <jni.h>


class LibjvmWrapper {
public:
    static LibjvmWrapper& instance();

    decltype(JNI_CreateJavaVM)* JNI_CreateJavaVM;

private:
    LibjvmWrapper();

    ~LibjvmWrapper();

    void* _lib_handle;
};


#endif //LIBJVMWRAPPER_HPP

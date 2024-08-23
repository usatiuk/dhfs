#include <cstdio>

#include "com_usatiuk_dhfs_supportlib_DhfsSupportNative.h"

extern "C" {
void Java_com_usatiuk_dhfs_supportlib_DhfsSupportNative_hello(JNIEnv* env, jclass klass) {
    printf("Hello, World!\n");
}
}

#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <cassert>

#include "com_usatiuk_dhfs_supportlib_DhfsSupportNative.h"

#include "Utils.h"
#include "MemoryHelpers.h"

extern "C" {
JNIEXPORT jlong JNICALL Java_com_usatiuk_dhfs_supportlib_DhfsSupportNative_allocateUninitializedByteBuffer
(JNIEnv* env, jclass klass, jobjectArray bb, jint size) {
    if (size < 0) {
        env->ThrowNew(env->FindClass("java/lang/IllegalArgumentException"), "Size less than 0?");
        return 0;
    }

    size_t checked_size = checked_cast<size_t>(size);

    void* buf;
    if (checked_size < MemoryHelpers::get_page_size())
        buf = malloc(checked_size);
    else
        buf = std::aligned_alloc(MemoryHelpers::get_page_size(),
                                 align_up(checked_size, MemoryHelpers::get_page_size()));

    if (buf == nullptr) {
        env->ThrowNew(env->FindClass("java/lang/OutOfMemoryError"), "Buffer memory allocation failed");
        return 0;
    }

    env->SetObjectArrayElement(bb, 0, env->NewDirectByteBuffer(buf, checked_cast<jlong>(checked_size)));

    jlong token = checked_cast<jlong>((uintptr_t) buf);
    return token;
}

JNIEXPORT void JNICALL Java_com_usatiuk_dhfs_supportlib_DhfsSupportNative_releaseByteBuffer
(JNIEnv* env, jclass klass, jlong token) {
    const auto addr = checked_cast<uintptr_t>(token);

    if (addr == 0) {
        env->ThrowNew(env->FindClass("java/lang/IllegalArgumentException"), "Trying to free null pointer");
        return;
    }

    free((void*) addr);
}

JNIEXPORT jint JNICALL Java_com_usatiuk_dhfs_supportlib_DhfsSupportNative_getPageSizeInternal
(JNIEnv*, jclass) {
    return checked_cast<jint>(MemoryHelpers::get_page_size());
}
}

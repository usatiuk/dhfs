#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <cassert>

#include <unistd.h>

#include "com_usatiuk_dhfs_supportlib_DhfsSupportNative.h"

long get_page_size() {
    static const long PAGE_SIZE = sysconf(_SC_PAGESIZE);
    return PAGE_SIZE;
}

constexpr uintptr_t align_up(uintptr_t what, size_t alignment) {
    assert(__builtin_popcount(alignment) == 1);
    const uintptr_t mask = alignment - 1;
    if (what & mask) {
        return (what & ~mask) + alignment;
    }
    return what;
}

extern "C" {
JNIEXPORT void JNICALL Java_com_usatiuk_dhfs_supportlib_DhfsSupportNative_hello(JNIEnv* env, jclass klass) {
    printf("Hello, World!\n");
}

JNIEXPORT jlong JNICALL Java_com_usatiuk_dhfs_supportlib_DhfsSupportNative_allocateUninitializedByteBuffer
(JNIEnv* env, jclass klass, jobjectArray bb, jint size) {
    void* buf;
    if (size < get_page_size())
        buf = malloc(size);
    else
        buf = std::aligned_alloc(get_page_size(), align_up(size, get_page_size()));

    if (buf == nullptr) {
        env->ThrowNew(env->FindClass("java/lang/OutOfMemoryError"), "Buffer memory allocation failed");
        return 0;
    }

    env->SetObjectArrayElement(bb, 0, env->NewDirectByteBuffer(buf, size));

    jlong token = static_cast<jlong>((uintptr_t) buf);
    assert(token == (uintptr_t)buf);
    return token;
}

JNIEXPORT void JNICALL Java_com_usatiuk_dhfs_supportlib_DhfsSupportNative_dropByteBuffer
(JNIEnv* env, jclass klass, jlong token) {
    uintptr_t addr = static_cast<uintptr_t>(token);
    assert(addr == token);

    if (addr == 0) {
        env->ThrowNew(env->FindClass("java/lang/IllegalArgumentException"), "Trying to free null pointer");
        return;
    }

    free((void*) addr);
}
}

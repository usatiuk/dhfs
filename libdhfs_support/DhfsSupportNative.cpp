#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <cassert>

#include <unistd.h>

#include "com_usatiuk_dhfs_supportlib_DhfsSupportNative.h"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-conversion"
#pragma GCC diagnostic ignored "-Wsign-compare"

template<typename To, typename From>
constexpr To checked_cast(const From& f) {
    To result = static_cast<To>(f);
    assert(f == result);
    return result;
}

#pragma GCC diagnostic pop

unsigned int get_page_size() {
    static const auto PAGE_SIZE = checked_cast<unsigned int>(sysconf(_SC_PAGESIZE));
    return PAGE_SIZE;
}

template<typename T, typename A>
T align_up(T what, A alignment) {
    assert(__builtin_popcount(alignment) == 1);

    const T mask = checked_cast<T>(alignment - 1);

    T ret;

    if (what & mask)
        ret = (what + mask) & ~mask;
    else
        ret = what;

    assert((ret & mask) == 0);

    return ret;
}

extern "C" {
JNIEXPORT void JNICALL Java_com_usatiuk_dhfs_supportlib_DhfsSupportNative_hello(JNIEnv* env, jclass klass) {
    printf("Hello, World!\n");
}

JNIEXPORT jlong JNICALL Java_com_usatiuk_dhfs_supportlib_DhfsSupportNative_allocateUninitializedByteBuffer
(JNIEnv* env, jclass klass, jobjectArray bb, jint size) {
    if (size < 0) {
        env->ThrowNew(env->FindClass("java/lang/IllegalArgumentException"), "Size less than 0?");
        return 0;
    }

    size_t checked_size = checked_cast<size_t>(size);

    void* buf;
    if (checked_size < get_page_size())
        buf = malloc(checked_size);
    else
        buf = std::aligned_alloc(get_page_size(), align_up(checked_size, get_page_size()));

    if (buf == nullptr) {
        env->ThrowNew(env->FindClass("java/lang/OutOfMemoryError"), "Buffer memory allocation failed");
        return 0;
    }

    env->SetObjectArrayElement(bb, 0, env->NewDirectByteBuffer(buf, checked_cast<jlong>(checked_size)));

    jlong token = checked_cast<jlong>((uintptr_t) buf);
    return token;
}

JNIEXPORT void JNICALL Java_com_usatiuk_dhfs_supportlib_DhfsSupportNative_dropByteBuffer
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
    return checked_cast<jint>(get_page_size());
}
}

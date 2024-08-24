//
// Created by stepus53 on 24.8.24.
//

#ifndef UTILS_H
#define UTILS_H

#include <cassert>

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

#endif //UTILS_H

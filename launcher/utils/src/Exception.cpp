//
// Created by Stepan Usatiuk on 01.05.2023.
//

#include "Exception.h"

#include <execinfo.h>
#include <sstream>

#include <openssl/err.h>

Exception::Exception(const std::string& text) : runtime_error(text + "\n" + getStacktrace()) {
}

Exception::Exception(const char* text) : runtime_error(std::string(text) + "\n" + getStacktrace()) {
}

// Based on: https://www.gnu.org/software/libc/manual/html_node/Backtraces.html
std::string Exception::getStacktrace() {
    std::vector<void*> functions(50);
    char** strings;
    int n;

    n = backtrace(functions.data(), 50);
    strings = backtrace_symbols(functions.data(), n);

    std::stringstream out;

    if (strings != nullptr) {
        out << "Stacktrace:" << std::endl;
        for (int i = 0; i < n; i++)
            out << strings[i] << std::endl;
    }

    free(strings);
    return out.str();
}

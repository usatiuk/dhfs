//
// Created by Stepan Usatiuk on 01.05.2023.
//

#ifndef SEMBACKUP_EXCEPTION_H
#define SEMBACKUP_EXCEPTION_H

#include <cstring>
#include <stdexcept>
#include <string>
#include <vector>

/// Custom exception class that uses execinfo to append a stacktrace to the exception message
class Exception : public std::runtime_error {
public:
    Exception(const std::string& text);
    Exception(const char* text);

private:
    /// Static function to get the current stacktrace
    static std::string getStacktrace();
};

class ErrnoException : public Exception {
public:
    ErrnoException(const std::string& text) : Exception(text + " " + std::strerror(errno)) {}
    ErrnoException(const char* text) : Exception(std::string(text) + " " + std::strerror(errno)) {}
};

#endif // SEMBACKUP_EXCEPTION_H

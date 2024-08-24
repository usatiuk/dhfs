//
// Created by stepus53 on 24.8.24.
//

#include "MemoryHelpers.h"

#include <unistd.h>

#include "Utils.h"

using namespace MemoryHelpers;

unsigned int get_page_size() {
    static const auto PAGE_SIZE = checked_cast<unsigned int>(sysconf(_SC_PAGESIZE));
    return PAGE_SIZE;
}
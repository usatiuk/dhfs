package com.usatiuk.dhfs.objects.transaction;

public enum LockingStrategy {
    READ,                 // Read only, no writes allowed, blocks writers
    READ_SERIALIZABLE,    // Exception if object was written to after transaction start
    OPTIMISTIC,           // Optimistic write, no blocking other possible writers
    WRITE,                // Write lock, blocks all other writers
    WRITE_SERIALIZABLE    // Exception if object was written to after transaction start
}

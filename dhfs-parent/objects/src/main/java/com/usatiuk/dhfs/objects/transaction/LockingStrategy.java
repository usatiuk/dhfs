package com.usatiuk.dhfs.objects.transaction;

public enum LockingStrategy {
    READ_ONLY,    // Read only, no writes allowed, blocks writers
    OPTIMISTIC,   // Optimistic write, no blocking other possible writers
    WRITE         // Write lock, blocks all other writers
}

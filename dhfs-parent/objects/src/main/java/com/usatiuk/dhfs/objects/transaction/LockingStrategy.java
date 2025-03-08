package com.usatiuk.dhfs.objects.transaction;

public enum LockingStrategy {
    OPTIMISTIC,           // Optimistic write, no blocking other possible writers/readers
    WRITE,                // Write lock, blocks all other writers
}

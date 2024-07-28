package com.usatiuk.dhfs.objects.jrepository;

import jakarta.inject.Singleton;

@Singleton
public class JObjectSizeEstimator {
    public long estimateObjectSize(JObjectData d) {
        if (d == null) return 1024; // Assume metadata etc takes up something
        else return d.estimateSize() + 1024;
    }
}

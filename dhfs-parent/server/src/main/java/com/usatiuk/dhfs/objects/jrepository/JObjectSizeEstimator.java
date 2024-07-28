package com.usatiuk.dhfs.objects.jrepository;

import jakarta.inject.Singleton;

@Singleton
public class JObjectSizeEstimator {
    public long estimateObjectSize(JObjectData d) {
        if (d == null) return 200; // Assume metadata etc takes up something
        else return d.estimateSize();
    }
}

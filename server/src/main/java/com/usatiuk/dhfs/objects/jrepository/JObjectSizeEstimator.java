package com.usatiuk.dhfs.objects.jrepository;

import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class JObjectSizeEstimator {
    public long estimateObjectSize(JObjectData d) {
        if (d == null) return 200; // Assume metadata etc takes up something
        else return d.estimateSize();
    }
}

package com.usatiuk.dhfs.objects;

import org.pcollections.PCollection;

import java.util.Collection;
import java.util.List;

public interface JDataRefcounted extends JData {
    PCollection<JObjectKey> refsFrom();

    JDataRefcounted withRefsFrom(PCollection<JObjectKey> refs);

    boolean frozen();

    JDataRefcounted withFrozen(boolean frozen);

    default Collection<JObjectKey> collectRefsTo() {
        return List.of();
    }
}

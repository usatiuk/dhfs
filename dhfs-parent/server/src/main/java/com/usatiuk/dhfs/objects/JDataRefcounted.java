package com.usatiuk.dhfs.objects;

import org.pcollections.PCollection;

import java.util.Collection;
import java.util.List;

public interface JDataRefcounted extends JData {
    PCollection<JDataRef> refsFrom();

    JDataRefcounted withRefsFrom(PCollection<JDataRef> refs);

    boolean frozen();

    JDataRefcounted withFrozen(boolean frozen);

    default Collection<JDataRef> collectRefsTo() {
        return List.of();
    }
}

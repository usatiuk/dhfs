package com.usatiuk.dhfs;

import com.usatiuk.dhfs.objects.JData;
import com.usatiuk.dhfs.objects.JObjectKey;
import org.pcollections.PCollection;

import java.util.Collection;
import java.util.List;

public interface JDataRefcounted extends JData {
    PCollection<JDataRef> refsFrom();

    JDataRefcounted withRefsFrom(PCollection<JDataRef> refs);

    boolean frozen();

    JDataRefcounted withFrozen(boolean frozen);

    default Collection<JObjectKey> collectRefsTo() {
        return List.of();
    }
}

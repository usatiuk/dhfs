package com.usatiuk.dhfs.testobjs;

import com.usatiuk.dhfs.JDataRef;
import com.usatiuk.dhfs.JDataRefcounted;
import com.usatiuk.objects.JObjectKey;
import org.pcollections.HashTreePSet;
import org.pcollections.PCollection;

import java.util.Collection;

public record TestRefcount(JObjectKey key, PCollection<JDataRef> refsFrom, boolean frozen,
                           PCollection<JObjectKey> kids) implements JDataRefcounted {

    public TestRefcount(JObjectKey key) {
        this(key, HashTreePSet.empty(), false, HashTreePSet.empty());
    }

    @Override
    public TestRefcount withRefsFrom(PCollection<JDataRef> refs) {
        return new TestRefcount(key, refs, frozen, kids);
    }

    @Override
    public TestRefcount withFrozen(boolean frozen) {
        return new TestRefcount(key, refsFrom, frozen, kids);
    }

    public TestRefcount withKids(PCollection<JObjectKey> kids) {
        return new TestRefcount(key, refsFrom, frozen, kids);
    }

    @Override
    public Collection<JObjectKey> collectRefsTo() {
        return kids;
    }
}

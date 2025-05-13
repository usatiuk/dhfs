package com.usatiuk.dhfs.refcount;

import com.usatiuk.objects.JData;
import com.usatiuk.objects.JObjectKey;
import org.pcollections.PCollection;

import java.util.Collection;
import java.util.List;

/**
 * Interface for a reference counted object
 */
public interface JDataRefcounted extends JData {
    /**
     * Returns list of incoming references to this object.
     *
     * @return list of incoming references
     */
    PCollection<JDataRef> refsFrom();

    /**
     * Create a copy of this object with the given list of incoming references.
     *
     * @param refs list of incoming references
     * @return copy of this object with the given list of incoming references
     */
    JDataRefcounted withRefsFrom(PCollection<JDataRef> refs);

    /**
     * Returns whether this object is frozen or not.
     * A frozen object cannot be garbage collected.
     *
     * @return true if this object is frozen, false otherwise
     */
    boolean frozen();

    /**
     * Create a copy of this object with the given frozen state.
     *
     * @param frozen true if this object should be frozen, false otherwise
     * @return copy of this object with the given frozen state
     */
    JDataRefcounted withFrozen(boolean frozen);

    /**
     * Collect outgoing references to other objects.
     *
     * @return list of outgoing references
     */
    default Collection<JObjectKey> collectRefsTo() {
        return List.of();
    }
}

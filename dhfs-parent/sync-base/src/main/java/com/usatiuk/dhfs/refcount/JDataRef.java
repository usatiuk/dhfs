package com.usatiuk.dhfs.refcount;

import com.usatiuk.objects.JObjectKey;

import java.io.Serializable;

/**
 * JDataRefs are used to store additional metadata about incoming references to objects for reference counting.
 */
public interface JDataRef extends Comparable<JDataRef>, Serializable {
    JObjectKey obj();
}

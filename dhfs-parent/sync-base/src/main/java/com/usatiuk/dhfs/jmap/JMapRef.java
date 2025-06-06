package com.usatiuk.dhfs.jmap;

import com.usatiuk.dhfs.refcount.JDataNormalRef;
import com.usatiuk.dhfs.refcount.JDataRef;
import com.usatiuk.objects.JObjectKey;

import java.util.Comparator;

/**
 * A reference from a JMap object to some object.
 * This is used to get the parent object from the reference.
 * @param holder the object that holds the map
 * @param mapKey the key in the map
 */
public record JMapRef(JObjectKey holder, JMapKey mapKey) implements JDataRef {
    @Override
    public JObjectKey obj() {
        return holder;
    }

    @Override
    public int compareTo(JDataRef o) {
        if (o instanceof JMapRef mr) {
            return Comparator.comparing(JMapRef::obj).thenComparing(JMapRef::mapKey).compare(this, mr);
        } else if (o instanceof JDataNormalRef) {
            // TODO: Prettier?
            return 1;
        } else {
            throw new IllegalArgumentException("Unknown type of JDataRef");
        }
    }
}

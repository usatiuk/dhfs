package com.usatiuk.dhfs.refcount;

import com.usatiuk.dhfs.jmap.JMapRef;
import com.usatiuk.objects.JObjectKey;

/**
 * Good old boring object reference.
 * @param obj the object that is the source of the reference
 */
public record JDataNormalRef(JObjectKey obj) implements JDataRef {
    @Override
    public int compareTo(JDataRef o) {
        if (o instanceof JDataNormalRef) {
            return obj.compareTo(((JDataNormalRef) o).obj);
        } else if (o instanceof JMapRef) {
            // TODO: Prettier?
            return -1;
        } else {
            throw new IllegalArgumentException("Unknown type of JDataRef");
        }
    }
}

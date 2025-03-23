package com.usatiuk.dhfs.objects;

import com.usatiuk.dhfs.objects.jmap.JMapRef;

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

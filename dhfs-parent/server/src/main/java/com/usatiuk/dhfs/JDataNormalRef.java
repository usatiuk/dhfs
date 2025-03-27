package com.usatiuk.dhfs;

import com.usatiuk.dhfs.jmap.JMapRef;
import com.usatiuk.dhfs.objects.JObjectKey;

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

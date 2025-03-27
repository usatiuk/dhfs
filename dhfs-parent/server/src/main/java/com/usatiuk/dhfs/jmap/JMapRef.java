package com.usatiuk.dhfs.jmap;

import com.usatiuk.dhfs.JDataNormalRef;
import com.usatiuk.dhfs.JDataRef;
import com.usatiuk.dhfs.objects.JObjectKey;

import java.util.Comparator;

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

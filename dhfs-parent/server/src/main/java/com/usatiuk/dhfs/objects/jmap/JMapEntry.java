package com.usatiuk.dhfs.objects.jmap;

import com.usatiuk.dhfs.objects.JData;
import com.usatiuk.dhfs.objects.JObjectKey;

public record JMapEntry<K extends JMapKey & Comparable<K>>(JObjectKey holder,
                                                           K selfKey,
                                                           JObjectKey ref) implements JData {
    @Override
    public JObjectKey key() {
        return JMapHelper.makeKey(holder, selfKey);
    }
}

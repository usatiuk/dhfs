package com.usatiuk.dhfs.jmap;

import com.usatiuk.objects.JData;
import com.usatiuk.objects.JObjectKey;

public record JMapEntry<K extends JMapKey>(JObjectKey key,
                                           JObjectKey holder,
                                           K selfKey,
                                           JObjectKey ref) implements JData {
    public JMapEntry(JObjectKey holder, K selfKey, JObjectKey ref) {
        this(JMapHelper.makeKey(holder, selfKey), holder, selfKey, ref);
    }
}

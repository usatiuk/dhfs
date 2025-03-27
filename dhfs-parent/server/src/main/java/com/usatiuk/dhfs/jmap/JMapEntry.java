package com.usatiuk.dhfs.jmap;

import com.usatiuk.objects.JData;
import com.usatiuk.objects.JObjectKey;

public record JMapEntry<K extends JMapKey>(JObjectKey holder,
                                           K selfKey,
                                           JObjectKey ref) implements JData {
    @Override
    public JObjectKey key() {
        return JMapHelper.makeKey(holder, selfKey);
    }
}

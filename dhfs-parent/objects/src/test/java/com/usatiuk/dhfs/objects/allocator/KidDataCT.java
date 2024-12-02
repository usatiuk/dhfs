package com.usatiuk.dhfs.objects.allocator;

import com.usatiuk.dhfs.objects.JObjectKey;
import com.usatiuk.dhfs.objects.data.Kid;
import lombok.Getter;

public class KidDataCT extends ChangeTrackerBase<Kid> implements Kid {
    private final JObjectKey _key;

    @Getter
    private String _name;

    @Override
    public void setName(String name) {
        _name = name;
        onChange();
    }

    public KidDataCT(Kid normal) {
        _key = normal.getKey();
        _name = normal.getName();
    }

    @Override
    public JObjectKey getKey() {
        return _key;
    }

    @Override
    public Kid wrapped() {
        return this;
    }
}

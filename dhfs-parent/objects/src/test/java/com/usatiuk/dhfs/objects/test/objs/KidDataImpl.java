package com.usatiuk.dhfs.objects.test.objs;

import com.usatiuk.dhfs.objects.JObjectKey;

public class KidDataImpl extends TestData implements KidData {
    private String _name;

    public KidDataImpl(long version, JObjectKey key, String name) {
        super(version, key);
        _name = name;
    }

    @Override
    public String getName() {
        return _name;
    }

    @Override
    public void setName(String name) {
        _name = name;
        onChanged();
    }

    @Override
    public KidDataImpl copy() {
        return new KidDataImpl(isChanged() ? getVersion() + 1 : getVersion(), getKey(), _name);
    }
}

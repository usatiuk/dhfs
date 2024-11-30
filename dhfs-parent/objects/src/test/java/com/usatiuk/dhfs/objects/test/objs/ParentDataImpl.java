package com.usatiuk.dhfs.objects.test.objs;

import com.usatiuk.dhfs.objects.JObjectKey;

public class ParentDataImpl extends TestData implements ParentData {
    private String _name;
    private JObjectKey _kidKey;

    public ParentDataImpl(long version, JObjectKey key, String name, JObjectKey kidKey) {
        super(version, key);
        _name = name;
        _kidKey = kidKey;
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
    public JObjectKey getKidKey() {
        return _kidKey;
    }

    @Override
    public void setKidKey(JObjectKey kid) {
        _kidKey = kid;
        onChanged();
    }

    @Override
    public ParentDataImpl copy() {
        return new ParentDataImpl(isChanged() ? getVersion() + 1 : getVersion(), getKey(), _name, _kidKey);
    }
}

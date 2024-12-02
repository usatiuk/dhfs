package com.usatiuk.dhfs.objects.allocator;

import com.usatiuk.objects.common.JObjectKey;
import com.usatiuk.dhfs.objects.data.Parent;
import lombok.Getter;

public class ParentDataCT extends ChangeTrackerBase<Parent> implements Parent {
    @Getter
    private JObjectKey _name;
    @Getter
    private JObjectKey _kidKey;
    @Getter
    private String _lastName;

    public void setKidKey(JObjectKey key) {
        _kidKey = key;
        onChange();
    }

    public void setLastName(String lastName) {
        _lastName = lastName;
        onChange();
    }

    public ParentDataCT(Parent normal) {
        _name = normal.getKey();
        _kidKey = normal.getKidKey();
        _lastName = normal.getLastName();
    }

    @Override
    public JObjectKey getKey() {
        return _name;
    }

    @Override
    public Parent wrapped() {
        return this;
    }
}

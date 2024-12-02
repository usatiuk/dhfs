package com.usatiuk.dhfs.objects.allocator;

import com.usatiuk.dhfs.objects.JObjectKey;
import com.usatiuk.dhfs.objects.data.Parent;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

public class ParentDataNormal implements Parent, Serializable {
    @Getter
    private JObjectKey _name;
    @Getter
    @Setter
    private JObjectKey _kidKey;
    @Getter
    @Setter
    private String _lastName;

    public ParentDataNormal(JObjectKey name) {
        _name = name;
    }

    @Override
    public JObjectKey getKey() {
        return _name;
    }

}

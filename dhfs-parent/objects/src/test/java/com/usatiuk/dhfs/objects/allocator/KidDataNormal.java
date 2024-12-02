package com.usatiuk.dhfs.objects.allocator;

import com.usatiuk.objects.common.JObjectKey;
import com.usatiuk.dhfs.objects.data.Kid;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

public class KidDataNormal implements Kid, Serializable {
    private final JObjectKey _key;

    @Getter
    @Setter
    private String _name;

    public KidDataNormal(JObjectKey key) {_key = key;}

    @Override
    public JObjectKey getKey() {
        return _key;
    }
}

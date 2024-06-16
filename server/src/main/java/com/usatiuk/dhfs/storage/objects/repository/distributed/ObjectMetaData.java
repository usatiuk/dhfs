package com.usatiuk.dhfs.storage.objects.repository.distributed;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class ObjectMetaData implements Serializable {
    public ObjectMetaData(String name, Boolean assumeUnique) {
        _name = name;
        _assumeUnique = assumeUnique;
    }

    @Getter
    final String _name;

    @Getter
    @Setter
    long _mtime;

    @Getter
    final Boolean _assumeUnique;

    @Getter
    final List<String> _remoteCopies = new ArrayList<>();
}

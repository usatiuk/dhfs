package com.usatiuk.dhfs.storage.objects.repository.distributed;

import lombok.Getter;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class ObjectIndexData implements Serializable {
    @Getter
    private final Map<String, ObjectMeta> _objectMetaMap = new HashMap<>();
}

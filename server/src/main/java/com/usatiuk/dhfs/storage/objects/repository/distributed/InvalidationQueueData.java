package com.usatiuk.dhfs.storage.objects.repository.distributed;

import lombok.Getter;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

public class InvalidationQueueData {
    @Getter
    private Map<String, Set<String>> _hostToInvObj = new LinkedHashMap<>();

    public Set<String> getSetForHost(String host) {
        return _hostToInvObj.computeIfAbsent(host, k -> new LinkedHashSet<>());
    }

    public Map<String, Set<String>> pullAll() {
        var ret = _hostToInvObj;
        _hostToInvObj = new LinkedHashMap<>();
        return ret;
    }
}

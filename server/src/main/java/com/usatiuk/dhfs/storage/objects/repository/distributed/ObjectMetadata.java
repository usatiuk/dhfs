package com.usatiuk.dhfs.storage.objects.repository.distributed;

import com.usatiuk.dhfs.objects.repository.distributed.ObjectChangelog;
import com.usatiuk.dhfs.objects.repository.distributed.ObjectChangelogEntry;
import com.usatiuk.dhfs.objects.repository.distributed.ObjectHeader;
import com.usatiuk.dhfs.storage.objects.jrepository.JObjectData;
import lombok.Getter;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

public class ObjectMetadata implements Serializable {
    public ObjectMetadata(String name, String conflictResolver, Class<? extends JObjectData> type) {
        _name = name;
        _conflictResolver = conflictResolver;
        _type = type;
    }

    @Getter
    private final String _name;

    @Getter
    private final String _conflictResolver;

    @Getter
    private final Class<? extends JObjectData> _type;

    @Getter
    private final Map<String, Long> _remoteCopies = new LinkedHashMap<>();

    @Getter
    private final Map<String, Long> _changelog = new LinkedHashMap<>();

    public Long getOurVersion() {
        return _changelog.values().stream().reduce(0L, Long::sum);
    }

    public Long getBestVersion() {
        if (_remoteCopies.isEmpty()) return getOurVersion();
        return Math.max(getOurVersion(), _remoteCopies.values().stream().max(Long::compareTo).get());
    }

    public ObjectChangelog toRpcChangelog() {
        var changelogBuilder = ObjectChangelog.newBuilder();
        for (var m : getChangelog().entrySet()) {
            changelogBuilder.addEntries(ObjectChangelogEntry.newBuilder().setHost(m.getKey()).setVersion(m.getValue()).build());
        }
        return changelogBuilder.build();
    }

    public ObjectHeader toRpcHeader() {
        var headerBuilder = ObjectHeader.newBuilder().setName(getName());
        headerBuilder.setConflictResolver(getConflictResolver());
        headerBuilder.setChangelog(toRpcChangelog());
        return headerBuilder.build();
    }
}

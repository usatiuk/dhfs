package com.usatiuk.dhfs.storage.objects.repository.distributed;

import com.usatiuk.dhfs.objects.repository.distributed.ObjectChangelog;
import com.usatiuk.dhfs.objects.repository.distributed.ObjectChangelogEntry;
import com.usatiuk.dhfs.objects.repository.distributed.ObjectHeader;
import lombok.Getter;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

public class ObjectMetadata implements Serializable {
    public ObjectMetadata(String name) {
        _name = name;
    }

    @Getter
    private final String _name;

    @Getter
    private final Map<UUID, Long> _remoteCopies = new LinkedHashMap<>();

    @Getter
    private final Map<UUID, Long> _changelog = new LinkedHashMap<>();

    @Getter
    private final Map<UUID, Boolean> _deletionMark = new LinkedHashMap<>();

    @Getter
    private final Map<UUID, Boolean> _deletionLog = new LinkedHashMap<>();

    @Getter
    private long _refcount = 0L;

    @Getter
    private boolean _locked = false;

    @Getter
    private boolean _seen = false;

    @Getter
    private boolean _invalid = false;

    public void markSeen() {
        if (!_seen) _seen = true;
    }

    public void markInvalid() {
        if (!_invalid) _invalid = true;
    }

    private final Map<String, Long> _referrers = new HashMap<>();

    public long lock() {
        if (_locked) throw new IllegalArgumentException("Already locked");
        _locked = true;
        _refcount++;
        return _refcount;
    }

    public long unlock() {
        if (!_locked) throw new IllegalArgumentException("Already unlocked");
        _locked = false;
        _refcount--;
        return _refcount;
    }

    public long addRef(String from) {
        _referrers.merge(from, 1L, Long::sum);
        _refcount++;
        return _refcount;
    }

    public long removeRef(String from) {
        _referrers.merge(from, -1L, Long::sum);
        if (_referrers.get(from).equals(0L)) _referrers.remove(from);
        _refcount--;
        return _refcount;
    }

    public Long getOurVersion() {
        return _changelog.values().stream().reduce(0L, Long::sum);
    }

    public Long getBestVersion() {
        if (_remoteCopies.isEmpty()) return getOurVersion();
        return Math.max(getOurVersion(), _remoteCopies.values().stream().max(Long::compareTo).get());
    }

    public void bumpVersion(UUID selfUuid) {
        _changelog.merge(selfUuid, 1L, Long::sum);
    }

    public ObjectChangelog toRpcChangelog() {
        var changelogBuilder = ObjectChangelog.newBuilder();
        for (var m : getChangelog().entrySet()) {
            changelogBuilder.addEntries(ObjectChangelogEntry.newBuilder()
                    .setHost(m.getKey().toString()).setVersion(m.getValue()).build());
        }
        return changelogBuilder.build();
    }

    public ObjectHeader toRpcHeader() {
        var headerBuilder = ObjectHeader.newBuilder().setName(getName());
        headerBuilder.setChangelog(toRpcChangelog());
        return headerBuilder.build();
    }
}

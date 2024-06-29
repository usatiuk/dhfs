package com.usatiuk.dhfs.storage.objects.repository.distributed;

import com.usatiuk.dhfs.objects.repository.distributed.ObjectChangelog;
import com.usatiuk.dhfs.objects.repository.distributed.ObjectChangelogEntry;
import com.usatiuk.dhfs.objects.repository.distributed.ObjectHeader;
import jakarta.enterprise.inject.spi.CDI;
import lombok.Getter;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

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
    private final Set<String> _savedRefs = new LinkedHashSet<>();

    @Getter
    private long _refcount = 0L;

    @Getter
    private boolean _locked = false;

    private AtomicBoolean _seen = new AtomicBoolean(false);

    private AtomicBoolean _deleted = new AtomicBoolean(false);

    public boolean isSeen() {
        return _seen.get();
    }

    public boolean isDeleted() {
        return _deleted.get();
    }

    public void markSeen() {
        _seen.set(true);
    }

    public void delete() {
        _deleted.set(true);
    }

    public void undelete() {
        _deleted.set(false);
    }

    private final Set<String> _referrers = new HashSet<>();

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

    public boolean checkRef(String from) {
        return _referrers.contains(from);
    }

    public long addRef(String from) {
        if (_referrers.contains(from)) return _refcount;
        _referrers.add(from);
        _refcount++;
        return _refcount;
    }

    public long removeRef(String from) {
        if (!_referrers.contains(from)) return _refcount;
        _referrers.remove(from);
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
        var hosts = CDI.current().select(PersistentRemoteHostsService.class).get();

        var all = new ArrayList<>(hosts.getHosts().stream().map(HostInfo::getUuid).toList());
        all.add(hosts.getSelfUuid());

        for (var h : all) {
            var logEntry = ObjectChangelogEntry.newBuilder();
            logEntry.setHost(h.toString());
            if (_changelog.containsKey(h))
                logEntry.setVersion(_changelog.get(h));
            changelogBuilder.addEntries(logEntry.build());
        }
        return changelogBuilder.build();
    }

    public ObjectHeader toRpcHeader() {
        var headerBuilder = ObjectHeader.newBuilder().setName(getName());
        headerBuilder.setChangelog(toRpcChangelog());
        return headerBuilder.build();
    }
}

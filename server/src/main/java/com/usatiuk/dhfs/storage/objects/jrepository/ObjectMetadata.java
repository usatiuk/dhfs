package com.usatiuk.dhfs.storage.objects.jrepository;

import com.usatiuk.dhfs.objects.repository.ObjectChangelog;
import com.usatiuk.dhfs.objects.repository.ObjectChangelogEntry;
import com.usatiuk.dhfs.objects.repository.ObjectHeader;
import com.usatiuk.dhfs.storage.SerializationHelper;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import lombok.Getter;
import lombok.Setter;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serial;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class ObjectMetadata implements Serializable {
    public ObjectMetadata(String name, boolean written, Class<? extends JObjectData> knownClass) {
        _name = name;
        _written.set(written);
        _knownClass.set(knownClass);
    }

    @Getter
    private final String _name;

    @Getter
    private final Map<UUID, Long> _remoteCopies = new LinkedHashMap<>();

    @Getter
    @Setter
    private Map<UUID, Long> _changelog = new LinkedHashMap<>();

    @Getter
    @Setter
    private Set<String> _savedRefs = Collections.emptySet();

    private final AtomicReference<Class<? extends JObjectData>> _knownClass = new AtomicReference<>();

    @Getter
    private long _refcount = 0L;

    @Getter
    private boolean _locked = false;

    private transient AtomicBoolean _written = new AtomicBoolean(true);

    private final AtomicBoolean _deleted = new AtomicBoolean(false);

    public Class<? extends JObjectData> getKnownClass() {
        return _knownClass.get();
    }

    protected void narrowClass(Class<? extends JObjectData> klass) {
        Class<? extends JObjectData> got = null;
        do {
            got = _knownClass.get();
            if (got.equals(klass)) return;
            if (klass.isAssignableFrom(got)) return;
            if (!got.isAssignableFrom(klass))
                throw new StatusRuntimeException(Status.DATA_LOSS.withDescription("Could not narrow class of object " + getName() + " from " + got + " to " + klass));
        } while (!_knownClass.compareAndSet(got, klass));
    }

    @Serial
    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        _written = new AtomicBoolean(true);
    }

    public boolean isDeleted() {
        return _deleted.get();
    }

    public void delete() {
        _deleted.set(true);
    }

    public void undelete() {
        _deleted.set(false);
    }

    public boolean isWritten() {
        return _written.get();
    }

    public void markWritten() {
        _written.set(true);
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

        for (var h : _changelog.entrySet()) {
            var logEntry = ObjectChangelogEntry.newBuilder();
            logEntry.setHost(h.getKey().toString());
            logEntry.setVersion(h.getValue());
            changelogBuilder.addEntries(logEntry.build());
        }
        return changelogBuilder.build();
    }

    public ObjectHeader toRpcHeader() {
        var headerBuilder = ObjectHeader.newBuilder().setName(getName());
        headerBuilder.setChangelog(toRpcChangelog());

        return headerBuilder.build();
    }

    public ObjectHeader toRpcHeader(JObjectData data) {
        var headerBuilder = ObjectHeader.newBuilder().setName(getName());
        headerBuilder.setChangelog(toRpcChangelog());

        if (data != null && data.pushResolution())
            headerBuilder.setPushedData(SerializationHelper.serialize(data));

        return headerBuilder.build();
    }
}

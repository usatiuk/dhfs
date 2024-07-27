package com.usatiuk.dhfs.objects.jrepository;

import com.usatiuk.dhfs.objects.persistence.JObjectDataP;
import com.usatiuk.dhfs.objects.repository.ObjectChangelog;
import com.usatiuk.dhfs.objects.repository.ObjectChangelogEntry;
import com.usatiuk.dhfs.objects.repository.ObjectHeader;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.logging.Log;
import io.vertx.core.impl.ConcurrentHashSet;
import lombok.Getter;
import lombok.Setter;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serial;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class ObjectMetadata implements Serializable {
    @Serial
    private static final long serialVersionUID = 1;
    @Getter
    private final String _name;
    @Getter
    private final Map<UUID, Long> _remoteCopies = new LinkedHashMap<>();
    private final AtomicReference<Class<? extends JObjectData>> _knownClass = new AtomicReference<>();
    private final AtomicBoolean _seen = new AtomicBoolean(false);
    private final AtomicBoolean _deleted = new AtomicBoolean(false);
    @Getter
    private final HashSet<UUID> _confirmedDeletes = new HashSet<>();
    private final Set<String> _referrers = new HashSet<>();
    @Getter
    @Setter
    private Map<UUID, Long> _changelog = new LinkedHashMap<>(4);
    @Getter
    @Setter
    private Set<String> _savedRefs = Collections.emptySet();
    @Getter
    private boolean _locked = false;
    @Getter
    private AtomicBoolean _haveLocalCopy = new AtomicBoolean(false);
    private transient AtomicBoolean _written = new AtomicBoolean(true);

    public ObjectMetadata(String name, boolean written, Class<? extends JObjectData> knownClass) {
        _name = name;
        _written.set(written);
        _knownClass.set(knownClass);
    }

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

    public boolean isSeen() {
        return _seen.get();
    }

    public boolean isDeleted() {
        return _deleted.get();
    }

    public void markSeen() {
        Log.trace("Marking seen: " + getName());
        _seen.set(true);
    }

    public void markDeleted() {
        _deleted.set(true);
    }

    public void undelete() {
        _confirmedDeletes.clear();
        _deleted.set(false);
    }

    public boolean isWritten() {
        return _written.get();
    }

    public void markWritten() {
        _written.set(true);
    }

    public boolean isReferred() {
        return !_referrers.isEmpty();
    }

    public void lock() {
        if (_locked) throw new IllegalArgumentException("Already locked");
        _confirmedDeletes.clear();
        Log.info("Locking " + getName());
        _locked = true;
    }

    public void unlock() {
        if (!_locked) throw new IllegalArgumentException("Already unlocked");
        Log.info("Unlocking " + getName());
        _locked = false;
    }

    public boolean checkRef(String from) {
        return _referrers.contains(from);
    }

    public void addRef(String from) {
        _confirmedDeletes.clear();
        _referrers.add(from);
        Log.trace("Adding ref " + from + " to " + getName());
    }

    public void removeRef(String from) {
        if (isLocked()) {
            unlock();
            Log.error("Object " + getName() + " is locked, but we removed a reference to it, unlocking!");
        }
        Log.trace("Removing ref " + from + " from " + getName());
        _referrers.remove(from);
    }

    public Collection<String> getReferrers() {
        return _referrers.stream().toList();
    }

    protected Collection<String> getReferrersMutable() {
        return _referrers;
    }

    public boolean isDeletionCandidate() {
        return !isLocked() && !isReferred();
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
            if (h.getValue() == 0) continue;
            var logEntry = ObjectChangelogEntry.newBuilder();
            logEntry.setHost(h.getKey().toString());
            logEntry.setVersion(h.getValue());
            changelogBuilder.addEntries(logEntry.build());
        }
        return changelogBuilder.build();
    }

    public ObjectHeader toRpcHeader() {
        return toRpcHeader(null);
    }

    public ObjectHeader toRpcHeader(JObjectDataP data) {
        var headerBuilder = ObjectHeader.newBuilder().setName(getName());
        headerBuilder.setChangelog(toRpcChangelog());

        if (data != null)
            headerBuilder.setPushedData(data);

        return headerBuilder.build();
    }

    public int metaHash() {
        return Objects.hash(_name, isSeen(), getKnownClass(), isDeleted(), _confirmedDeletes, _referrers, _changelog, _locked, _remoteCopies, _savedRefs, _haveLocalCopy);
    }

    public int externalHash() {
        return Objects.hash(_changelog, _haveLocalCopy);
    }

    // Not really a hash
    public int dataHash() {
        return Objects.hash(_changelog);
    }
}

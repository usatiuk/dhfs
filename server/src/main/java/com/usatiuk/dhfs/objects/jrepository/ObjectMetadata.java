package com.usatiuk.dhfs.objects.jrepository;

import com.usatiuk.dhfs.SerializationHelper;
import com.usatiuk.dhfs.objects.repository.ObjectChangelog;
import com.usatiuk.dhfs.objects.repository.ObjectChangelogEntry;
import com.usatiuk.dhfs.objects.repository.ObjectHeader;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.logging.Log;
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
    @Serial
    private static final long serialVersionUID = 1;

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
    private boolean _locked = false;

    private transient AtomicBoolean _written = new AtomicBoolean(true);

    private final AtomicBoolean _seen = new AtomicBoolean(false);

    private final AtomicBoolean _deleted = new AtomicBoolean(false);

    @Getter
    private final HashSet<UUID> _confirmedDeletes = new HashSet<>();

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
        _seen.set(true);
    }

    public void delete() {
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

    private String _referrer = null;

    @Getter
    private boolean _isReferred = false;

    public void lock() {
        if (_locked) throw new IllegalArgumentException("Already locked");
        _confirmedDeletes.clear();
        _locked = true;
    }

    public void unlock() {
        if (!_locked) throw new IllegalArgumentException("Already unlocked");
        _locked = false;
    }

    public boolean checkRef(String from) {
        return Objects.equals(_referrer, from) && _isReferred; // Racey!
    }

    public void addRef(String from) {
        _confirmedDeletes.clear();

        if (_referrer == null) {
            _referrer = from;
            _isReferred = true;
            return;
        }

        if (Objects.equals(_referrer, from)) {
//            if (_isReferred)
//                throw new IllegalStateException("Adding a ref to an already referenced object: " + getName());
            _isReferred = true;
            return;
        }

        if (_knownClass.get().isAnnotationPresent(Movable.class)) {
            Log.debug("Object " + getName() + " changing ownership from " + _referrer + " to " + from);
            _referrer = from;
            _isReferred = true;
            return;
        }

        throw new IllegalStateException("Trying to move an immovable object " + getName());
    }

    public void removeRef(String from) {
        if (isLocked()) {
            unlock();
            Log.error("Object " + getName() + " is locked, but we removed a reference to it, unlocking!");
        }
        if (!_isReferred)
            throw new IllegalStateException("Removing a ref from an unreferenced object!: " + getName());
        if (!Objects.equals(_referrer, from))
            throw new IllegalStateException("Removing a wrong ref from an object " + getName() + " expected: " + _referrer + " have: " + from);

        _isReferred = false;
    }

    public String getRef() {
        if (_isReferred && _referrer != null) return _referrer;
        return null;
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

    public int metaHash() {
        return Objects.hash(isSeen(), getKnownClass(), isDeleted(), _referrer, _isReferred, _locked, _remoteCopies, _savedRefs);
    }

    // Not really a hash
    public int dataHash() {
        return Objects.hash(_changelog);
    }
}

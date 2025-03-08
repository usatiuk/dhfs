package com.usatiuk.dhfs.objects.jrepository;

import com.usatiuk.dhfs.objects.persistence.JObjectDataP;
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
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

public class ObjectMetadata {
    @Getter
    private final String _name;
    @Getter
    private final Map<UUID, Long> _remoteCopies = new LinkedHashMap<>();
    private final AtomicReference<Class<? extends JObjectData>> _knownClass = new AtomicReference<>();
    @Getter
    private final HashSet<UUID> _confirmedDeletes = new LinkedHashSet<>();
    private final Set<String> _referrers = new LinkedHashSet<>();
    @Getter
    private volatile boolean _seen = false;
    @Getter
    private volatile boolean _deleted = false;
    @Getter
    @Setter
    private Map<UUID, Long> _changelog = new LinkedHashMap<>(4);
    @Getter
    @Setter
    private Set<String> _savedRefs = Collections.emptySet();
    @Getter
    private boolean _frozen = false;
    @Getter
    @Setter
    private volatile boolean _haveLocalCopy = false;
    @Getter
    private transient volatile boolean _written = true;
    @Getter
    @Setter
    private long _lastModifiedTx = -1; // -1 if it's already on disk

    public ObjectMetadata(String name, boolean written, Class<? extends JObjectData> knownClass) {
        _name = name;
        _written = written;
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
        _written = true;
    }

    public void markSeen() {
        Log.trace("Marking seen: " + getName());
        _seen = true;
    }

    public void markDeleted() {
        _deleted = true;
    }

    public void undelete() {
        _confirmedDeletes.clear();
        _deleted = false;
    }

    public void markWritten() {
        _written = true;
    }

    // FIXME:? a better way?
    public void markUnWritten() {
        _written = false;
    }

    public boolean isReferred() {
        return !_referrers.isEmpty();
    }

    public void freeze() {
        if (_frozen) throw new IllegalArgumentException("Already frozen");
        _confirmedDeletes.clear();
        Log.trace("Freezing " + getName());
        _frozen = true;
    }

    public void unfreeze() {
        if (!_frozen) throw new IllegalArgumentException("Already unfrozen");
        Log.trace("Unfreezing " + getName());
        _frozen = false;
    }

    public boolean checkRef(String from) {
        return _referrers.contains(from);
    }

    public void addRef(String from) {
        if (from.equals(getName()))
            throw new IllegalArgumentException("Trying to make object refer to itself: " + getName());
        _confirmedDeletes.clear();
        _referrers.add(from);
        if (Log.isTraceEnabled())
            Log.trace("Adding ref " + from + " to " + getName());
    }

    public void removeRef(String from) {
        if (Log.isTraceEnabled())
            Log.trace("Removing ref " + from + " from " + getName());
        _referrers.remove(from);
    }

    public Collection<String> getReferrers() {
        return _referrers.stream().toList();
    }

    public Collection<String> getReferrersMutable() {
        return _referrers;
    }

    public boolean isDeletionCandidate() {
        return !isFrozen() && !isReferred();
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

    public int changelogHash() {
        int res = Objects.hashCode(_changelog);
        res = 31 * res + Objects.hashCode(_haveLocalCopy);
        return res;
    }

    public boolean isOnlyLocal() {
        return getKnownClass().isAnnotationPresent(OnlyLocal.class);
    }
}

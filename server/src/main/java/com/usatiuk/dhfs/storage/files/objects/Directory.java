package com.usatiuk.dhfs.storage.files.objects;

import com.usatiuk.dhfs.storage.files.conflicts.DirectoryConflictResolver;
import com.usatiuk.dhfs.storage.objects.jrepository.JObjectData;
import com.usatiuk.dhfs.storage.objects.repository.distributed.ConflictResolver;
import lombok.Getter;
import lombok.Setter;

import java.util.*;

public class Directory extends FsNode {
    public Directory(UUID uuid) {
        super(uuid);
    }

    public Directory(UUID uuid, long mode) {
        super(uuid, mode);
    }

    @Getter
    @Setter
    private Map<String, UUID> _children = new TreeMap<>();

    @Override
    public Class<? extends ConflictResolver> getConflictResolver() {
        return DirectoryConflictResolver.class;
    }

    public Optional<UUID> getKid(String name) {
        if (_children.containsKey(name))
            return Optional.of(_children.get(name));
        else return Optional.empty();
    }

    public boolean removeKid(String name) {
        return _children.remove(name) != null;
    }

    public boolean putKid(String name, UUID uuid) {
        if (_children.containsKey(name)) return false;

        _children.put(name, uuid);
        return true;
    }
    @Override
    public Class<? extends JObjectData> getRefType() {
        return FsNode.class;
    }

    @Override
    public Collection<String> extractRefs() {
        return _children.values().stream().map(UUID::toString).toList();
    }

    public List<String> getChildrenList() {
        return _children.keySet().stream().toList();
    }

    @Override
    public long estimateSize() {
        return _children.size() * 16L;
    }
}

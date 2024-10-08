package com.usatiuk.dhfs.files.objects;

import com.usatiuk.dhfs.files.conflicts.DirectoryConflictResolver;
import com.usatiuk.dhfs.objects.jrepository.JObjectData;
import com.usatiuk.dhfs.objects.repository.ConflictResolver;
import lombok.Getter;
import lombok.Setter;

import java.io.Serial;
import java.util.*;

public class Directory extends FsNode {
    @Serial
    private static final long serialVersionUID = 1;
    @Getter
    @Setter
    private Map<String, UUID> _children = new HashMap<>();

    public Directory(UUID uuid) {
        super(uuid);
    }

    public Directory(UUID uuid, long mode) {
        super(uuid, mode);
    }

    @Override
    public Class<? extends ConflictResolver> getConflictResolver() {
        return DirectoryConflictResolver.class;
    }

    public Optional<UUID> getKid(String name) {
        return Optional.ofNullable(_children.get(name));
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
    public int estimateSize() {
        return _children.size() * 192;
    }
}

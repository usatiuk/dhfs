package com.usatiuk.dhfs.storage.files.objects;

import java.util.*;

public class Directory extends DirEntry {
    public Directory(UUID uuid) {
        super(uuid);
    }

    final Map<String, UUID> _children = new TreeMap<>();

    public synchronized Map<String, UUID> getChildrenMap() {
        return new TreeMap<>(_children);
    }

    public synchronized Optional<UUID> getKid(String name) {
        if (_children.containsKey(name))
            return Optional.of(_children.get(name));
        else
            return Optional.empty();
    }

    public synchronized boolean removeKid(String name) {
        return _children.remove(name) != null;
    }

    public synchronized boolean putKid(String name, UUID uuid) {
        if (_children.containsKey(name))
            return false;

        _children.put(name, uuid);
        return true;
    }

    public synchronized List<String> getChildrenList() {
        return _children.keySet().stream().toList();
    }
}

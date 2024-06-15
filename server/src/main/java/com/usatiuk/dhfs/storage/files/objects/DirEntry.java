package com.usatiuk.dhfs.storage.files.objects;

import com.usatiuk.dhfs.storage.objects.jrepository.JObject;
import lombok.Getter;

import java.util.UUID;

public abstract class DirEntry extends JObject {
    @Getter
    final UUID uuid;

    protected DirEntry(UUID uuid) {
        this.uuid = uuid;
    }

    @Override
    public String getName() {
        return uuid.toString();
    }

    long mode;

    public synchronized long getMode() {
        return mode;
    }

    public synchronized DirEntry setMode(long mode) {
        this.mode = mode;
        return this;
    }
}

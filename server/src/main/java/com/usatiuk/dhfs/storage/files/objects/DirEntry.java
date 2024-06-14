package com.usatiuk.dhfs.storage.files.objects;

import com.usatiuk.dhfs.storage.objects.jrepository.JObject;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.util.UUID;

@Accessors(chain = true)
@Getter
@Setter
public abstract class DirEntry extends JObject {
    final UUID uuid;

    protected DirEntry(UUID uuid) {
        this.uuid = uuid;
    }

    @Override
    public String getName() {
        return uuid.toString();
    }

    long mode;
}

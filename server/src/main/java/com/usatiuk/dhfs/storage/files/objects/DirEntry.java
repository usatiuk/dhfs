package com.usatiuk.dhfs.storage.files.objects;

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.io.Serial;
import java.io.Serializable;
import java.util.UUID;

@Accessors(chain = true)
@Getter
@Setter
public abstract class DirEntry implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    UUID uuid;
}

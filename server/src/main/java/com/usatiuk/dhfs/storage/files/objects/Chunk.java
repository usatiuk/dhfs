package com.usatiuk.dhfs.storage.files.objects;

import java.io.Serial;
import java.io.Serializable;

public class Chunk implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    String hash;
    byte[] bytes;
}

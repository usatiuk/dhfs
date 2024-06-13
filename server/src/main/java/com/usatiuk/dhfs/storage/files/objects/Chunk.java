package com.usatiuk.dhfs.storage.files.objects;

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.codec.digest.DigestUtils;

import java.io.Serial;
import java.io.Serializable;
import java.util.Arrays;

@Accessors(chain = true)
@Getter
@Setter
public class Chunk implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    public Chunk(byte[] bytes) {
        this.bytes = Arrays.copyOf(bytes, bytes.length);
        this.hash = DigestUtils.sha512Hex(bytes);
    }

    final String hash;
    final byte[] bytes;
}

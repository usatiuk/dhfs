package com.usatiuk.dhfs.storage.files.objects;

import com.usatiuk.dhfs.storage.objects.jrepository.JObject;
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
public class Chunk extends JObject {
    public Chunk(byte[] bytes) {
        this.bytes = Arrays.copyOf(bytes, bytes.length);
        this.hash = DigestUtils.sha512Hex(bytes);
    }

    @Override
    public String getName() {
        return hash;
    }

    final String hash;
    final byte[] bytes;
}

package com.usatiuk.dhfs.storage.files.objects;

import com.usatiuk.dhfs.storage.objects.jrepository.JObject;
import lombok.Getter;
import org.apache.commons.codec.digest.DigestUtils;

import java.util.Arrays;

@Getter
public class Chunk extends JObject {
    final String hash;
    final byte[] bytes;

    public Chunk(byte[] bytes) {
        this.bytes = Arrays.copyOf(bytes, bytes.length);
        this.hash = DigestUtils.sha512Hex(bytes);
    }

    @Override
    public String getName() {
        return hash;
    }
}

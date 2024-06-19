package com.usatiuk.dhfs.storage.files.objects;

import com.usatiuk.dhfs.storage.objects.jrepository.JObject;
import lombok.Getter;
import org.apache.commons.codec.digest.DigestUtils;

import java.util.Arrays;

@Getter
public class ChunkData extends JObject {
    final String _hash;
    final byte[] _bytes;

    public ChunkData(byte[] bytes) {
        this._bytes = Arrays.copyOf(bytes, bytes.length);
        this._hash = DigestUtils.sha512Hex(bytes);
    }

    @Override
    public String getName() {
        return getNameFromHash(_hash);
    }

    public static String getNameFromHash(String hash) {
        return hash + "_data";
    }
}

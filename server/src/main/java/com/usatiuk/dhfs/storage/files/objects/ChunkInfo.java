package com.usatiuk.dhfs.storage.files.objects;

import com.usatiuk.dhfs.storage.objects.jrepository.JObject;
import lombok.Getter;
import org.apache.commons.codec.digest.DigestUtils;

@Getter
public class ChunkInfo extends JObject {
    final String _hash;
    final Integer _size;

    public ChunkInfo(String hash, Integer size) {
        this._hash = hash;
        this._size = size;
    }

    @Override
    public String getName() {
        return getNameFromHash(_hash);
    }

    public static String getNameFromHash(String hash) {
        return hash + "_info";
    }
}

package com.usatiuk.dhfs.files.objects;

import com.usatiuk.autoprotomap.runtime.ProtoSerializer;
import com.usatiuk.dhfs.persistence.FileDtoP;
import com.usatiuk.dhfs.utils.SerializationHelper;
import jakarta.inject.Singleton;

import java.io.IOException;

@Singleton
public class FileProtoSerializer implements ProtoSerializer<FileDtoP, FileDto> {
    @Override
    public FileDto deserialize(FileDtoP message) {
        try (var is = message.getSerializedData().newInput()) {
            return SerializationHelper.deserialize(is);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public FileDtoP serialize(FileDto object) {
        return FileDtoP.newBuilder().setSerializedData(SerializationHelper.serialize(object)).build();
    }
}

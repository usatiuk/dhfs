package com.usatiuk.dhfs.files.objects;

import com.usatiuk.dhfs.JDataRemote;
import com.usatiuk.dhfs.objects.JObjectKey;
import com.usatiuk.dhfs.repository.JDataRemoteDto;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;

public record FileDto(File file, List<Pair<Long, JObjectKey>> chunks) implements JDataRemoteDto {
    @Override
    public Class<? extends JDataRemote> objClass() {
        return File.class;
    }
}

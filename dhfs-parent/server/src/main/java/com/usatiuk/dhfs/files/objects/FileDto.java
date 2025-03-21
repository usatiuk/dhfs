package com.usatiuk.dhfs.files.objects;

import com.usatiuk.dhfs.objects.JDataRemote;
import com.usatiuk.dhfs.objects.JObjectKey;
import com.usatiuk.dhfs.objects.repository.JDataRemoteDto;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.Map;

public record FileDto(File file, List<Pair<Long, JObjectKey>> chunks) implements JDataRemoteDto {
    @Override
    public Class<? extends JDataRemote> objClass() {
        return File.class;
    }
}

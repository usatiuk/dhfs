package com.usatiuk.dhfsfs.objects;

import com.usatiuk.dhfs.remoteobj.JDataRemote;
import com.usatiuk.dhfs.remoteobj.JDataRemoteDto;
import com.usatiuk.objects.JObjectKey;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;

public record FileDto(File file, List<Pair<Long, JObjectKey>> chunks) implements JDataRemoteDto {
    @Override
    public Class<? extends JDataRemote> objClass() {
        return File.class;
    }
}

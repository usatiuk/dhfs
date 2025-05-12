package com.usatiuk.dhfsfs.objects;

import com.usatiuk.dhfs.remoteobj.JDataRemote;
import com.usatiuk.dhfs.remoteobj.JDataRemoteDto;
import com.usatiuk.objects.JObjectKey;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;

/**
 * FileDto is a data transfer object that contains a file and its chunks.
 * @param file the file
 * @param chunks the list of chunks, each represented as a pair of a long and a JObjectKey
 */
public record FileDto(File file, List<Pair<Long, JObjectKey>> chunks) implements JDataRemoteDto {
    @Override
    public Class<? extends JDataRemote> objClass() {
        return File.class;
    }
}

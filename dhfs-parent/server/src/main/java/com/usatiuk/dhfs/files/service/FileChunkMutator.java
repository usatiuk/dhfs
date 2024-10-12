package com.usatiuk.dhfs.files.service;

import com.usatiuk.dhfs.files.objects.File;
import com.usatiuk.dhfs.objects.jrepository.JMutator;

import java.util.NavigableMap;

public class FileChunkMutator implements JMutator<File> {
    private final long _oldTime;
    private final long _newTime;
    private final NavigableMap<Long, String> _removedChunks;
    private final NavigableMap<Long, String> _newChunks;

    public FileChunkMutator(long oldTime, long newTime, NavigableMap<Long, String> removedChunks, NavigableMap<Long, String> newChunks) {
        _oldTime = oldTime;
        _newTime = newTime;
        _removedChunks = removedChunks;
        _newChunks = newChunks;
    }

    @Override
    public boolean mutate(File object) {
        object.setMtime(_newTime);
        object.getChunks().keySet().removeAll(_removedChunks.keySet());
        object.getChunks().putAll(_newChunks);
        return true;
    }

    @Override
    public void revert(File object) {
        object.setMtime(_oldTime);
        object.getChunks().keySet().removeAll(_newChunks.keySet());
        object.getChunks().putAll(_removedChunks);
    }

}

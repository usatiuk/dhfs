package com.usatiuk.dhfsfs.objects;

import com.usatiuk.dhfs.jmap.JMapHelper;
import com.usatiuk.dhfs.syncmap.DtoMapper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/**
 * Maps a {@link File} object to a {@link FileDto} object and vice versa.
 */
@ApplicationScoped
public class FileDtoMapper implements DtoMapper<File, FileDto> {
    @Inject
    JMapHelper jMapHelper;
    @Inject
    FileHelper fileHelper;

    @Override
    public FileDto toDto(File obj) {
        return new FileDto(obj, fileHelper.getChunks(obj));
    }

    @Override
    public File fromDto(FileDto dto) {
        throw new UnsupportedOperationException();
    }
}

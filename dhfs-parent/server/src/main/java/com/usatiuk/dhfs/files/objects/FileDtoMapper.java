package com.usatiuk.dhfs.files.objects;

import com.usatiuk.dhfs.objects.jmap.JMapHelper;
import com.usatiuk.dhfs.objects.repository.syncmap.DtoMapper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

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

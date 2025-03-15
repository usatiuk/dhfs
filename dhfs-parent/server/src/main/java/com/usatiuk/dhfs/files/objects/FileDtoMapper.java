package com.usatiuk.dhfs.files.objects;

import com.usatiuk.dhfs.objects.JObjectKey;
import com.usatiuk.dhfs.objects.jmap.JMapHelper;
import com.usatiuk.dhfs.objects.repository.syncmap.DtoMapper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.Collections;

@ApplicationScoped
public class FileDtoMapper implements DtoMapper<File, FileDto> {
    @Inject
    JMapHelper jMapHelper;

    @Override
    public FileDto toDto(File obj) {
        ArrayList<Pair<Long, JObjectKey>> chunks = new ArrayList<>();
        try (var it = jMapHelper.getIterator(obj)) {
            while (it.hasNext()) {
                var cur = it.next();
                chunks.add(Pair.of(cur.getKey().key(), cur.getValue().ref()));
            }
        }

        return new FileDto(obj, Collections.unmodifiableList(chunks));
    }

    @Override
    public File fromDto(FileDto dto) {
        throw new UnsupportedOperationException();
    }
}

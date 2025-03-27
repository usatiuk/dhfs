package com.usatiuk.dhfs.files.objects;

import com.usatiuk.dhfs.objects.JObjectKey;
import com.usatiuk.dhfs.jmap.JMapHelper;
import com.usatiuk.dhfs.jmap.JMapLongKey;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.List;

@ApplicationScoped
public class FileHelper {
    @Inject
    JMapHelper jMapHelper;

    public List<Pair<Long, JObjectKey>> getChunks(File file) {
        ArrayList<Pair<Long, JObjectKey>> chunks = new ArrayList<>();
        try (var it = jMapHelper.getIterator(file)) {
            while (it.hasNext()) {
                var cur = it.next();
                chunks.add(Pair.of(cur.getKey().key(), cur.getValue().ref()));
            }
        }
        return List.copyOf(chunks);
    }

    public void replaceChunks(File file, List<Pair<Long, JObjectKey>> chunks) {
        jMapHelper.deleteAll(file);

        for (var f : chunks) {
            jMapHelper.put(file, JMapLongKey.of(f.getLeft()), f.getRight());
        }
    }
}

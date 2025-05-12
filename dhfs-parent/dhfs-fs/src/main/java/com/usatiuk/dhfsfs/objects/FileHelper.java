package com.usatiuk.dhfsfs.objects;

import com.usatiuk.dhfs.jmap.JMapHelper;
import com.usatiuk.dhfs.jmap.JMapLongKey;
import com.usatiuk.objects.JObjectKey;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.List;

/**
 * Helper class for working with files.
 */
@ApplicationScoped
public class FileHelper {
    @Inject
    JMapHelper jMapHelper;

    /**
     * Get the chunks of a file.
     * Transaction is expected to be already started.
     * @param file the file to get chunks from
     * @return a list of pairs of chunk offset and chunk key
     */
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

    /**
     * Replace the chunks of a file.
     * All previous chunks will be deleted.
     * Transaction is expected to be already started.
     * @param file the file to replace chunks in
     * @param chunks the list of pairs of chunk offset and chunk key
     */
    public void replaceChunks(File file, List<Pair<Long, JObjectKey>> chunks) {
        jMapHelper.deleteAll(file);

        for (var f : chunks) {
            jMapHelper.put(file, JMapLongKey.of(f.getLeft()), f.getRight());
        }
    }
}

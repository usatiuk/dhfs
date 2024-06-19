package com.usatiuk.dhfs.storage.files.objects;

import com.usatiuk.dhfs.objects.repository.distributed.ObjectHeader;
import com.usatiuk.dhfs.storage.DeserializationHelper;
import com.usatiuk.dhfs.storage.objects.repository.distributed.ConflictResolver;
import com.usatiuk.dhfs.storage.objects.repository.distributed.ObjectMetaData;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.UUID;

@ApplicationScoped
public class DirectoryConflictResolver implements ConflictResolver {
    @ConfigProperty(name = "dhfs.objects.distributed.selfname")
    String selfname;

    @Override
    public ConflictResolutionResult resolve(byte[] oursData, ObjectHeader oursHeader, byte[] theirsData, ObjectHeader theirsHeader, String theirsSelfname) {
        var ours = (Directory) DeserializationHelper.deserialize(oursData);
        var theirs = (Directory) DeserializationHelper.deserialize(theirsData);
        if (!ours.getClass().equals(Directory.class) || !theirs.getClass().equals(Directory.class)) {
            Log.error("Object type mismatch!");
            throw new NotImplementedException();
        }

        LinkedHashMap<String, UUID> mergedChildren = new LinkedHashMap<>(((Directory) ours).getChildrenMap());
        for (var entry : ((Directory) theirs).getChildrenMap().entrySet()) {
            if (mergedChildren.containsKey(entry.getKey())) {
                mergedChildren.put(entry.getValue() + ".conflict." + theirsSelfname, entry.getValue());
            }
        }

        var newMetaData = new ObjectMetaData(oursHeader.getName(), oursHeader.getConflictResolver());

        for (var entry : oursHeader.getChangelog().getEntriesList()) {
            newMetaData.getChangelog().put(entry.getHost(), entry.getVersion());
        }
        for (var entry : theirsHeader.getChangelog().getEntriesList()) {
            newMetaData.getChangelog().merge(entry.getHost(), entry.getVersion(), Long::max);
        }

        newMetaData.getChangelog().merge(selfname, 1L, Long::sum);

        var newHdr = newMetaData.toRpcHeader();

        var newDir = new Directory(((Directory) ours).getUuid(), ((Directory) ours).getMode());
        for (var entry : mergedChildren.entrySet()) newDir.putKid(entry.getKey(), entry.getValue());

        // FIXME:
        newDir.setMtime(System.currentTimeMillis());
        newDir.setCtime(ours.getCtime());

        return new ConflictResolutionResult(ConflictResolutionResult.Type.RESOLVED,
                List.of(Pair.of(newHdr, SerializationUtils.serialize(newDir))));
    }
}

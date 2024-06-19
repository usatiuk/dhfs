package com.usatiuk.dhfs.storage.objects.jrepository;

import com.usatiuk.dhfs.objects.repository.distributed.ObjectHeader;
import com.usatiuk.dhfs.storage.DeserializationHelper;
import com.usatiuk.dhfs.storage.objects.repository.distributed.ConflictResolver;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.apache.commons.lang3.NotImplementedException;

@ApplicationScoped
@Named("JObjectConflictResolution")
public class JObjectConflictResolution implements ConflictResolver {
    @Inject
    Instance<ConflictResolver> conflictResolvers;

    @Override
    public ConflictResolutionResult
    resolve(byte[] oursData, ObjectHeader oursHeader, byte[] theirsData, ObjectHeader theirsHeader, String theirsSelfname) {
        var ours = (JObject) DeserializationHelper.deserialize(oursData);
        var theirs = (JObject) DeserializationHelper.deserialize(theirsData);
        if (!ours.getClass().equals(theirs.getClass())) {
            Log.error("Object type mismatch!");
            throw new NotImplementedException();
        }
        return conflictResolvers.select(ours.getConflictResolver()).get().resolve(oursData, oursHeader, theirsData, theirsHeader, theirsSelfname);
    }
}

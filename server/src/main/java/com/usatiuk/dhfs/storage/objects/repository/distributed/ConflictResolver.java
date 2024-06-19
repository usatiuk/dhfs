package com.usatiuk.dhfs.storage.objects.repository.distributed;

import com.usatiuk.dhfs.objects.repository.distributed.ObjectHeader;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;

public interface ConflictResolver {
    @AllArgsConstructor
    @Getter
    public static class ConflictResolutionResult {
        public enum Type {
            EQUIVALENT,
            RESOLVED,
            FAILED
        }

        private final Type _type;
        private final List<Pair<ObjectHeader, byte[]>> _results;
    }

    public ConflictResolutionResult
    resolve(byte[] oursData, ObjectHeader oursHeader, byte[] theirsData, ObjectHeader theirsHeader, String theirsSelfname);
}

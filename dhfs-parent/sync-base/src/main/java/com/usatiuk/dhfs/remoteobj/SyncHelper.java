package com.usatiuk.dhfs.remoteobj;

import com.usatiuk.dhfs.peersync.PeerId;
import org.pcollections.PMap;

import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Helper class for synchronizing objects.
 */
public class SyncHelper {

    /**
     * Compares two changelogs.
     *
     * @param current the current changelog
     * @param other   the other changelog
     * @return the result of the comparison
     */
    public static ChangelogCmpResult compareChangelogs(PMap<PeerId, Long> current, PMap<PeerId, Long> other) {
        boolean hasLower = false;
        boolean hasHigher = false;
        for (var e : Stream.concat(current.keySet().stream(), other.keySet().stream()).collect(Collectors.toUnmodifiableSet())) {
            if (other.getOrDefault(e, 0L) < current.getOrDefault(e, 0L))
                hasLower = true;
            if (other.getOrDefault(e, 0L) > current.getOrDefault(e, 0L))
                hasHigher = true;
        }

        if (hasLower && hasHigher)
            return ChangelogCmpResult.CONFLICT;

        if (hasLower)
            return ChangelogCmpResult.OLDER;

        if (hasHigher)
            return ChangelogCmpResult.NEWER;

        return ChangelogCmpResult.EQUAL;
    }

    public enum ChangelogCmpResult {
        EQUAL,
        NEWER,
        OLDER,
        CONFLICT
    }

//    public static PMap<PeerId,Long> mergeChangelogs(PMap<PeerId, Long> current, PMap<PeerId, Long> other) {
//        return current.plusAll(other);
//    }
}

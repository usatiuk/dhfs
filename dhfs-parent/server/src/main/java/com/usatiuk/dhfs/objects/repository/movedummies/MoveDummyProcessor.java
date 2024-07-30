package com.usatiuk.dhfs.objects.repository.movedummies;

import com.usatiuk.dhfs.objects.jrepository.JObject;
import com.usatiuk.dhfs.objects.jrepository.JObjectData;
import com.usatiuk.dhfs.objects.jrepository.JObjectManager;
import com.usatiuk.dhfs.objects.repository.ObjectHeader;
import com.usatiuk.dhfs.objects.repository.PushedMoves;
import com.usatiuk.dhfs.objects.repository.RemoteObjectServiceClient;
import com.usatiuk.dhfs.objects.repository.SyncHandler;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Optional;
import java.util.UUID;

@ApplicationScoped
public class MoveDummyProcessor {
    @Inject
    RemoteObjectServiceClient remoteObjectServiceClient;
    @Inject
    JObjectManager jObjectManager;
    @Inject
    SyncHandler syncHandler;

    private void ensureDummyLink(JObject<?> from, String to) {
        from.assertRWLock();

        if (from.hasLocalCopy()) {
            from.tryResolve(JObject.ResolutionStrategy.LOCAL_ONLY);
            if (!from.getData().extractRefs().contains(to))
                throw new IllegalStateException("Pushed move has actual copy but no ref " + from.getName() + "->" + to);
            return;
        }

        var toObj = jObjectManager.get(to).orElse(null);
        if (toObj == null) return;

        toObj.rwLock();
        try {
            if (toObj.getMeta().isDeleted())
                Log.warn("Pushed move but object is already deleted: " + from.getName() + "->" + to);

            toObj.markSeen(); // TODO: Is this needed?
            from.markSeen();

            var meta = from.getMeta();
            if (meta.getSavedRefs() == null) meta.setSavedRefs(new HashSet<>());

            meta.getSavedRefs().add(to);
            meta.setMoveDummy(true);

            toObj.getMeta().addRef(from.getName());
        } finally {
            toObj.rwUnlock();
        }
    }

    private void processPushedMove(UUID from, ObjectHeader parent, String kid) {
        var obj = jObjectManager.getOrPutLocked(parent.getName(), JObjectData.class, Optional.empty());
        try {
            if (Log.isTraceEnabled())
                Log.trace("Processing pushed move from " + from + " " + parent.getName() + "->" + kid);
            syncHandler.handleOneUpdate(from, parent);
            ensureDummyLink(obj, kid);
            if (Log.isTraceEnabled())
                Log.trace("Processed pushed move from " + from + " " + parent.getName() + "->" + kid);
        } finally {
            obj.rwUnlock();
        }
    }

    public void processPushedMoves(UUID from, PushedMoves moves) {
        ArrayList<MoveDummyEntry> processed = new ArrayList<>();

        for (var m : moves.getPushedMovesList()) {
            try {
                processPushedMove(from, m.getParent(), m.getKid());
            } catch (Exception e) {
                Log.error("Error when processing pushed move " + m.getParent().getName() + "->" + m.getKid() + " from " + from, e);
                continue;
            }
            processed.add(new MoveDummyEntry(m.getParent().getName(), m.getKid()));
        }

        remoteObjectServiceClient.pushConfirmedPushedMoves(from, processed);
    }
}

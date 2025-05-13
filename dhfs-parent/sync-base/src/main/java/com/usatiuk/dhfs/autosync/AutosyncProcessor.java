package com.usatiuk.dhfs.autosync;

import com.usatiuk.dhfs.remoteobj.JDataRemote;
import com.usatiuk.dhfs.remoteobj.RemoteObjectMeta;
import com.usatiuk.dhfs.remoteobj.RemoteTransaction;
import com.usatiuk.objects.JData;
import com.usatiuk.objects.JObjectKey;
import com.usatiuk.objects.iterators.IteratorStart;
import com.usatiuk.objects.transaction.Transaction;
import com.usatiuk.objects.transaction.TransactionManager;
import com.usatiuk.utils.HashSetDelayedBlockingQueue;
import io.quarkus.logging.Log;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Simple class to automatically download remote objects in the background.
 */
@ApplicationScoped
public class AutosyncProcessor {
    private final HashSetDelayedBlockingQueue<JObjectKey> _pending = new HashSetDelayedBlockingQueue<>(0);
    private final HashSetDelayedBlockingQueue<JObjectKey> _retries = new HashSetDelayedBlockingQueue<>(10000); //FIXME:
    @Inject
    TransactionManager txm;
    @ConfigProperty(name = "dhfs.objects.autosync.threads")
    int autosyncThreads;
    @Inject
    ExecutorService executorService;
    @Inject
    Transaction curTx;
    @Inject
    RemoteTransaction remoteTx;
    @ConfigProperty(name = "dhfs.objects.autosync.download-all")
    boolean downloadAll;

    private ExecutorService _autosyncExcecutor;

    void init(@Observes @Priority(300) StartupEvent event) {
        BasicThreadFactory factory = new BasicThreadFactory.Builder()
                .namingPattern("autosync-%d")
                .build();

        _autosyncExcecutor = Executors.newFixedThreadPool(autosyncThreads, factory);
        for (int i = 0; i < autosyncThreads; i++) {
            _autosyncExcecutor.submit(this::autosync);
        }

        if (downloadAll)
            executorService.submit(() -> {
                Log.info("Adding all to autosync");
                txm.run(() -> {
                    try (var it = curTx.getIterator(IteratorStart.GE, JObjectKey.first())) {
                        while (it.hasNext()) {
                            var key = it.peekNextKey();
                            txm.run(() -> {
                                var gotObj = curTx.get(JData.class, key).orElse(null);
                                if (!(gotObj instanceof RemoteObjectMeta meta))
                                    return;
                                if (!meta.hasLocalData())
                                    add(meta.key());
                            }, true);
                            it.skip();
                        }
                    }
                });
                Log.info("Adding all to autosync: finished");
            });
    }

    void shutdown(@Observes @Priority(10) ShutdownEvent event) {
        _autosyncExcecutor.shutdownNow();
    }

    /**
     * Adds an object to the queue to be downloaded.
     *
     * @param name the object to add
     */
    public void add(JObjectKey name) {
        _pending.add(name);
    }

    private void autosync() {
        try {
            while (!Thread.interrupted()) {
                JObjectKey name = null;

                while (name == null) {
                    name = _pending.tryGet();
                    if (name == null)
                        name = _retries.tryGet();
                    if (name == null)
                        name = _pending.get(1000L); //FIXME:
                }

                try {
                    JObjectKey finalName = name;
                    boolean ok = txm.run(() -> {
                        RemoteObjectMeta obj;
                        try {
                            obj = remoteTx.getMeta(finalName).orElse(null);
                        } catch (ClassCastException cex) {
                            Log.debugv("Not downloading object {0}, not remote object", finalName);
                            return true;
                        }

                        if (obj == null) {
                            Log.debugv("Not downloading object {0}, not found", finalName);
                            return true;
                        }
                        if (obj.hasLocalData()) {
                            Log.debugv("Not downloading object {0}, already have local data", finalName);
                            return true;
                        }
                        var data = remoteTx.getData(JDataRemote.class, finalName);
                        return data.isPresent();
                    });
                    if (ok) {
                        Log.debugv("Downloaded object {0}", name);
                    }
                    if (!ok) {
                        Log.debug("Failed downloading object " + name + ", will retry.");
                        _retries.add(name);
                    }
                } catch (Exception e) {
                    Log.debug("Failed downloading object " + name + ", will retry.", e);
                    _retries.add(name);
                }
            }
        } catch (InterruptedException ignored) {
        }
        Log.info("Autosync thread exiting");

    }
}

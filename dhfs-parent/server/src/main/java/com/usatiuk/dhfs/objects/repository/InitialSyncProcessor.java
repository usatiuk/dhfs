package com.usatiuk.dhfs.objects.repository;

import com.usatiuk.dhfs.objects.JData;
import com.usatiuk.dhfs.objects.JObjectKey;
import com.usatiuk.dhfs.objects.PeerId;

public interface InitialSyncProcessor<T extends JData> {
    void prepareForInitialSync(PeerId from, JObjectKey key);
}

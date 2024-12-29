package com.usatiuk.dhfs.objects;

import com.usatiuk.objects.common.runtime.JData;
import com.usatiuk.objects.common.runtime.JObjectKey;

public interface PreCommitTxHook {
    default void onChange(JObjectKey key, JData old, JData cur) {
    }

    default void onCreate(JObjectKey key, JData cur) {
    }

    default void onDelete(JObjectKey key, JData cur) {
    }

    default int getPriority() {
        return 0;
    }
}

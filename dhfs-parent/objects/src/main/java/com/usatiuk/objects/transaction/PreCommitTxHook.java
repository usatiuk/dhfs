package com.usatiuk.objects.transaction;

import com.usatiuk.objects.JData;
import com.usatiuk.objects.JObjectKey;

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

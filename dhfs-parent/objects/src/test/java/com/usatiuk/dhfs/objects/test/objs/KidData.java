package com.usatiuk.dhfs.objects.test.objs;

import com.usatiuk.dhfs.objects.JData;
import com.usatiuk.dhfs.objects.JObject;
import com.usatiuk.dhfs.objects.transaction.Transaction;

import java.util.function.Function;

public interface KidData extends JData {
    String getName();

    void setName(String name);

    KidData bindCopy();

    default Function<Transaction, JObject> binder(boolean isLocked) {
        return jo -> new Kid(jo, bindCopy());
    }
}

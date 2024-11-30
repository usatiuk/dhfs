package com.usatiuk.dhfs.objects.test.objs;

import com.usatiuk.dhfs.objects.JData;
import com.usatiuk.dhfs.objects.JObject;
import com.usatiuk.dhfs.objects.JObjectInterface;

import java.util.function.Function;

public interface KidData extends JData {
    String getName();

    void setName(String name);

    KidData bindCopy();

    default Function<JObjectInterface, JObject> binder() {
        return jo -> new Kid(jo, bindCopy());
    }
}

package com.usatiuk.dhfs.objects.test.objs;

import com.usatiuk.dhfs.objects.JData;
import com.usatiuk.dhfs.objects.JObjectKey;

public interface ParentData extends JData {
    String getName();

    void setName(String name);

    JObjectKey getKidKey();

    void setKidKey(JObjectKey kid);
}

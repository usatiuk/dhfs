package com.usatiuk.objects.alloc.it;

import com.usatiuk.objects.common.runtime.JData;
import com.usatiuk.objects.common.runtime.JObjectKey;

public interface TestJDataAssorted extends JData {
    String getLastName();

    void setLastName(String lastName);

    long getAge();

    void setAge(long age);

    JObjectKey getKidKey();

    void setKidKey(JObjectKey kid);
}

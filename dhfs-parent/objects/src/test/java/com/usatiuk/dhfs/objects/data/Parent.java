package com.usatiuk.dhfs.objects.data;

import com.usatiuk.objects.common.runtime.JData;
import com.usatiuk.objects.common.runtime.JObjectKey;

public interface Parent extends JData {
    String getLastName();

    void setLastName(String lastName);

    JObjectKey getKidKey();

    void setKidKey(JObjectKey kid);
}

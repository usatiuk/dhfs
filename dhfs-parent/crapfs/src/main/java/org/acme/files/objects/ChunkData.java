package org.acme.files.objects;

import com.usatiuk.objects.common.runtime.JData;

public interface ChunkData extends JData {
    byte[] getData();

    void setData(byte[] data);
}

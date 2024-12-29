package com.usatiuk.dhfs.files.objects;

import com.google.protobuf.ByteString;
import com.usatiuk.dhfs.objects.JDataRefcounted;
import com.usatiuk.objects.common.runtime.JData;

import java.io.Serializable;

public interface ChunkData extends JDataRefcounted, Serializable {
    ByteString getData();

    void setData(ByteString data);
}

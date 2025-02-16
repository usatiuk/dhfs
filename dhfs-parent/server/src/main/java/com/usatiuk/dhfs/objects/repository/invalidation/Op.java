package com.usatiuk.dhfs.objects.repository.invalidation;

import com.usatiuk.autoprotomap.runtime.ProtoMirror;
import com.usatiuk.dhfs.objects.JObjectKey;
import com.usatiuk.dhfs.objects.repository.OpPushPayload;

import java.util.Collection;

@ProtoMirror(OpPushPayload.class)
public interface Op {
    Collection<JObjectKey> getEscapedRefs();
}

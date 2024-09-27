package com.usatiuk.dhfs.objects.repository.opsupport;

import com.usatiuk.autoprotomap.runtime.ProtoMirror;
import com.usatiuk.dhfs.objects.repository.OpPushPayload;

import java.util.Collection;

@ProtoMirror(OpPushPayload.class)
public interface Op {
    Collection<String> getEscapedRefs();
}

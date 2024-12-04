package com.usatiuk.objects.alloc.it;

import com.usatiuk.objects.common.runtime.JDataAllocVersionProvider;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class DummyVersionProvider implements JDataAllocVersionProvider {

    long version = 0;

    @Override
    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

}

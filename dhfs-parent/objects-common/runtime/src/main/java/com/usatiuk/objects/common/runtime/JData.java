package com.usatiuk.objects.common.runtime;

// TODO: This could be maybe moved to a separate module?
// The base class for JObject data
// Only one instance of this "exists" per key, the instance in the manager is canonical
// When committing a transaction, the instance is checked against it, if it isn't the same, a race occurred.
// It is immutable, its version is filled in by the allocator from the AllocVersionProvider
public interface JData {
    JObjectKey getKey();

    long getVersion();
}
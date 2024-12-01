package com.usatiuk.dhfs.objects;

// The base class for JObject data
// Only one instance of this exists per key, the instance in the manager is canonical
// When committing a transaction, the instance is checked against it, if it isn't the same, a race occurred.
public interface JData {
    JObjectKey getKey();
}

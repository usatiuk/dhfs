package com.usatiuk.dhfs.objects.jrepository;

public interface SoftJObject<T extends JObjectData> {
    JObject<T> get();
    String getName();
}

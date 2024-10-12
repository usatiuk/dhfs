package com.usatiuk.dhfs.objects.jrepository;

public interface SoftJObject<T extends JObjectData> {
    JObject<? extends T> get();

    String getName();
}

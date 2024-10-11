package com.usatiuk.dhfs.objects.jrepository;

public interface JMutator<T extends JObjectData> {
    boolean mutate(T object);

    void revert(T object);
}

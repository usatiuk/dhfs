package com.usatiuk.dhfs.remoteobj;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation for eagerly pushed remote objects.
 * This annotation is used to mark remote object which notification operations should contain the object itself,
 * to avoid the other peer having to download it.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface JDataRemotePush {
}

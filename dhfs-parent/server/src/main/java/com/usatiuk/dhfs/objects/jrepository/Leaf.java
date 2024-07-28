package com.usatiuk.dhfs.objects.jrepository;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

// Indicates the object never has references
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface Leaf {
}

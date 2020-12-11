package io.basestar.storage.annotation;

import io.basestar.storage.Storage;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface ConfigurableStorage {

    Class<? extends Storage.Builder> builderClass();
}

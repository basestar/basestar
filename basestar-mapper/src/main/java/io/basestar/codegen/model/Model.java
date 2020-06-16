package io.basestar.codegen.model;

import io.basestar.codegen.CodegenSettings;

public class Model {

    private final CodegenSettings settings;

    public Model(final CodegenSettings settings) {

        this.settings = settings;
    }

    protected CodegenSettings getSettings() {

        return settings;
    }

    public String getPackageName() {

        return settings.getPackageName();
    }
}

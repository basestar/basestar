package io.basestar.codegen;

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

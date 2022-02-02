package io.basestar;

public class Basestar {

    private Basestar() {

    }

    public static void init() {

        System.setProperty("java.protocol.handler.pkgs", "io.basestar.protocol");
    }
}

package io.basestar.auth;

import lombok.Data;

import java.util.Collections;
import java.util.Map;

public interface Caller {

    Anon ANON = new Anon();

    Super SUPER = new Super();

    boolean isAnon();

    boolean isSuper();

    String getSchema();

    String getId();

    Map<String, Object> getClaims();

    @Data
    class Delegating implements Caller {

        private final Caller delegate;

        @Override
        public boolean isAnon() {

            return delegate.isAnon();
        }

        @Override
        public boolean isSuper() {

            return delegate.isSuper();
        }

        @Override
        public String getSchema() {

            return delegate.getSchema();
        }

        @Override
        public String getId() {

            return delegate.getId();
        }

        @Override
        public Map<String, Object> getClaims() {

            return delegate.getClaims();
        }
    }

    class Anon implements Caller {

        @Override
        public boolean isAnon() {

            return true;
        }

        @Override
        public boolean isSuper() {

            return false;
        }

        @Override
        public String getSchema() {

            return null;
        }

        @Override
        public String getId() {

            return null;
        }

        @Override
        public Map<String, Object> getClaims() {

            return Collections.emptyMap();
        }
    }

    class Super implements Caller {

        @Override
        public boolean isAnon() {

            return false;
        }

        @Override
        public boolean isSuper() {

            return true;
        }

        @Override
        public String getSchema() {

            return null;
        }

        @Override
        public String getId() {

            return null;
        }

        @Override
        public Map<String, Object> getClaims() {

            return Collections.emptyMap();
        }
    }

    //boolean verify(Permission permission, Map<String, Object> context);
}

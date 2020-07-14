package io.basestar.maven.test;

@javax.validation.Valid
@io.basestar.mapper.annotation.ObjectSchema(name = "Test")
public class Test  {

    @io.basestar.mapper.annotation.Created
    private java.time.LocalDateTime created;

    @io.basestar.mapper.annotation.Hash
    private String hash;

    @io.basestar.mapper.annotation.Id
    private String id;

    @io.basestar.mapper.annotation.Updated
    private java.time.LocalDateTime updated;

    @io.basestar.mapper.annotation.Version
    private Long version;

    @io.basestar.mapper.annotation.Property(name = "a")
    @javax.validation.constraints.NotNull
    private io.basestar.maven.test.a.Test a;

    @io.basestar.mapper.annotation.Property(name = "b")
    @javax.validation.constraints.NotNull
    private io.basestar.maven.test.b.Test b;

    public java.time.LocalDateTime getCreated() {

        return created;
    }

    public Test setCreated(final java.time.LocalDateTime created) {

        this.created = created;
        return this;
    }

    public String getHash() {

        return hash;
    }

    public Test setHash(final String hash) {

        this.hash = hash;
        return this;
    }

    public String getId() {

        return id;
    }

    public Test setId(final String id) {

        this.id = id;
        return this;
    }

    public java.time.LocalDateTime getUpdated() {

        return updated;
    }

    public Test setUpdated(final java.time.LocalDateTime updated) {

        this.updated = updated;
        return this;
    }

    public Long getVersion() {

        return version;
    }

    public Test setVersion(final Long version) {

        this.version = version;
        return this;
    }

    public io.basestar.maven.test.a.Test getA() {

        return a;
    }

    public Test setA(final io.basestar.maven.test.a.Test a) {

        this.a = a;
        return this;
    }

    public io.basestar.maven.test.b.Test getB() {

        return b;
    }

    public Test setB(final io.basestar.maven.test.b.Test b) {

        this.b = b;
        return this;
    }

    @Override
    public String toString() {
        return "Test{created=" + created
            + ", hash=" + hash
            + ", id=" + id
            + ", updated=" + updated
            + ", version=" + version
            + ", a=" + a
            + ", b=" + b
            + "}";
    }

    protected boolean canEqual(final Object other) {

        return other instanceof Test;
    }

    @Override
    public boolean equals(final Object o) {

        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final Test other = (Test) o;
        return java.util.Objects.equals(created, other.created)
            && java.util.Objects.equals(hash, other.hash)
            && java.util.Objects.equals(id, other.id)
            && java.util.Objects.equals(updated, other.updated)
            && java.util.Objects.equals(version, other.version)
            && java.util.Objects.equals(a, other.a)
            && java.util.Objects.equals(b, other.b);
    }

    @Override
    public int hashCode() {

        return java.util.Objects.hash(created, hash, id, updated, version, a, b);
    }
}

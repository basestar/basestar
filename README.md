![Basestar](https://raw.githubusercontent.com/basestar/basestar/master/etc/header.png)

![master build](https://github.com/basestar/basestar/workflows/master%20build/badge.svg?branch=master) [![maven central](https://maven-badges.herokuapp.com/maven-central/io.basestar/basestar/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.basestar/basestar)

# The modern, declarative data toolkit

Writing CRUD layers is a pain, maintaining generated CRUD layers is a pain, you've defined CRUD APIs a million times, and you want to get on with the interesting parts of your project - you should be able to describe your data, the relationships between your data, and rules for accessing your data succinctly, in one place, and all of the infrastructure and machinery implied by that declaration should just exist.

You shouldn't have to choose between tools that work for your web-app and tools that work for massive scale - online or offline - data processing and analysis. You shouldn't be constrained to one implementation, or trapped by one cloud provider.

Basestar solves a relatively simple problem, but it solves it comprehensively and gives you simplicity and power without taking away flexibility or control.

## Get started

Pick the parts you need, for example:

```xml
<dependencies>
    <dependency>
        <groupId>io.basestar</groupId>
        <artifactId>basestar-database-server</artifactId>
        <version>${basestar.version}</version>
    </dependency>
    <dependency>
        <groupId>io.basestar</groupId>
        <artifactId>basestar-storage-dynamodb</artifactId>
        <version>${basestar.version}</version>
    </dependency>
</dependencies>
```

## Schema first design

See: [Basestar schema (basestar-schema)](basestar-schema)

## Integrated expression language

See: [Basestar expressions (basestar-expression)](basestar-expression)

## Row-level security

Write me

## Multi-value indexes

Write me

## Storage engines

- [DynamoDB](basestar-storage-dynamodb)
- [Cognito users/groups](basestar-storage-cognito)
- [Elasticsearch](basestar-storage-elasticsearch)
- [SQL (dialects as per JOOQ)](basestar-storage-sql)
- [Hazelcast](basestar-storage-hazelcast)
- [S3](basestar-storage-s3)
- [Spark](basestar-storage-spark)


## Examples

```yaml

Principal:
    type: object

User:
    type: object
    extend: Principal
    links:
      groups:
        schema: Group
        expression: member.id == this.id for any member in members

Group:
    type: object
    extend: Principal
    properties:
      members:
        array: User
    indexes:
      over:
        member: members
      partition:
        - member.id

Project:
    type: object
    properties:
      owner:
        type: Principal
    permissions:
      read:
        expression: this.principal.id in {caller.id} + {group.id for group in caller.groups}
        expand:
          - caller.groups

Thing:
  type: object
  properties:
    project:
      type: Project
  permissions:
    read:
      inherit:
        - project.read
        
```


# Building
1. Ensure you have ![Localstack](https://docs.localstack.cloud/get-started/) started
    - if this is the first time, this will get localstack up and running:
   ```bash
   python3 -m pip install localstack
   docker pull localstack/localstack
   docker pull amazon/dynamodb-local:latest
   docker pull docker.elastic.co/elasticsearch/elasticsearch:7.8.0
   localstack start
   ```
2. Ensure you have the leveldb 1.8 native library installed on your system if you can
   (failure to provide this will result in testing against a slower, Java-based implementation of LevelDB)
3. Build and run tests
```bash
mvn test
```


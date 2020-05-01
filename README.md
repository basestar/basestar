![Basestar](https://raw.githubusercontent.com/basestar/basestar/master/etc/header.png)


# The modern, declarative data toolkit

Writing CRUD layers is a pain, maintaining generated CRUD layers is a pain, you've definied CRUD APIs a million times, and you want to get on with the interesting parts of your project - you should be able to describe your data, the relationships between your data, and rules for accessing your data succinctly, in one place, and all of the infrastructure and machinery implied by that declaration should just exist.

You shouldn't have to choose between tools that work for your web-app and tools that work for massive scale - online or offline - data processing and analysis. You shouldn't be constrained to one implementation, or trapped by one cloud provider.

Basestar solves a relatively simple problem, but it solves it comprehensively and gives you simplicity and power without taking away flexibility or control.

## Schema first design

Write me

## Integrated expression language

Write me

## Row-level security

Write me

## Multi-value indexes

Write me

## Storage engines

- DynamoDB
- Cognito users/groups
- Elasticsearch
- SQL (dialects as per JOOQ)
- Hazelcast
- S3
- Hive
- Spark


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

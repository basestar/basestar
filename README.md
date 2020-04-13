# Basestar

**Example:**

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
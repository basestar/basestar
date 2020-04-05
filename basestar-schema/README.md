# Schema

## Namespace


Container for schema objects

<strong>Example</strong>
<pre>
MyObject:
  type: object
MyStruct:
  type: struct
</pre>

## Schema


Base type for schema definitions

**Fields**

- `type` **"object"** | **"struct"** | **"enum"**

  Indicates whether this definition is an [Object](#object), [Struct](#struct) or [Enum](#enum) schema.


### Object Schema


Objects are persisted by reference, and may be polymorphic.

Objects may contain properties, transients and links, these member types share the same
namespace, meaning you cannot define a property and a transient or link with the same name.

<strong>Example</strong>
<pre>
MyObject:
  type: object
  properties:
     myProperty1:
         type: string
</pre>

**Fields**

- `version` **long**

  Current version of the schema, defaults to 1

- `extend` **string (name)**

  Parent schema, may be another object schema or a struct schema

- `description` **string**

  Description of the schema

- `allProperties` **map of [Property](#property)**

  Map of property definitions (shares namespace with transients and links)

- `allTransients` **map of [Transient](#transient)**

  Map of link definitions (shares namespace with properties and transients)

- `allLinks` **map of [Link](#link)**

  Map of link definitions (shares namespace with properties and transients)

- `allIndexes` **map of [Index](#index)**

  Map of index definitions

- `allPermissions` **map of [Permission](#permission)**

  


### Struct Schema


Struct schemas may have properties, but no transients, links etc. A struct is persisted by value.

When a struct type is referenced as a property type, it is static (non-polymorphic), that is
when persisting only the fields of the declared type can be stored.

<strong>Example</strong>
<pre>
MyStruct:
  type: struct
  properties:
     myProperty1:
         type: string
</pre>

**Fields**

- `version` **long**

  Current version of the schema, defaults to 1

- `extend` **io.basestar.schema.StructSchema**

  Parent schema, may be another struct schema only

- `description` **string**

  Description of the schema

- `allProperties` **map of [Property](#property)**

  Map of property definitions


### Enum Schema


Enum schemas may be used to constrain strings to a predefined set of values. They are persisted by-value.

<strong>Example</strong>
<pre>
MyEnum:
  type: enum
  values:
  - VALUE1
  - VALUE2
  - VALUE3
</pre>

**Fields**

- `description` **string**

  Text description

- `values` **array of string**

  Valid values for the enumeration (case sensitive)



## Property



**Fields**

- `description` **string**

  

- `type` **[Type Use](#type-use)**

  

- `required` **boolean**

  

- `immutable` **boolean**

  

- `expression` **Expression**

  

- `constraints` **map of [Constraint](#constraint)**

  

- `visibility` **[Visibility](#visibility)**

  


## Transient



**Fields**

- `description` **string**

  

- `expression` **Expression**

  

- `visibility` **[Visibility](#visibility)**

  


## Link



**Fields**

- `description` **string**

  

- `schema` **io.basestar.schema.ObjectSchema**

  

- `query` **Expression**

  

- `sort` **array of Sort**

  


## Index



**Fields**

- `version` **long**

  

- `description` **string**

  

- `partition` **array of Path**

  

- `sort` **array of Sort**

  

- `projection` **set of string**

  

- `over` **map of Path**

  

- `consistency` **io.basestar.schema.Consistency**

  

- `unique` **boolean**

  

- `sparse` **boolean**

  

- `max` **int**

  


## Permission



**Fields**

- `description` **string**

  

- `anonymous` **boolean**

  

- `expression` **Expression**

  

- `expand` **set of Path**

  

- `inherit` **set of Path**

  


## Type Use



### String Type


<strong>Example</strong>
<pre>
type: string
</pre>

### Object Type


Stores a reference to the object.

<strong>Example</strong>
<pre>
type: MyObject
</pre>

### Integer Type


Stored as 64bit (long) integer.

<strong>Example</strong>
<pre>
type: integer
</pre>

### Array Type


<strong>Example</strong>
<pre>
type:
  array: string
</pre>

### Set Type


<strong>Example</strong>
<pre>
type:
  set: string
</pre>

### Binary Type


Input/output as a Base64 encoded string

<strong>Example</strong>
<pre>
type: binary
</pre>

### Struct Type


Stores a copy of the object, the declared (static) type is used, so properties defined
in a subclass of the declared struct type will be lost.

For polymorphic storage, an Object type must be used.

<strong>Example</strong>
<pre>
type: MyStruct
</pre>

### Enum Type


<strong>Example</strong>
<pre>
type: MyEnum
</pre>

### Map Type


<strong>Example</strong>
<pre>
type:
  map: string
</pre>

### Number Type


Stored as double-precision floating point.

<strong>Example</strong>
<pre>
type: number
</pre>

### Boolean Type


<strong>Example</strong>
<pre>
type: boolean
</pre>



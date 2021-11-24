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

  Indicates whether this definition is an [Object](#object-schema), [Struct](#struct-schema) or [Enum](#enum-schema) schema.


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

- `qualifiedName` **io.basestar.util.Name**

  

- `slot` **int**

  

- `version` **long**

  Current version of the schema, defaults to 1

- `extend` **string (name)**

  Parent schema, may be another object schema or a struct schema

- `id` **io.basestar.schema.Id**

  Id configuration

- `history` **io.basestar.schema.History**

  History configuration

- `description` **string**

  Description of the schema

- `properties` **map of [Property](#property)**

  Map of property definitions (shares namespace with transients and links)

- `transients` **map of [Transient](#transient)**

  Map of link definitions (shares namespace with properties and transients)

- `links` **map of [Link](#link)**

  Map of link definitions (shares namespace with properties and transients)

- `indexes` **map of [Index](#index)**

  Map of index definitions

- `permissions` **map of [Permission](#permission)**

  

- `expand` **set of io.basestar.util.Name**

  

- `concrete` **boolean**

  

- `extensions` **map of java.lang.Object**

  


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

- `qualifiedName` **io.basestar.util.Name**

  

- `slot` **int**

  

- `version` **long**

  Current version of the schema, defaults to 1

- `extend` **io.basestar.schema.StructSchema**

  Parent schema, may be another struct schema only

- `description` **string**

  Description of the schema

- `properties` **map of [Property](#property)**

  Map of property definitions

- `concrete` **boolean**

  

- `extensions` **map of java.lang.Object**

  


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

- `qualifiedName` **io.basestar.util.Name**

  

- `version` **long**

  Current version of the schema, defaults to 1

- `slot` **int**

  

- `description` **string**

  Text description

- `values` **array of string**

  Valid values for the enumeration (case sensitive)

- `extensions` **map of java.lang.Object**

  
### View Schema

View schemas filter/transform or calculate aggregates for a source Object or View.

<strong>Example</strong>
<pre>
MyView:
  type: view
  values:
  - VALUE1
  - VALUE2
  - VALUE3
</pre>

Each view property must have an expression.

## Property



**Fields**

- `qualifiedName` **io.basestar.util.Name**

  

- `description` **string**

  

- `type` **[Type Use](#type-use)**

  

- `required` **boolean**

  

- `immutable` **boolean**

  

- `defaultValue` **java.lang.Object**

  

- `expression` **Expression**

  

- `constraints` **array of [Constraint](#constraint)**

  

- `visibility` **[Visibility](#visibility)**

  

- `extensions` **map of java.lang.Object**

  


## Transient



**Fields**

- `qualifiedName` **io.basestar.util.Name**

  

- `type` **[Type Use](#type-use)**

  

- `description` **string**

  

- `expression` **Expression**

  

- `visibility` **[Visibility](#visibility)**

  

- `expand` **set of io.basestar.util.Name**

  

- `extensions` **map of java.lang.Object**

  


## Link



**Fields**

- `qualifiedName` **io.basestar.util.Name**

  

- `description` **string**

  

- `schema` **string (name)**

  

- `expression` **Expression**

  

- `single` **boolean**

  

- `sort` **array of Sort**

  

- `visibility` **[Visibility](#visibility)**


- `extensions` **map of java.lang.Object**

## Index

Certain storage engines (DDB, SQL) require indexes for optimal querying, basestar indexes are of the partition/sort
type, where to be a candidate index for a query, equality expressions must exist for all partition properties, and for
the initial sequence of sort properties.

<strong>Example</strong>
<pre>
Address:
properties:
    country:
      type: string?
    state:
      type: string?
    city:
      type: string?
    zip:
      type: string?
indexes:
    Country:
      sparse: true
      partition:
        - country
      sort:
        - city
        - zip
    State:
      sparse: true
      partition:
        - state
      sort:
        - city
        - zip
</pre>

The <code>Country</code> index will support queries of the form <code>country == 'UK'</code>,
<code>country == 'UK' && city == 'London'</code>, <code>country == 'UK' && city == 'London' && zip >= 'SE1'</code>.

**Fields**

- `qualifiedName` **io.basestar.util.Name**


- `version` **long**


- `description` **string**

  

- `partition` **array of io.basestar.util.Name**

  List of properties that form the partition (hash) key

- `sort` **array of Sort**

  Optional list of properties that form the sort (range) key

- `projection` **set of string**

  Optional set of properties to be included in the index record for faster server-side filtering, default is all
  properties

- `over` **map of io.basestar.util.Name**

  Optional closure for multi-valued indexes

- `consistency` **io.basestar.schema.Consistency**

  Storage consistency for the index, default is EVENTUAL

- `extensions` **map of java.lang.Object**

  

- `unique` **boolean**

  If true, this index also acts as a uniqueness constraint on the set of indexed propertie

- `sparse` **boolean**

  If true, records with null values for indexed properties will be excluded from the index (space optimization)

- `max` **int**

  Maximum number of multi-value index records per input record, if this is exceeded the create or update will fail (
  default is 100)


## Permission


Permissions describe - using expressions - the rules for reading, creating, updating and deleting objects.

The variables available in the context of a permission expression depend on the type of the expression, as follows:

- Read
   - `this` the object as it currently appears
- Create
   - `after` the object as it would appear if it were successfully created
- Update
   - `before` the object as it currently appears
   - `after` the object as it would appear if it were successfully updated
- Delete
   - `before` the object as it currently appears

**Fields**

- `description` **string**

  

- `anonymous` **boolean**

  

- `expression` **Expression**

  

- `expand` **set of io.basestar.util.Name**

  

- `inherit` **set of io.basestar.util.Name**

  


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

### 



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

### 



### Map Type


<strong>Example</strong>
<pre>
type:
  map: string
</pre>

### 



### 



### 



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



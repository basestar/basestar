# Expressions


- [Arithmetic expressions](#arithmetic-expressions)
  - [Modulo](#modulo)
  - [Subtract](#subtract)
  - [Negate](#negate)
  - [Divide](#divide)
  - [Pow](#pow)
  - [Add](#add)
  - [Multiply](#multiply)
- [Bitwise expressions](#bitwise-expressions)
  - [Bitwise Left-Shift](#bitwise-left-shift)
  - [Bitwise Right-Shift](#bitwise-right-shift)
  - [Bitwise And](#bitwise-and)
  - [Bitwise Or](#bitwise-or)
  - [Bitwise Xor](#bitwise-xor)
  - [Bitwise Not](#bitwise-not)
- [Comparison expressions](#comparison-expressions)
  - [Less Than](#less-than)
  - [Equals](#equals)
  - [Compare](#compare)
  - [Greater Than or Equal](#greater-than-or-equal)
  - [Greater Than](#greater-than)
  - [Not Equal](#not-equal)
  - [Less Than or Equal](#less-than-or-equal)
- [Functional expressions](#functional-expressions)
  - [If-Else](#if-else)
  - [Member](#member)
  - [Coalesce](#coalesce)
  - [In](#in)
  - [](#)
  - [](#)
  - [Lambda](#lambda)
  - [Index](#index)
  - [With](#with)
  - [](#)
- [Iterator expressions](#iterator-expressions)
  - [Iterate](#iterate)
  - [For All](#for-all)
  - [For Any](#for-any)
  - [Set Comprehension](#set-comprehension)
  - [Object Comprehension](#object-comprehension)
  - [Array Comprehension](#array-comprehension)
  - [Where](#where)
- [Literal expressions](#literal-expressions)
  - [Literal Array](#literal-array)
  - [Literal Set](#literal-set)
  - [Literal Object](#literal-object)
- [Logical expressions](#logical-expressions)
  - [Or](#or)
  - [Not](#not)
  - [And](#and)

## Arithmetic expressions

### Modulo


**Syntax:**
<pre>
lhs % rhs
</pre>
**Parameters:**

- `lhs`:`number` Left hand operand
- `rhs`:`number` Right hand operand


### Subtract


**Syntax:**
<pre>
lhs - rhs
</pre>
**Parameters:**

- `lhs`:`number` Left hand operand
- `rhs`:`number` Right hand operand


### Negate


**Syntax:**
<pre>
-operand
</pre>
**Parameters:**

- `operand`:`number` Operand


### Divide

 Numeric division

**Syntax:**
<pre>
lhs / rhs
</pre>
**Parameters:**

- `lhs`:`number` Left hand operand
- `rhs`:`number` Right hand operand


### Pow

 Numeric exponentiation

**Syntax:**
<pre>
lhs ** rhs
</pre>
**Parameters:**

- `lhs`:`number` Left hand operand
- `rhs`:`number` Right hand operand


### Add

 Numeric addition/String concatenation

**Syntax:**
<pre>
lhs + rhs
</pre>
**Parameters:**

- `lhs`:`number|string` Left hand operand
- `rhs`:`number|string` Right hand operand

**Return type:** number|string

### Multiply

 Numeric multiplication

**Syntax:**
<pre>
lhs * rhs
</pre>
**Parameters:**

- `lhs`:`number` Left hand operand
- `rhs`:`number` Right hand operand



## Bitwise expressions

### Bitwise Left-Shift


**Syntax:**
<pre>
lhs << rhs
</pre>
**Parameters:**

- `lhs`:`integer` Left hand operand
- `rhs`:`integer` Right hand operand


### Bitwise Right-Shift


**Syntax:**
<pre>
lhs >> rhs
</pre>
**Parameters:**

- `lhs`:`integer` Left hand operand
- `rhs`:`integer` Right hand operand


### Bitwise And


**Syntax:**
<pre>
lhs & rhs
</pre>
**Parameters:**

- `lhs`:`integer` Left hand operand
- `rhs`:`integer` Right hand operand


### Bitwise Or


**Syntax:**
<pre>
lhs | rhs
</pre>
**Parameters:**

- `lhs`:`integer` Left hand operand
- `rhs`:`integer` Right hand operand


### Bitwise Xor


**Syntax:**
<pre>
lhs ^ rhs
</pre>
**Parameters:**

- `lhs`:`integer` Left hand operand
- `rhs`:`integer` Right hand operand


### Bitwise Not


**Syntax:**
<pre>
~operand
</pre>
**Parameters:**

- `operand`:`integer` Operand



## Comparison expressions

### Less Than


**Syntax:**
<pre>
lhs < rhs
</pre>
**Parameters:**

- `lhs`:`any` Left hand operand
- `rhs`:`any` Right hand operand


### Equals


**Syntax:**
<pre>
lhs == rhs
</pre>
**Parameters:**

- `lhs`:`any` Left hand operand
- `rhs`:`any` Right hand operand


### Compare


**Syntax:**
<pre>
lhs <=> rhs
</pre>
**Parameters:**

- `lhs`:`any` Left hand operand
- `rhs`:`any` Right hand operand


### Greater Than or Equal


**Syntax:**
<pre>
lhs >= rhs
</pre>
**Parameters:**

- `lhs`:`any` Left hand operand
- `rhs`:`any` Right hand operand


### Greater Than


**Syntax:**
<pre>
lhs > rhs
</pre>
**Parameters:**

- `lhs`:`any` Left hand operand
- `rhs`:`any` Right hand operand


### Not Equal


**Syntax:**
<pre>
lhs != rhs
</pre>
**Parameters:**

- `lhs`:`any` Left hand operand
- `rhs`:`any` Right hand operand


### Less Than or Equal


**Syntax:**
<pre>
lhs <= rhs
</pre>
**Parameters:**

- `lhs`:`any` Left hand operand
- `rhs`:`any` Right hand operand



## Functional expressions

### If-Else

 Ternary operator

**Syntax:**
<pre>
predicate ? then : otherwise
</pre>
**Parameters:**

- `predicate`:`boolean` Left hand operand
- `then`:`expression` Evaluated if true
- `otherwise`:`expression` Evaluated if false


### Member


**Syntax:**
<pre>
with.member
</pre>
**Parameters:**

- `with`:`any` Left hand operand
- `member`:`collection` Right hand operand


### Coalesce

 Return the first non-null operand

**Syntax:**
<pre>
lhs ?? rhs
</pre>
**Parameters:**

- `lhs`:`any` Left hand operand
- `rhs`:`any` Right hand operand


### In

 Set membership

**Syntax:**
<pre>
lhs in rhs
</pre>
**Parameters:**

- `lhs`:`any` Left hand operand
- `rhs`:`collection` Right hand operand


### 


**Syntax:**
<pre>

</pre>
**Parameters:**



### 


**Syntax:**
<pre>

</pre>
**Parameters:**



### Lambda


**Syntax:**
<pre>
(args...) -> yield
</pre>
**Parameters:**

- `args`:`string` 
- `yield`:`expression` 


### Index


**Syntax:**
<pre>
lhs[rhs]
</pre>
**Parameters:**

- `lhs`:`collection|object` Left hand operand
- `rhs`:`any` Right hand operand


### With

 Call the provided expression with additional bound variables

**Syntax:**
<pre>

</pre>
**Parameters:**



### 


**Syntax:**
<pre>

</pre>
**Parameters:**




## Iterator expressions

### Iterate

 Create an iterator

**Syntax:**
<pre>
key:value of expr
</pre>
**Parameters:**

- `key`:`identifier` Name-binding for key
- `value`:`identifier` Name-binding for value
- `expr`:`map` Map to iterate

<pre>
value of expr
</pre>
**Parameters:**

- `value`:`identifier` Name-binding for value
- `expr`:`collection` Collection to iterate


### For All

 Universal Quantification: Returns true if the provided predicate is true for all iterator results

**Syntax:**
<pre>
lhs for all rhs
</pre>
**Parameters:**

- `lhs`:`expression` Predicate
- `rhs`:`iterator` Iterator


### For Any

 Existential Quantification: Returns true if the provided predicate is true for any iterator result

**Syntax:**
<pre>
lhs for any rhs
</pre>
**Parameters:**

- `lhs`:`expression` Predicate
- `rhs`:`iterator` Iterator


### Set Comprehension

 Create a set from an iterator

**Syntax:**
<pre>
[yield for iter]
</pre>
**Parameters:**

- `yield`:`expression` Value-yielding expression
- `iter`:`iterator` Iterator


### Object Comprehension

 Create an object from an iterator

**Syntax:**
<pre>
{yieldKey:yieldValue for iter}
</pre>
**Parameters:**

- `yieldKey`:`expression` Key-yielding expression
- `yieldValue`:`expression` Value-yielding expression
- `iter`:`iterator` Iterator


### Array Comprehension

 Create an array from an iterator

**Syntax:**
<pre>
[yield for iter]
</pre>
**Parameters:**

- `yield`:`expression` Value-yielding expression
- `iter`:`iterator` Iterator


### Where

 Filter an iterator using a predicate

**Syntax:**
<pre>
lhs where rhs
</pre>
**Parameters:**

- `lhs`:`iterator` Iterator
- `rhs`:`predicate` Expression to be evaluated for each iterator



## Literal expressions

### Literal Array

 Create an array by providing values

**Syntax:**
<pre>
[args...]
</pre>
**Parameters:**

- `args`:`expression` Array values


### Literal Set

 Create a set by providing values

**Syntax:**
<pre>
{args...}
</pre>
**Parameters:**

- `args`:`expression` Array values


### Literal Object

 Create an object by providing keys and values

**Syntax:**
<pre>

</pre>
**Parameters:**




## Logical expressions

### Or

 Logical Disjunction

**Syntax:**
<pre>

</pre>
**Parameters:**


<pre>
lhs || rhs
</pre>
**Parameters:**

- `lhs`:`any` Left hand operand
- `rhs`:`any` Right hand operand

<pre>

</pre>
**Parameters:**



### Not

 Logical Complement

**Syntax:**
<pre>
!operand
</pre>
**Parameters:**

- `operand`:`any` Operand


### And

 Logical Conjunction

**Syntax:**
<pre>

</pre>
**Parameters:**


<pre>
lhs && rhs
</pre>
**Parameters:**

- `lhs`:`any` Left hand operand
- `rhs`:`any` Right hand operand

<pre>

</pre>
**Parameters:**





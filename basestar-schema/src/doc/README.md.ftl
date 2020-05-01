# Schema

<#assign schemaTypes=[
    "io.basestar.schema.ObjectSchema",
    "io.basestar.schema.StructSchema",
    "io.basestar.schema.EnumSchema"
]>

<#assign memberTypes=[
    "io.basestar.schema.Property",
    "io.basestar.schema.Transient",
    "io.basestar.schema.Link",
    "io.basestar.schema.Index",
    "io.basestar.schema.Permission"
]>

<#assign cls=classNamed("io.basestar.schema.Namespace")>
## ${cls.commentText()?keep_before("\n")}

${cls.commentText()?keep_after("\n")?replace("^ ", "", "rm")}

<#assign cls=classNamed("io.basestar.schema.Schema")>
## ${cls.commentText()?keep_before("\n")}

${cls.commentText()?keep_after("\n")?replace("^ ", "", "rm")}

**Fields**

- `type` **"object"** | **"struct"** | **"enum"**

  Indicates whether this definition is an [Object](#object-schema), [Struct](#struct-schema) or [Enum](#enum-schema) schema.

<#list schemaTypes as schemaType>
<#assign cls=classNamed(schemaType)>

### ${cls.commentText()?keep_before("\n")}

${cls.commentText()?keep_after("\n")?replace("^ ", "", "rm")}

**Fields**

<#list cls.fields(false) as field>
<#if !field.isStatic() && (field.name() != "name") && !field.name()?starts_with("declared")>
- `${field.name()}` **<@type_link type=field.type()/>**

  ${field.commentText()}

</#if>
</#list>
</#list>

<#list memberTypes as memberType>
<#assign cls=classNamed(memberType)>

## ${cls.commentText()?keep_before("\n")}

${cls.commentText()?keep_after("\n")?replace("^ ", "", "rm")}

**Fields**

<#list cls.fields(false) as field>
<#if !field.isStatic() && (field.name() != "name") && !field.name()?starts_with("declared")>
- `${field.name()}` **<@type_link type=field.type()/>**

  ${field.commentText()}

</#if>
</#list>
</#list>

<#assign cls=classNamed("io.basestar.schema.use.Use")>
## ${cls.commentText()?keep_before("\n")}

${cls.commentText()?keep_after("\n")?replace("^ ", "", "rm")}

<#list packageNamed("io.basestar.schema.use").ordinaryClasses() as cls>
<#if cls.qualifiedName() != "io.basestar.schema.use.UseNamed">
### ${cls.commentText()?keep_before("\n")}

${cls.commentText()?keep_after("\n")?replace("^ ", "", "rm")}

</#if>
</#list>

<#macro index_type_link type index><#if (type.typeArguments())?? && (type.typeArguments()[index])??><@type_link type=type.typeArguments()[index]/><#else>Unknown</#if></#macro>

<#macro type_link type><#switch type.qualifiedTypeName()>
<#case "java.lang.Boolean">boolean<#break>
<#case "java.lang.Integer">int32<#break>
<#case "java.lang.Long">int64<#break>
<#case "java.lang.String">string<#break>
<#case "io.basestar.schema.InstanceSchema">string (name)<#break>
<#case "io.basestar.schema.Schema">string (name)<#break>
<#case "java.util.Map">map of <@index_type_link type=type index=1/><#break>
<#case "java.util.SortedMap">map of <@index_type_link type=type index=1/><#break>
<#case "java.util.List">array of <@index_type_link type=type index=0/><#break>
<#case "java.util.Set">set of <@index_type_link type=type index=0/><#break>
<#case "java.util.SortedSet">set of <@index_type_link type=type index=0/><#break>
<#case "io.basestar.schema.Property">[Property](#property)<#break>
<#case "io.basestar.schema.Transient">[Transient](#transient)<#break>
<#case "io.basestar.schema.Link">[Link](#link)<#break>
<#case "io.basestar.schema.Index">[Index](#index)<#break>
<#case "io.basestar.schema.Permission">[Permission](#permission)<#break>
<#case "io.basestar.schema.Constraint">[Constraint](#constraint)<#break>
<#case "io.basestar.schema.Visibility">[Visibility](#visibility)<#break>
<#case "io.basestar.schema.use.Use">[Type Use](#type-use)<#break>
<#case "io.basestar.expression.Expression">Expression<#break>
<#case "io.basestar.util.Path">Path<#break>
<#case "io.basestar.util.Sort">Sort<#break>
<#default>${type.qualifiedTypeName()}</#switch></#macro>
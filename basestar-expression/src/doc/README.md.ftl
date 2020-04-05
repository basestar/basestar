# Expressions

<#assign packageNames=[
    "io.basestar.expression.arithmetic",
    "io.basestar.expression.bitwise",
    "io.basestar.expression.compare",
    "io.basestar.expression.function",
    "io.basestar.expression.iterate",
    "io.basestar.expression.literal",
    "io.basestar.expression.logical"
]>

<#list packageNames as packageName>
<#assign package=packageNamed(packageName)>
- [${package.commentText()?keep_before("\n")}](#${package.commentText()?keep_before("\n")?lower_case?replace("\\s+", "-", "r")})
<#list package.ordinaryClasses() as cls>
  - [${cls.commentText()?keep_before("\n")}](#${cls.commentText()?keep_before("\n")?lower_case?replace("\\s+", "-", "r")})
</#list>
</#list>

<#list packageNames as packageName>
<#assign package=packageNamed(packageName)>
## ${package.commentText()?keep_before("\n")}

<#list package.ordinaryClasses() as cls>
### ${cls.commentText()?keep_before("\n")}
${cls.commentText()?keep_after("\n")}

**Syntax:**
<#list cls.constructors() as ctor>
<pre>
${ctor.commentText()?keep_before("\n")}
</pre>
**Parameters:**

<#list ctor.paramTags() as p>
- `${p.parameterName()}`:`${p.parameterComment()?keep_before(" ")}` ${p.parameterComment()?keep_after(" ")}
</#list>

</#list>
<#list cls.methods() as method>
<#if method.name() == "evaluate">
<#list method.tags("@return") as ret>
**Return type:** ${ret.text()}
</#list>
</#if>
</#list>

</#list>

</#list>
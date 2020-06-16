# Basestar Maven plugin

## Goals

### Codegen

#### Description

Generate model code from specified schema files.

Default phase ```generate-sources```

#### Configuration

|Property|Type|Default value|Description|
|---|---|---|---|
|```language```| String | java | Output language (supported: java) |
|```packageName```| String | N/A | Java package name |
|```schemaUrls```| List<String> | N/A | URLs to schema files |
|```outputDirectory```| String | ${project.build.directory}/generated-sources/basestar | Output directory |
|```addSources```| Boolean | false | Should output directory be attached as a source directory? |


#### Example

```xml
<plugin>
    <groupId>io.basestar</groupId>
    <artifactId>basestar-maven-plugin</artifactId>
    <version>${basestar.version}</version>
    <configuration>
        <packageName>io.basestar.example</packageName>
        <schemaUrls>
            <schemaUrl>file://${project.basedir}/src/main/resources/schema.yml</schemaUrl>
        </schemaUrls>
        <addSources>true</addSources>
    </configuration>
    <executions>
        <execution>
            <goals>
                <goal>codegen</goal>
            </goals>
            <phase>generate-sources</phase>
        </execution>
    </executions>
</plugin>
```
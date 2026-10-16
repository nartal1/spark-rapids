# Databricks 18.3 Delta Lake implementation

The DBR 18.3 build profile compiles this module and includes it in the plugin distribution.
It contains the reader, transaction, catalog, and write implementations for this runtime.

Build the Scala 2.13 reactor with Java 17:

```bash
mvn -B -f scala2.13/pom.xml \
  -Ddatabricks -Dbuildver=410db183 -DskipTests -Dmaven.scaladoc.skip clean install
```

This enables compile/build support. Runtime integration coverage is tracked in
NVIDIA/cudf-spark#16282; the DBR 18.3 pre-merge integration-test guard is unchanged.

This repo provides steps to reproduce an innocuous, but showstopping (for us), bug in `fastavro`.

1. Define a schema with a nullable enum field (v1)
2. Write data using v1 schema
3. Add a new symbol to the enum field (v2)
4. Write data using v2 schema
5. Attempt to read the data written with _both_ schemas using the v2 schema
6. Reading the data written with the v1 schema will fail with a `SchemaResolutionError`

Per the [schema resolution](https://avro.apache.org/docs/current/spec.html#Schema+Resolution) section of the Avro spec, 
the v2 schema should be able to read data written with the v1 schema. I have proven that a similar error does not occur
with the Apache Avro libraries for both Java and Python.

This repo includes a [test](test_avro.py) that demonstrates this use case with both Apache Avro and `fastavro` libraries. 
To run it:
```
$ pip install -r requirements.txt
$ pytest -v
```

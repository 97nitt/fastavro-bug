import pytest

from avro.io import BinaryEncoder, BinaryDecoder, DatumWriter, DatumReader
from avro.schema import make_avsc_object
from fastavro import schemaless_writer, schemaless_reader
from six import BytesIO


def schema_v1():
    return {
        "type": "record",
        "name": "TestSchema",
        "fields": [
            {
                "name": "field1",
                "type": [
                    "null",
                    {
                        "type": "enum",
                        "name": "SomeEnum",
                        "symbols": [
                            "one",
                            "two",
                            "three"
                        ]
                    }
                ]
            }
        ]
    }


def schema_v2():
    return {
        "type": "record",
        "name": "TestSchema",
        "fields": [
            {
                "name": "field1",
                "type": [
                    "null",
                    {
                        "type": "enum",
                        "name": "SomeEnum",
                        "symbols": [
                            "one",
                            "two",
                            "three",
                            "four"
                        ]
                    }
                ]
            }
        ]
    }


def avro_encoder(writer_schema, datum):
    buffer = BytesIO()
    encoder = BinaryEncoder(buffer)
    writer = DatumWriter(make_avsc_object(writer_schema))
    writer.write(datum, encoder)
    return buffer.getvalue()


def avro_decoder(writer_schema, reader_schema, encoded):
    buffer = BytesIO(encoded)
    decoder = BinaryDecoder(buffer)
    reader = DatumReader(make_avsc_object(writer_schema), make_avsc_object(reader_schema))
    return reader.read(decoder)


def fastavro_encoder(writer_schema, datum):
    buffer = BytesIO()
    schemaless_writer(buffer, writer_schema, datum)
    return buffer.getvalue()


def fastavro_decoder(writer_schema, reader_schema, encoded):
    buffer = BytesIO(encoded)
    return schemaless_reader(buffer, writer_schema, reader_schema)


class TestAvro:

    @pytest.mark.parametrize(
        argnames="encoder, decoder, writer_schema, reader_schema, datum",
        ids=[
          "Apache Avro: write with v1, read with v2",
          "Apache Avro: write with v2, read with v2",
          "fastavro: write with v1, read with v2",
          "fastavro: write with v2, read with v2"
        ],
        argvalues=[
            (avro_encoder, avro_decoder, schema_v1(), schema_v2(), {"field1": "one"}),
            (avro_encoder, avro_decoder, schema_v2(), schema_v2(), {"field1": "one"}),
            (fastavro_encoder, fastavro_decoder, schema_v1(), schema_v2(), {"field1": "one"}),
            (fastavro_encoder, fastavro_decoder, schema_v2(), schema_v2(), {"field1": "one"})
        ])
    def test_avro_serde(self, encoder, decoder, writer_schema, reader_schema, datum):
        encoded = encoder(writer_schema, datum)
        decoded = decoder(writer_schema, reader_schema, encoded)

        assert len(decoded) == len(datum)
        assert decoded["field1"] == datum["field1"]

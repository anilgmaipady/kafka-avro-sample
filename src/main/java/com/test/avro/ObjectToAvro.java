package com.test.avro;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class ObjectToAvro {

    public static byte[] marshal(Object activity) throws IOException {
        Schema schema = ReflectData.get().getSchema(activity.getClass());
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ReflectDatumWriter< Object > reflectDatumWriter = new ReflectDatumWriter< Object >(schema);
        DataFileWriter< Object > writer = new DataFileWriter< Object >(reflectDatumWriter).create(schema, outputStream);
        writer.append(activity);
        writer.close();
        return outputStream.toByteArray();
    }

    public static < T > T unmarshal(Class< T > returnType, byte[] bytes) throws IOException {
        Schema schema = ReflectData.get().getSchema(returnType);
        ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
        ReflectDatumReader< T > reflectDatumReader = new ReflectDatumReader< T >(schema);
        DataFileStream< T > reader = new DataFileStream< T >(inputStream, reflectDatumReader);
        Object activity = reader.next();
        reader.close();
        inputStream.close();
        return ( T ) activity;
    }
}
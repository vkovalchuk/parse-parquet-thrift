package com.github.vkovalchuk;

import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.ReadSupport;

import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.io.api.RecordMaterializer;

import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type.Repetition;

/**
 * Prints stacktrace on getting 1st double value when using Parquet-Hadoop record reader API.
 * This stacktrace will aloow to see what code path has invoked.
 */
public class DebugRead {

    public static void main(String[] args) throws Exception {
        String fileName = args[0];
        // PAGE @4 len: 916

        MessageType schema = new MessageType("s", new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.DOUBLE, "time"));
        ArrayReadSupport result = new ArrayReadSupport(schema);
        ParquetReader<Object[]> reader = new ParquetReader<>(new Path(fileName), result);
        Object[] v0 = reader.read();
        System.out.println(v0);
    }

}

class ArrayReadSupport extends ReadSupport<Object[]> {

    protected final MessageType schema;

    public ArrayReadSupport(MessageType schema) {
        this.schema = schema;
    }

    @Override
    public ReadContext init(Configuration c, Map<String, String> keyValueMetaData, MessageType fileSchema) {
        return new ReadContext(schema);
    }

    @Override
    public RecordMaterializer<Object[]> prepareForRead(Configuration c, Map<String, String> _1,
            MessageType fileSchema, ReadContext readContext) {
        return new ArrayRecordMaterializer();
    }

    public static class ArrayRecordMaterializer extends RecordMaterializer<Object[]> {

        private final GroupConverter root = new GroupConverter() {
            @Override
            public Converter getConverter(int fieldIndex) {
                return new PrimitiveConverter() {
                    @Override
                    public void addDouble(double value) {
                        throw new RuntimeException("addDouble() called!");
                    }
                };
            }

            @Override
            public void start() {}

            @Override
            public void end() {}
        };

        @Override
        public Object[] getCurrentRecord() {
            return null;
        }

        @Override
        public GroupConverter getRootConverter() {
            return root;
        }
    }
}
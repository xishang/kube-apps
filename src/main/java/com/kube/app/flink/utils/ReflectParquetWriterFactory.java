//package com.kube.app.flink.utils;
//
//
//import org.apache.avro.Schema;
//import org.apache.avro.reflect.ReflectData;
//import org.apache.flink.formats.parquet.ParquetBuilder;
//import org.apache.flink.formats.parquet.ParquetWriterFactory;
//import org.apache.parquet.avro.AvroParquetWriter;
//import org.apache.parquet.hadoop.ParquetWriter;
//import org.apache.parquet.io.OutputFile;
//
//import java.io.IOException;
//
//public class ReflectParquetWriterFactory {
//
//    static class ReflectParquetBuilder<T> implements ParquetBuilder<T> {
//        private Class<T> type;
//
//        public ReflectParquetBuilder(Class<T> type) {
//            this.type = type;
//        }
//
//        public ParquetWriter<T> createWriter(OutputFile out) throws IOException {
//            String schemaString = ReflectData.get().getSchema(type).toString();
//            final Schema schema = new Schema.Parser().parse(schemaString);
//            return AvroParquetWriter.<T>builder(out)
//                    .withSchema(schema)
//                    .withDataModel(ReflectData.get())
//                    .build();
//        }
//    }
//
//    public static <T> ParquetWriterFactory<T> forSchema(Class<T> type) throws IOException {
//        return new ParquetWriterFactory<T>(new ReflectParquetBuilder(type));
//    }
//
//}

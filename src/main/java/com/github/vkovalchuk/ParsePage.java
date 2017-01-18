package com.github.vkovalchuk;


import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Decompressor;

import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.values.ValuesReader;

import org.apache.parquet.format.DataPageHeader;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;


public class ParsePage {

    public static void main(String[] args) throws Exception {
        Path path = new Path(args[0]);
        long pageOffset = Long.parseLong(args[1]);

        printPageHeader(path, pageOffset, PrimitiveTypeName.DOUBLE);
    }

    public static void printPageHeader(Path path, long pageOffset, PrimitiveTypeName type)
        throws Exception
    {
        ParseParquetFooter.log("Opening " + path + ", page: " + pageOffset);

        FSDataInputStream from = openFileStream(path);

        from.seek(pageOffset);

        PageHeader ph = Util.readPageHeader(from);
        System.out.println("PageHeader: " + ph.toString());

        DataPageHeader dataHeaderV1 = ph.getData_page_header();
        if (dataHeaderV1 != null) {
            int page_num_values = dataHeaderV1.getNum_values();
            ValuesReader valuesReader = getDataPageV1ValuesReader(type, from, ph);
            for (int i = 0; i < page_num_values; i++) {
                double d = valuesReader.readDouble();
                System.out.println(":" + d);
            }
            System.out.println("After page:" + from.getPos());
        }
    }

    public static ValuesReader getDataPageV1ValuesReader(PrimitiveTypeName type, FSDataInputStream from, PageHeader ph)
        throws IOException, InstantiationException, IllegalAccessException
    {
        DataPageHeader dataHeaderV1 = ph.getData_page_header();
        int page_num_values = dataHeaderV1.getNum_values();

        int compressedPageSize = ph.compressed_page_size;
        byte[] pageCompressedBytes = new byte[compressedPageSize];
        from.readFully(pageCompressedBytes);

        Class<CompressionCodec> hadoopCodecClass = CompressionCodecName.SNAPPY.getHadoopCompressionCodecClass();
        CompressionCodec codec = hadoopCodecClass.newInstance();
        Decompressor decompressor = codec.createDecompressor();

        decompressor.setInput(pageCompressedBytes, 0, compressedPageSize);

        int uncompressedPageSize = ph.uncompressed_page_size;
        byte[] uncompressedPageBytes = new byte[uncompressedPageSize];
        int db = decompressor.decompress(uncompressedPageBytes, 0, uncompressedPageSize);
        System.out.println("Decompressed " + compressedPageSize + " into " + db);

        BytesInput pageBytesInput = readAsBytesInput(uncompressedPageBytes, 0, uncompressedPageSize);

        org.apache.parquet.format.Statistics statistics = dataHeaderV1.getStatistics();
        org.apache.parquet.column.statistics.Statistics stats = org.apache.parquet.column.statistics.Statistics.getStatsBasedOnType(type);
        stats.setMinMaxFromBytes(statistics.min.array(), statistics.max.array());

        DataPageV1 pdesc = new DataPageV1(
                pageBytesInput,
                page_num_values,
                uncompressedPageSize,
                stats,
                converter.getEncoding(dataHeaderV1.getRepetition_level_encoding()),
                converter.getEncoding(dataHeaderV1.getDefinition_level_encoding()),
                converter.getEncoding(dataHeaderV1.getEncoding())
                );
        System.out.println("DataPage: " + pdesc.toString());

        ColumnDescriptor colDesc = new ColumnDescriptor(null, type, 0, 0);
        ValuesReader valuesReader = pdesc.getValueEncoding().getValuesReader(colDesc, null);
        valuesReader.initFromPage(page_num_values, pdesc.getBytes().toByteArray(), 0);
        return valuesReader;
    }

    static ParquetMetadataConverter converter = new ParquetMetadataConverter();

    public static BytesInput readAsBytesInput(byte[] buf, int pos, int size) throws IOException {
        final BytesInput r = BytesInput.from(buf, pos, size);
        return r;
      }

    public static FSDataInputStream openFileStream(Path path)
        throws IOException, URISyntaxException
    {
        // Yes that is a resource leak, OK for command line tool
        RawLocalFileSystem fileSystem = new RawLocalFileSystem();
        fileSystem.initialize(new URI("file:///"), new Configuration());
        FSDataInputStream original = fileSystem.open(path, 65536);
        return original;
    }

    public static FSDataInputStream createLoggingWrapper(FSDataInputStream original) {
        FSDataInputStream from = new FSDataInputStream(original) {

            @Override
            public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
                System.out.println("readFully(pos=" + position + ", len=" + length + ")");
                super.readFully(position, buffer, offset, length);
            }

            @Override
            public void readFully(long position, byte[] buffer) throws IOException {
                System.out.println("readFully(pos=" + position + ", len=" + buffer.length + ")");
                super.readFully(position, buffer);
            }

            @Override
            public void seek(long desired) throws IOException {
                System.out.println("seek: " + desired);
                super.seek(desired);
            }

            @Override
            public int read(long position, byte[] buffer, int offset, int length) throws IOException {
                System.out.println("read(pos=" + position + ", len=" + length + ")");
                return super.read(position, buffer, offset, length);
            }
        };
        return from;
    }
}

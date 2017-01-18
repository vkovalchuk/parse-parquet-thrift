package com.github.vkovalchuk;

import static org.apache.parquet.bytes.BytesUtils.readIntLittleEndian;
import static org.apache.parquet.hadoop.ParquetFileWriter.MAGIC;

import java.io.*;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.parquet.Log;
//import org.apache.parquet.format.Util;

import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;


public class ParseParquetFooter {

    public static void main(String[] args) throws Exception {
        Path path = new Path(args[0]);
        Configuration conf = new Configuration();

        FileStatus file = FileSystem.get(conf).getFileStatus(path);
        long l = file.getLen();
        log("Opening " + path + ", len: " + l + "...");
        FSDataInputStream from = openFile(file, conf);

        long footerPos = readFooterOffset(from, l);
        float percent = (float)(l - footerPos) / (float)l * 100.0f;
        log("FileMetaData offset: " + footerPos + " (size: " + (l - footerPos) + ", " + percent + "%)");

        from.seek(footerPos);

        log("Reading FileMetadata, wait...");
        ParquetMetadataConverter cvt = new ParquetMetadataConverter();
        org.apache.parquet.format.FileMetaData formatFMD = Util.readFileMetaData(from);

        log("Converting FileMetadata, wait...");
        ParquetMetadata pmd = cvt.fromParquetMetadata(formatFMD);

        FileMetaData fmd = pmd.getFileMetaData();
        log("  pmd.FMD.CreatedBy: " + fmd.getCreatedBy());
        log("  pmd.FMD.KVMD: " + fmd.getKeyValueMetaData());

        List<BlockMetaData> blocks = pmd.getBlocks();
        String colName = args.length > 1 ? args[1] : "APP_r";
        printBlocks(blocks, colName, from);

        from.close();
    }

    static void log(String s) {
        System.out.println(new java.util.Date() + ": " + s);
    }

    public static FSDataInputStream openFile(FileStatus file, Configuration conf)
            throws IOException
    {
        FileSystem fileSystem = file.getPath().getFileSystem(conf);
        FSDataInputStream f = fileSystem.open(file.getPath());
        return f;
    }

    public static long readFooterOffset(FSDataInputStream f, long l) throws IOException {
        int FOOTER_LENGTH_SIZE = 4;
        long footerLengthIndex = l - FOOTER_LENGTH_SIZE - MAGIC.length;

        f.seek(footerLengthIndex);
        int footerLength = readIntLittleEndian(f);
        byte[] magic = new byte[MAGIC.length];
        f.readFully(magic);

        long footerIndex = footerLengthIndex - footerLength;

        return footerIndex;
    }

    static boolean firstBlock = true;

    public static void printBlocks(List<BlockMetaData> blocks, String colName, FSDataInputStream from) throws IOException {
        int colIndex = -1;
        log("  Total pmd.Blocks: " + blocks.size());
        boolean printAllColumns = "*".equals(colName);
        for (BlockMetaData b : blocks) {
            List<ColumnChunkMetaData> columns = b.getColumns();
            log("Block: start: " + b.getStartingPos() + ", rows: " + b.getRowCount() + ", cols: " + columns.size() +
                    ", total sz: " + b.getTotalByteSize() + ", compressed sz: " + b.getCompressedSize());
            if (printAllColumns) {
                int index = 0;
                for (ColumnChunkMetaData ccmd :columns) {
                    printColumnMetadata(ccmd.getPath().toDotString(), index++, ccmd);
                }
            } else {
                colIndex = findColumn(columns, colName, colIndex);
                if (colIndex >= 0) {
                    ColumnChunkMetaData colMd = columns.get(colIndex);
                    printColumnMetadata(colName, colIndex, colMd);
                    if (firstBlock) {
                        long startingPos = colMd.getStartingPos();
                        int totalSize = (int) colMd.getTotalSize();
                        CompressionCodecName codec = colMd.getCodec();
                        printData(startingPos, totalSize, codec, from);
                    }
                    firstBlock = false;
                }
            }
        }
    }

    public static int findColumn(List<ColumnChunkMetaData> columns, String colName, int colIndex) {
        // shortcut
        if (colIndex >= 0 && nameEquals(columns, colIndex, colName)) {
            return colIndex;
        }
        // find by iteration
        for (int i = 0; i < columns.size(); i++) {
            if (nameEquals(columns, i, colName)) {
                return i;
            }
        }
        return -1;
    }

    public static boolean nameEquals(List<ColumnChunkMetaData> columns, int colIndex, String colName) {
        ColumnChunkMetaData colMd = columns.get(colIndex);
        String[] pathArr = colMd.getPath().toArray();
        boolean matches = pathArr.length == 1 && pathArr[0].equals(colName);
        return matches;
    }

    public static void printColumnMetadata(String colName, int index, ColumnChunkMetaData colMd) {
        log(" col " + colName + ": #" + index + ", start: " + colMd.getStartingPos() +
                ", FDP: " + colMd.getFirstDataPageOffset() + ", sz: " + colMd.getTotalSize() +
                ", valuesCount: " + colMd.getValueCount() + ", dict off: " + colMd.getDictionaryPageOffset() +
                ", codec: " + colMd.getCodec() + ", enc: " + colMd.getEncodings());
    }

    public static void printData(long startingPos, int totalSize, CompressionCodecName codec, FSDataInputStream from)
            throws IOException
    {
        byte[] buffer = new byte[totalSize];
        from.readFully(startingPos, buffer, 0, totalSize);
        System.out.println("    PAGE @" + startingPos + " len: " + totalSize);

        System.out.print("    DATA: ");
        for (int i = 0; i < buffer.length; i++) {
            System.out.printf("|%02x", buffer[i]);
        }
        System.out.println();

        if (codec == CompressionCodecName.SNAPPY) {
            SnappyCodec c = new SnappyCodec();
            CompressionInputStream is = c.createInputStream(new ByteArrayInputStream(buffer));
            byte[] out = new byte[buffer.length];
            is.read(out);
            System.out.print("    UNCOMPR DATA: ");
            for (int i = 0; i < buffer.length; i++) {
                System.out.printf("|%02x", buffer[i]);
            }
        }
    }

}

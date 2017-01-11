package com.github.vkovalchuk;

import static com.github.vkovalchuk.ParseParquetFooter.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.PageHeader;

import parquet.org.apache.thrift.protocol.TProtocol;


/**
 * Only prints data for the specified column. Parsing of the FileMetaData is minimal, only required objects.
 */
public class PrintColumnData {

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: PrintColumnData parquet_file_name column_name");
            System.exit(1);
        }

        Path path = new Path(args[0]);
        String columnName = args[1];

        Configuration conf = new Configuration();
        FileStatus file = FileSystem.get(conf).getFileStatus(path);
        long l = file.getLen();
        log("Opening " + path + ", len: " + l + "...");
        FSDataInputStream from = openFile(file, conf);

        long footerPos = readFooterOffset(from, l);
        log("FileMetaData offset: " + footerPos + " (size: " + (l - footerPos) + ")");

        from.seek(footerPos);

        TProtocol proto = Util.createProtocol(from, false);
        FileMetaData fmd = Util.readColumnChunkFileMetaData(proto, columnName);
        ColumnChunk columnChunk = fmd.getRow_groups().get(0).getColumns().get(0);
        long file_offset = columnChunk.getFile_offset();
        CompressionCodec codec = columnChunk.getMeta_data().getCodec();
        log("Found ColumnChunk for " + columnName + ": offset " + file_offset + ", codec: " + codec);

        from.seek(file_offset);
        PageHeader ph = Util.readPageHeader(from);
        log(ph.toString());
    }
}

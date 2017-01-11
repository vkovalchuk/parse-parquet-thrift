package com.github.vkovalchuk;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.RowGroup;

import com.github.vkovalchuk.TDebuggingProtocol.FieldType;

import parquet.org.apache.thrift.TBase;
import parquet.org.apache.thrift.TException;
import parquet.org.apache.thrift.protocol.TCompactProtocol;
import parquet.org.apache.thrift.protocol.TList;
import parquet.org.apache.thrift.protocol.TProtocol;
import parquet.org.apache.thrift.transport.TIOStreamTransport;


class Util {

    /**
     * Copy-pasted implementation from {@code org.apache.parquet.format.Util#readFileMetaData(java.io.InputStream)}
     * @param from
     * @return
     * @throws IOException
     */
    public static FileMetaData readFileMetaData(FSDataInputStream from) throws IOException {
        return read(from, new FileMetaData(), TDebuggingProtocol.ROOT_FMD_TYPE);
    }

    public static PageHeader readPageHeader(FSDataInputStream from) throws IOException {
        return read(from, new PageHeader(), TDebuggingProtocol.ROOT_PAGE_TYPE);
    }

    private static <T extends TBase<?,?>> T read(FSDataInputStream from, T tbase, FieldType root) throws IOException {
        TDebuggingProtocol proto = new TDebuggingProtocol(new TIOStreamTransport(from), from, root);
        try {
          tbase.read(proto);
          return tbase;
        } catch (TException e) {
          throw new IOException("can not read " + tbase.getClass() + ": " + e.getMessage(), e);
        }
    }

    public static TProtocol createProtocol(FSDataInputStream from, boolean debugging)
        throws IOException
    {
        return new TCompactProtocol(new TIOStreamTransport(from));
    }

    public static FileMetaData readColumnChunkFileMetaData(TProtocol proto, String colName) throws IOException {
        FileMetaData result = new FileMetaData();
        try {
            readIntoFileMetaData(proto, result, colName);
          } catch (TException e) {
            throw new IOException("can not read FileMetaData: " + e.getMessage(), e);
          }
        return result;
      }

    private static void readIntoFileMetaData(TProtocol iprot, FileMetaData result, String colName) throws TException {
        iprot.readStructBegin();
        while (true) {
          parquet.org.apache.thrift.protocol.TField field = iprot.readFieldBegin();
          if (field.type == parquet.org.apache.thrift.protocol.TType.STOP) {
            break;
          }
          switch (field.id) {
            case 4: // ROW_GROUPS
                result.row_groups = new ArrayList<>();
                TList list = iprot.readListBegin();
                for (int i = 0; i < list.size; i++) {
                    RowGroup rowGroup = new RowGroup();
                    boolean finished = readIntoRowGroup(iprot, rowGroup, colName);
                    if (finished) {
                        result.row_groups.add(rowGroup);
                        return;
                    }
                }
                iprot.readListEnd();
                break;
            default:
                skip(iprot, field.type);
          }
          iprot.readFieldEnd();
        }
        iprot.readStructEnd();
    }

    private static boolean readIntoRowGroup(TProtocol iprot, RowGroup rowGroup, String colName) throws TException {
        iprot.readStructBegin();
        while (true) {
            parquet.org.apache.thrift.protocol.TField field = iprot.readFieldBegin();
            if (field.type == parquet.org.apache.thrift.protocol.TType.STOP) {
                break;
            }
            switch (field.id) {
                case 1: // COLUMNS
                    TList list = iprot.readListBegin();
                    rowGroup.columns = new ArrayList<>(); // 1 column only, not list.size);
                    for (int i = 0; i < list.size; i++) {
                        ColumnChunk columnChunk = new ColumnChunk();
                        columnChunk.read(iprot);
                        List<String> name_path = columnChunk.getMeta_data().getPath_in_schema();
                        if (name_path.size() == 1 && name_path.get(0).equals(colName)) {
                            rowGroup.columns.add(columnChunk);
                            return true;
                        }
                    }
                    iprot.readListEnd();
                    break;
                case 2: // TOTAL_BYTE_SIZE
                    rowGroup.total_byte_size = iprot.readI64();
                    break;
                case 3: // NUM_ROWS
                    rowGroup.num_rows = iprot.readI64();
                    break;
                default:
                    skip(iprot, field.type);
            }
            iprot.readFieldEnd();
        }
        iprot.readStructEnd();
        return false;
    }

    private static void skip(TProtocol iprot, byte type) throws TException {
        parquet.org.apache.thrift.protocol.TProtocolUtil.skip(iprot, type);
    }

}
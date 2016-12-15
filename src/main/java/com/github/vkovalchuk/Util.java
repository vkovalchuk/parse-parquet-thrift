package com.github.vkovalchuk;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.parquet.format.InterningProtocol;
//import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.format.FileMetaData;

import parquet.org.apache.thrift.TBase;
import parquet.org.apache.thrift.TException;
import parquet.org.apache.thrift.transport.TIOStreamTransport;


class Util {
    public static FileMetaData readFileMetaData(FSDataInputStream from) throws IOException {
        return read(from, new FileMetaData());
    }

    private static <T extends TBase<?,?>> T read(FSDataInputStream from, T tbase) throws IOException {
        TDebuggingProtocol proto = new TDebuggingProtocol(new TIOStreamTransport(from), from);
        try {
          tbase.read(proto);
          return tbase;
        } catch (TException e) {
          throw new IOException("can not read " + tbase.getClass() + ": " + e.getMessage(), e);
        }
    }

}
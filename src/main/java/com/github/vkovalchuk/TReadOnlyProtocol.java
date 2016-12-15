package com.github.vkovalchuk;

import java.nio.ByteBuffer;

import parquet.org.apache.thrift.TException;
import parquet.org.apache.thrift.protocol.TField;
import parquet.org.apache.thrift.protocol.TList;
import parquet.org.apache.thrift.protocol.TMap;
import parquet.org.apache.thrift.protocol.TMessage;
import parquet.org.apache.thrift.protocol.TProtocol;
import parquet.org.apache.thrift.protocol.TSet;
import parquet.org.apache.thrift.protocol.TStruct;
import parquet.org.apache.thrift.transport.TTransport;

public abstract class TReadOnlyProtocol extends TProtocol {

    public TReadOnlyProtocol(TTransport trans) {
        super(trans);
    }

    @Override
    public void writeBinary(ByteBuffer arg0) throws TException {
        throw new UnsupportedOperationException("write not supported");
    }

    @Override
    public void writeBool(boolean arg0) throws TException {
        throw new UnsupportedOperationException("write not supported");
    }

    @Override
    public void writeByte(byte arg0) throws TException {
        throw new UnsupportedOperationException("write not supported");
    }

    @Override
    public void writeDouble(double arg0) throws TException {
        throw new UnsupportedOperationException("write not supported");
    }

    @Override
    public void writeFieldBegin(TField arg0) throws TException {
        throw new UnsupportedOperationException("write not supported");
    }

    @Override
    public void writeFieldEnd() throws TException {
        throw new UnsupportedOperationException("write not supported");
    }

    @Override
    public void writeFieldStop() throws TException {
        throw new UnsupportedOperationException("write not supported");
    }

    @Override
    public void writeI16(short arg0) throws TException {
        throw new UnsupportedOperationException("write not supported");
    }

    @Override
    public void writeI32(int arg0) throws TException {
        throw new UnsupportedOperationException("write not supported");
    }

    @Override
    public void writeI64(long arg0) throws TException {
        throw new UnsupportedOperationException("write not supported");
    }

    @Override
    public void writeListBegin(TList arg0) throws TException {
        throw new UnsupportedOperationException("write not supported");
    }

    @Override
    public void writeListEnd() throws TException {
        throw new UnsupportedOperationException("write not supported");
    }

    @Override
    public void writeMapBegin(TMap arg0) throws TException {
        throw new UnsupportedOperationException("write not supported");
    }

    @Override
    public void writeMapEnd() throws TException {
        throw new UnsupportedOperationException("write not supported");
    }

    @Override
    public void writeMessageBegin(TMessage arg0) throws TException {
        throw new UnsupportedOperationException("write not supported");
    }

    @Override
    public void writeMessageEnd() throws TException {
        throw new UnsupportedOperationException("write not supported");
    }

    @Override
    public void writeSetBegin(TSet arg0) throws TException {
        throw new UnsupportedOperationException("write not supported");
    }

    @Override
    public void writeSetEnd() throws TException {
        throw new UnsupportedOperationException("write not supported");
    }

    @Override
    public void writeString(String arg0) throws TException {
        throw new UnsupportedOperationException("write not supported");
    }

    @Override
    public void writeStructBegin(TStruct arg0) throws TException {
        throw new UnsupportedOperationException("write not supported");
    }

    @Override
    public void writeStructEnd() throws TException {
        throw new UnsupportedOperationException("write not supported");
    }

}
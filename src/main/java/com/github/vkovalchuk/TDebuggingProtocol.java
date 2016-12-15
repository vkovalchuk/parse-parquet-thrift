package com.github.vkovalchuk;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Stack;

import org.apache.hadoop.fs.FSDataInputStream;

import parquet.org.apache.thrift.TException;
import parquet.org.apache.thrift.protocol.TCompactProtocol;
import parquet.org.apache.thrift.protocol.TField;
import parquet.org.apache.thrift.protocol.TList;
import parquet.org.apache.thrift.protocol.TMap;
import parquet.org.apache.thrift.protocol.TMessage;
import parquet.org.apache.thrift.protocol.TSet;
import parquet.org.apache.thrift.protocol.TStruct;
import parquet.org.apache.thrift.transport.TTransport;

class TDebuggingProtocol extends TReadOnlyProtocol {

    static class FieldType {
        public String name;
        public boolean isList;
        public FieldType(String name, boolean isList) {
            this.name = name; this.isList = isList;
        }
        public boolean isStandard() {
            return name.equals("i32") || name.equals("i64") || name.equals("string");
        }
    }
    private static final FieldType ROOT_TYPE = new FieldType("FileMetaData", false);

    private static final ParquetThriftStructDictionary dict = new ParquetThriftStructDictionary();

    private final TCompactProtocol delegate;
    private final FSDataInputStream from;
    private final long startingPos;

    private int indent = 0;
    private Stack<FieldType> structs = new Stack<>();

    public TDebuggingProtocol(TTransport transport, FSDataInputStream from) throws IOException {
      super(transport);
      delegate = new TCompactProtocol(transport);
      this.from = from;
      startingPos = from.getPos();
      structs.push(ROOT_TYPE);
    }

    void log(String msg) {
        long pos = getPos();
        String indentSpaces = getIndent();
        System.out.printf("@%9d: %s%s%n", pos, indentSpaces, msg);
    }

    protected String getIndent() {
        String indentSpaces = "";
        for (int i = 0; i < indent; i++) {
            indentSpaces += "  ";
        }
        return indentSpaces;
    }

    <T> T log(String msg, T s) {
        long pos = getPos();
        String indentSpaces = getIndent();
        System.out.printf("@%9d: %s%s: %s%n", pos, indentSpaces, msg, String.valueOf(s));
        return s;
    }

    protected long getPos() {
        long pos = -2; // means ERROR
        try {
            pos = from.getPos() - startingPos;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return pos;
    }

    @Override
    public TStruct readStructBegin()
        throws TException
    {
        FieldType currentType = structs.peek();
        log("struct " + currentType.name + " {");
        indent++;
        return delegate.readStructBegin();

    }

    @Override
    public void readStructEnd()
        throws TException
    {
        indent--;
        FieldType currentType = structs.peek();
        log("} " + currentType.name);
        delegate.readStructEnd();
    }

    @Override
    public TList readListBegin()
        throws TException
    {
        indent++;
        log("List (");
        indent++;
        return delegate.readListBegin();
    }

    @Override
    public void readListEnd()
        throws TException
    {
        indent--;
        log(") List");
        delegate.readListEnd();
        indent--;
    }

    @Override
    public TField readFieldBegin()
        throws TException
    {
        TField fld = delegate.readFieldBegin();
        if (fld.id == 0) { // TSTOP
            // log("  TSTOP;");
            return fld;
        }

        FieldType parentType = structs.peek();
        String fieldName = dict.getFieldName(parentType.name, fld.id);
        log("field " + fieldName + " =");

        boolean isKnownType = !fieldName.startsWith("#");
        if (isKnownType) {
            String[] fldPair = fieldName.split(":");
            String fldType = fldPair[1].trim();
            boolean isList = fldType.startsWith("List ");
            if (isList) {
                fldType = fldType.substring("List ".length());
            }
            FieldType newType = new FieldType(fldType, isList);
            structs.push(newType);
        }

        return fld;
    }

    @Override
    public void readFieldEnd()
        throws TException
    {
        // log("  ;");
        delegate.readFieldEnd();
        structs.pop();
    }

    @Override
    public String readString()
        throws TException
    {
        return log("  string", delegate.readString());
    }

    @Override
    public ByteBuffer readBinary()
        throws TException
    {
        ByteBuffer buf = delegate.readBinary();
        byte[] bytes = buf.array();

        String rep = "";
        boolean isUTF8 = checkAllPrintable(bytes);
        if (isUTF8) {
            rep = new String(bytes);
        } else {
            for (int i = 0; i < bytes.length; i++) {
                rep += String.format(" %02x", bytes[i]);
            }
        }
        log("  bin", "[" + rep + "]");
        return buf;
    }

    public static boolean checkAllPrintable(byte[] bytes) {
        for (byte b : bytes) {
            if ((b < 0x20 && b != 13 && b != 10)|| b >= 127)
                return false;
        }
        return true;
    }

    @Override
    public int readI32()
        throws TException
    {
        FieldType currentType = structs.peek();
        String immediateType = currentType.name;
        if (immediateType.equals("i32")) {
            return log("  i32", delegate.readI32());
        }

        int value = delegate.readI32();
        String enumValueName = dict.getEnumName(immediateType, value);
        log("    " + enumValueName);
        return value;
    }

    @Override
    public long readI64()
        throws TException
    {
        return log("    i64", delegate.readI64());
    }

    // ======================================================
    @Override
    public TMap readMapBegin()
        throws TException
    {
        log("readMapBegin");
        return delegate.readMapBegin();
    }

    @Override
    public void readMapEnd()
        throws TException
    {
        log("readMapEnd");
        delegate.readMapEnd();
    }

    @Override
    public TMessage readMessageBegin()
        throws TException
    {
        log("readMessageBegin");
        return delegate.readMessageBegin();
    }

    @Override
    public void readMessageEnd()
        throws TException
    {
        log("readMessageEnd");
        delegate.readMessageEnd();
    }

    @Override
    public TSet readSetBegin()
        throws TException
    {
        log("readSetBegin");
        return delegate.readSetBegin();
    }

    @Override
    public void readSetEnd()
        throws TException
    {
        log("readSetEnd");
        delegate.readSetEnd();
    }

    @Override
    public short readI16()
        throws TException
    {
        log("readI16");
        return delegate.readI16();
    }

    // -----------------------------------------------
    @Override
    public boolean readBool()
        throws TException
    {
        return log("readBool", delegate.readBool());
    }

    @Override
    public byte readByte()
        throws TException
    {
        return log("readByte", delegate.readByte());
    }

    @Override
    public double readDouble()
        throws TException
    {
        log("readDouble");
        return delegate.readDouble();
    }

}
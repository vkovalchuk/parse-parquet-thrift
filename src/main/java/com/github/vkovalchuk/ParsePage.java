package com.github.vkovalchuk;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.parquet.format.PageHeader;


public class ParsePage {

    public static void main(String[] args) throws Exception {
        Path path = new Path(args[0]);
        long pageOffset = Long.parseLong(args[1]);

        printPageHeader(path, pageOffset);
    }

    public static void printPageHeader(Path path, long pageOffset)
        throws IOException, URISyntaxException
    {
        ParseParquetFooter.log("Opening " + path + ", page: " + pageOffset);

        FSDataInputStream from = openFileStream(path);

        from.seek(pageOffset);

        PageHeader ph = Util.readPageHeader(from);
        System.out.println("PageHeader: " + ph.toString());
    }

    public static FSDataInputStream openFileStream(Path path)
        throws IOException, URISyntaxException
    {
        RawLocalFileSystem fileSystem = new RawLocalFileSystem();
        fileSystem.initialize(new URI("file:///"), new Configuration());
        FSDataInputStream original = fileSystem.open(path, 65536);
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

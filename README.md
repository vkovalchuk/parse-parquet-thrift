# parse-parquet-thrift

## ParseParquetFooter

* This tool is similar to "parquet-tools meta";
* Accepts 2 parameters, parquet file URI and optional column name (defaults to APP_r);
* it reads parquet's FileMetaData, prints CreatedBy, KeyValueMetaData and then file blocks with their details (offset, size);
* If it finds specified column then prints data page from the first block;
* If column name was "\*" then prints details for all columns in all blocks.

### Usage
java -classpath target\parquet-struct-jar-with-dependencies.jar com.github.vkovalchuk.ParseParquetFooter part-0

### Sample output

> @        0: struct FileMetaData {  
> @        1:   field version: i32 =  
> @        2:     i32: 1  
> @        3:   field schema: List SchemaElement =  
> @        3:     List (  
> @        5:       struct SchemaElement {  
> @        6:         field name: string =  
> @       47:           string: bda46dae-7fc5-42b3-a6d4-4dee6c64146c-5-0  
> @       48:         field num_children: i32 =  
> @       50:           i32: 90  
> @       51:       }  
> @       51:       struct SchemaElement {  
> @       52:         field type: Type =  
> @       53:             DOUBLE   
> @       54:         field repetition_type: FieldRepetitionType =  
> @       55:             required  
> @       56:         field name: string =  
> @       61:           string: time  
> @       62:       }  
> ...  
> @     8912:       } RowGroup  
> @     8912:     )  
> @     8913:   field created_by: string =  
> @     8938:     string: parquet-mr version 1.6.0  
> @     8939: } FileMetaData  
> Tue Jan 17 16:24:08 MSK 2017: Converting FileMetadata, wait...  
> Tue Jan 17 16:24:08 MSK 2017:   pmd.FMD.CreatedBy: parquet-mr version 1.6.0  
> Tue Jan 17 16:24:08 MSK 2017:   pmd.FMD.KVMD: {}  
> Tue Jan 17 16:24:08 MSK 2017:   Total pmd.Blocks: 1  
> Tue Jan 17 16:24:08 MSK 2017: Block: start: 4, rows: 47, cols: 87, total sz: 36367, compressed sz: 6342  

## PrintColumnData
* Usage: PrintColumnData parquet_file_name column_name;
* prints data for the specified column;
* Parsing of the FileMetaData is minimal, only required objects.

### Sample output (wrapped)
> Tue Jan 17 16:41:50 MSK 2017: PageHeader(type:DATA_PAGE,
>  uncompressed_page_size:408,
>  compressed_page_size:202,
>  data_page_header:DataPageHeader(num_values:51,
> 	 encoding:PLAIN,
> 	 definition_level_encoding:BIT_PACKED,
> 	 repetition_level_encoding:BIT_PACKED,
> 	 statistics:Statistics(max:00 00 00 00 00 00 59 40, min:00 00 00 00 00 00 49 40,
> 	 null_count:0
> )))

## ParsePage
* Similar to PrintColumnData but uses page offset, not column name;
* Does not parse file metadata!

## DebugRead
* Intended to print stack trace from consumers of "time" column; this is helpful to see what code parts were invoked;
* Uses ParquetReader API to open specified file, throws Exception from double value consumer.

### Output
> Exception in thread "main" org.apache.parquet.io.ParquetDecodingException: Can not read value at 1 in block 0 in fi
>         at org.apache.parquet.hadoop.InternalParquetRecordReader.nextKeyValue(InternalParquetRecordReader.java:243)
>         at org.apache.parquet.hadoop.ParquetReader.read(ParquetReader.java:125)
>         at org.apache.parquet.hadoop.ParquetReader.read(ParquetReader.java:129)
>         at com.github.vkovalchuk.DebugRead.main(DebugRead.java:33)
> Caused by: java.lang.RuntimeException: addDouble() called!
>         at com.github.vkovalchuk.ArrayReadSupport$ArrayRecordMaterializer$1$1.addDouble(DebugRead.java:66)
>         at org.apache.parquet.column.impl.ColumnReaderImpl$2$2.writeValue(ColumnReaderImpl.java:234)
>         at org.apache.parquet.column.impl.ColumnReaderImpl.writeCurrentValueToConverter(ColumnReaderImpl.java:371)
>         at org.apache.parquet.io.RecordReaderImplementation.read(RecordReaderImplementation.java:405)
>         at org.apache.parquet.hadoop.InternalParquetRecordReader.nextKeyValue(InternalParquetRecordReader.java:218)
>         ... 3 more

* (TODO) Then intended to verify that ReadColumnar prints correct set of values.

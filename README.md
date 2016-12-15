# parse-parquet-thrift

This tool is similar to "parquet-tools meta": it reads parquet's FileMetaData, print CreatedBy, KeyValueMetaData and then print parquet file blocks with their offsets.

### Usage
java -classpath target\parquet-struct-jar-with-dependencies.jar com.github.vkovalchuk.ParseParquetFooter parquets\part-0

### Sample output



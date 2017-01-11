package com.github.vkovalchuk;

import java.util.HashMap;
import java.util.Map;

class ParquetThriftStructDictionary {
    Map<Integer, String> empty = new HashMap<>();

    private static Map<String, Map<Integer, String>> DICTIONARY = new HashMap<>();
    // see https://raw.githubusercontent.com/Parquet/parquet-format/master/src/thrift/parquet.thrift
    static {
        Map<Integer, String> dictFileMetaData = new HashMap<>();
        dictFileMetaData.put(1, "version: i32");
        dictFileMetaData.put(2, "schema: List SchemaElement");
        dictFileMetaData.put(3, "num_rows: i64");
        dictFileMetaData.put(4, "row_groups: List RowGroup");
        dictFileMetaData.put(5, "key_value_metadata: List KeyValue");
        dictFileMetaData.put(6, "created_by: string");
        DICTIONARY.put("FileMetaData", dictFileMetaData);

        Map<Integer, String> dictSchemaElement = new HashMap<>();
        dictSchemaElement.put(1, "type: Type");
        dictSchemaElement.put(2, "type_length: i32");
        dictSchemaElement.put(3, "repetition_type: FieldRepetitionType");
        dictSchemaElement.put(4, "name: string");
        dictSchemaElement.put(5, "num_children: i32");
        dictSchemaElement.put(6, "converted_type: ConvertedType");
        dictSchemaElement.put(7, "scale: i32");
        dictSchemaElement.put(8, "precision: i32");
        dictSchemaElement.put(9, "field_id: i32");
        DICTIONARY.put("SchemaElement", dictSchemaElement);

        Map<Integer, String> dictRowGroup = new HashMap<>();
        dictRowGroup.put(1, "columns: List ColumnChunk");
        dictRowGroup.put(2, "total_byte_size: i64");
        dictRowGroup.put(3, "num_rows: i64");
        dictRowGroup.put(4, "sorting_columns: List SortingColumns");
        DICTIONARY.put("RowGroup", dictRowGroup);

        Map<Integer, String> dictStatistics = new HashMap<>();
        dictStatistics.put(1, "max: binary");
        dictStatistics.put(2, "min: binary");
        dictStatistics.put(3, "nulls_count: i64");
        dictStatistics.put(4, "distinct_count: i64");
        DICTIONARY.put("Statistics", dictStatistics);

        Map<Integer, String> dictColumnChunk = new HashMap<>();
        dictColumnChunk.put(1, "file_path: string");
        dictColumnChunk.put(2, "file_offset: i64");
        dictColumnChunk.put(3, "meta_data: ColumnMetaData");
        DICTIONARY.put("ColumnChunk", dictColumnChunk);

        Map<Integer, String> dictPageEncodingStats = new HashMap<>();
        dictPageEncodingStats.put(1, "page_type: PageType");
        dictPageEncodingStats.put(2, "encoding: Encoding");
        dictPageEncodingStats.put(3, "count: i32");
        DICTIONARY.put("PageEncodingStats", dictPageEncodingStats);

        Map<Integer, String> dictKeyValue = new HashMap<>();
        dictKeyValue.put(1, "key: string");
        dictKeyValue.put(2, "value: string");
        DICTIONARY.put("KeyValue", dictKeyValue);

        Map<Integer, String> dictColumnMetaData = new HashMap<>();
        dictColumnMetaData.put(1, "type: Type");
        dictColumnMetaData.put(2, "encodings: List Encoding");
        dictColumnMetaData.put(3, "path_in_schema: List string");
        dictColumnMetaData.put(4, "codec: CompressionCodec");
        dictColumnMetaData.put(5, "num_values: i64");
        dictColumnMetaData.put(6, "total_uncompressed_size: i64");
        dictColumnMetaData.put(7, "total_compressed_size: i64");
        dictColumnMetaData.put(8, "key_value_metadata: List KeyValue");
        dictColumnMetaData.put(9, "data_page_offset: i64");
        dictColumnMetaData.put(10, "index_page_offset: i64");
        dictColumnMetaData.put(11, "dictionary_page_offset: i64");
        dictColumnMetaData.put(12, "statistics: Statistics");
        dictColumnMetaData.put(13, "encoding_stats: List PageEncodingStats");
        DICTIONARY.put("ColumnMetaData", dictColumnMetaData);
    }

    public String getFieldName(String structType, int fieldId) {
        return DICTIONARY.getOrDefault(structType, empty).getOrDefault(fieldId, "#" + fieldId);
    }

    private static Map<String, Map<Integer, String>> ENUMTYPES = new HashMap<>();
    // see https://raw.githubusercontent.com/Parquet/parquet-format/master/src/thrift/parquet.thrift
    static {
        Map<Integer, String> enumFieldRepetitionType = new HashMap<>();
        enumFieldRepetitionType.put(0, "required");
        enumFieldRepetitionType.put(1, "optional");
        enumFieldRepetitionType.put(2, "repeated");
        ENUMTYPES.put("FieldRepetitionType", enumFieldRepetitionType);

        Map<Integer, String> enumType = new HashMap<>();
        enumType.put(0, "BOOLEAN");
        enumType.put(1, "INT32");
        enumType.put(2, "INT64");
        enumType.put(3, "INT96");
        enumType.put(4, "FLOAT");
        enumType.put(5, "DOUBLE");
        enumType.put(6, "BYTE_ARRAY");
        enumType.put(7, "FIXED_LEN_BYTE_ARRAY");
        ENUMTYPES.put("Type", enumType);

        Map<Integer, String> enumConvertedType = new HashMap<>();
        enumConvertedType.put(0, "UTF8");
        enumConvertedType.put(1, "MAP");
        enumConvertedType.put(2, "MAP_KEY_VALUE");
        enumConvertedType.put(3, "LIST");
        enumConvertedType.put(4, "ENUM");
        ENUMTYPES.put("ConvertedType", enumConvertedType);

        Map<Integer, String> enumCompressionCodec = new HashMap<>();
        enumCompressionCodec.put(0, "UNCOMPRESSED");
        enumCompressionCodec.put(1, "SNAPPY");
        enumCompressionCodec.put(2, "GZIP");
        enumCompressionCodec.put(3, "LZO");
        ENUMTYPES.put("CompressionCodec", enumCompressionCodec);

        Map<Integer, String> enumEncoding = new HashMap<>();
        enumEncoding.put(0, "PLAIN");
        enumEncoding.put(2, "PLAIN_DICTIONARY");
        enumEncoding.put(3, "RLE");
        enumEncoding.put(4, "BIT_PACKED");
        ENUMTYPES.put("Encoding", enumEncoding);
    }

    static {
        Map<Integer, String> dictPageHeader = new HashMap<>();
        dictPageHeader.put(1, "type: PageType");
        dictPageHeader.put(2, "uncompressed_page_size: i32");
        dictPageHeader.put(3, "compressed_page_size: i32");
        dictPageHeader.put(4, "crc: i32");
        dictPageHeader.put(5, "data_page_header: DataPageHeader");
        dictPageHeader.put(6, "index_page_header: IndexPageHeader");
        dictPageHeader.put(7, "dictionary_page_header: DictionaryPageHeader");
        dictPageHeader.put(8, "data_page_header_v2: DataPageHeaderV2");
        DICTIONARY.put("PageHeader", dictPageHeader);

        Map<Integer, String> dictDataPageHeader = new HashMap<>();
        dictDataPageHeader.put(1, "num_values: i32");
        dictDataPageHeader.put(2, "encoding: Encoding");
        dictDataPageHeader.put(3, "definition_level_encoding: Encoding");
        dictDataPageHeader.put(4, "repetition_level_encoding: Encoding");
        dictDataPageHeader.put(5, "statistics: Statistics");
        DICTIONARY.put("DataPageHeader", dictDataPageHeader);

        Map<Integer, String> dictDictionaryPageHeader = new HashMap<>();
        dictDictionaryPageHeader.put(1, "num_values: i32");
        dictDictionaryPageHeader.put(2, "encoding: Encoding");
        dictDictionaryPageHeader.put(3, "is_sorted: boolean");
        DICTIONARY.put("DictionaryPageHeader", dictDictionaryPageHeader);

        Map<Integer, String> dictDataPageHeaderV2 = new HashMap<>();
        dictDataPageHeaderV2.put(1, "num_values: i32");
        dictDataPageHeaderV2.put(2, "num_nulls: i32");
        dictDataPageHeaderV2.put(3, "num_rows: i32");
        dictDataPageHeaderV2.put(4, "encoding: Encoding");
        dictDataPageHeaderV2.put(5, "definition_levels_byte_length: i32");
        dictDataPageHeaderV2.put(6, "repetition_levels_byte_length: i32");
        dictDataPageHeaderV2.put(7, "is_compressed: boolean");
        dictDataPageHeaderV2.put(8, "statistics: Statistics");
        DICTIONARY.put("dictDataPageHeaderV2", dictDataPageHeaderV2);
    }
    static {
        Map<Integer, String> enumPageType = new HashMap<>();
        enumPageType.put(0, "DATA_PAGE");
        enumPageType.put(1, "INDEX_PAGE");
        enumPageType.put(2, "DICTIONARY_PAGE");
        enumPageType.put(3, "DATA_PAGE_V2");
        ENUMTYPES.put("PageType", enumPageType);
    }

    public String getEnumName(String enumType, int enumValue) {
        return ENUMTYPES.getOrDefault(enumType, empty).getOrDefault(enumValue, "???" + enumValue);
    }
}
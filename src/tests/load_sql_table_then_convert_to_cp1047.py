
# I need a pipeline that reade from sql table which is in cp1252 encoding with column name id and recordline
#, i need it loaded as a pyspark dataframe ,sort it by id, drop the first row , then convert the dataframe to cp1047 encoding and write it as cp1047 file, each recoreline is fix length, i can provide you the recordline length size
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
def process_sql_to_cp1047(spark: SparkSession, jdbc_url: str, user: str, password: str, table_name: str, recordline_length: int, output_path: str):
    """
    Process SQL table encoded in cp1252, sort by id, drop first row,
    convert to cp1047 encoding and write to fixed length file.

    :param spark: SparkSession
    :param jdbc_url: JDBC URL for the database
    :param user: Database user
    :param password: Database password
    :param table_name: Name of the SQL table to read
    :param recordline_length: Fixed length of each record line
    :param output_path: Output path for the cp1047 encoded file
    :return: None
    """
    # Load SQL table as DataFrame
    df = spark.read.format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", table_name) \
        .option("user", user) \
        .option("password", password) \
        .load()
    # Sort by id and drop the first row
    sorted_df = df.orderBy(col("id")).limit(df.count() - 1)
    # a function to split recordline to specification that is a list of  length for each record field, some fileld may have leading space to trim,and also need to pad, then concated  the trimmed and padded filed back as the fixed length line


    from typing import Iterable, List, Optional, Union

    def split_and_fix(
            recordline: str,
            spec: Iterable[int],
            trim_mask: Optional[Union[bool, Iterable[bool]]] = None,
            pad_char: str = " ",
    ) -> str:
        """
        Split `recordline` into fields according to `spec` (list of field lengths),
        optionally trim leading spaces for fields indicated by `trim_mask`, then
        pad (right-pad) or truncate each field to its fixed length and concatenate.

        :param recordline: Input raw record line (may be shorter/longer than total spec).
        :param spec: Iterable of positive integers specifying each field length.
        :param trim_mask: If None, no trimming. If a single bool, applies to all fields.
                          If iterable of bools, must match length of spec.
                          True means strip leading spaces from that field before padding.
        :param pad_char: Single character used to pad fields (default space).
        :return: Fixed-length concatenated record line (length == sum(spec)).
        """
        lengths: List[int] = [int(l) for l in spec]
        if any(l <= 0 for l in lengths):
            raise ValueError("All field lengths in spec must be positive integers.")
        if len(pad_char) != 1:
            raise ValueError("pad_char must be a single character.")

        total_len = sum(lengths)
        # Normalize trim_mask to a list of booleans matching spec length
        if trim_mask is None:
            trims = [False] * len(lengths)
        elif isinstance(trim_mask, bool):
            trims = [trim_mask] * len(lengths)
        else:
            trims = list(bool(t) for t in trim_mask)
            if len(trims) != len(lengths):
                raise ValueError("trim_mask length must match spec length.")

        out_fields: List[str] = []
        pos = 0
        for length, do_trim in zip(lengths, trims):
            raw = recordline[pos: pos + length] if pos < len(recordline) else ""
            pos += length
            if do_trim:
                value = raw.lstrip()
            else:
                value = raw
            # Right-pad with pad_char and ensure exact length (truncate if needed)
            fixed = (value + pad_char * length)[:length]
            out_fields.append(fixed)
        return "".join(out_fields)

    to_fixed_length_udf = spark.udf.register("to_fixed_length", to_fixed_length)
    fixed_length_df = sorted_df.withColumn("recordline", to_fixed_length_udf(col("recordline")))
    # Collect the recordlines and write to cp1047 file
    recordlines = fixed_length_df.select("recordline").rdd.map(lambda row: row.recordline).collect()
    with open(output_path, "wb") as f:
        for line in recordlines:
            f.write(line.encode("cp1047"))
            f.write(b"\n")  # Newline after each recordline
# Example usage:
if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("SQL to CP1047 Pipeline") \
        .getOrCreate()
    process_sql_to_cp1047(
        spark,
        jdbc_url="jdbc:your_database_url",
        user="your_username",
        password="your_password",
        table_name="your_table_name",
        recordline_length=100,  # Example fixed length
        output_path="output_cp1047.txt"
    )
    spark.stop()

    """
    Process SQL table encoded in cp1252, sort by id, drop first row,
    convert to cp1047 encoding and write to fixed length file.  
    :param spark: SparkSession
    :param jdbc_url: JDBC URL for the database
    :param user: Database user
    :param password: Database password
    :param table_name: Name of the SQL table to read
    :param recordline_length: Fixed length of each record line
    :param output_path: Output path for the cp1047 encoded file
    :return: None
    """
    # Load SQL table as DataFrame
    df = spark.read.format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", table_name) \
        .option("user", user) \
        .option("password", password) \
        .load()
    # Sort by id and drop the first row
    sorted_df = df.orderBy(col("id")).limit(df.count() - 1)
    # Convert recordline to fixed length, the record line is a multi-field fixed length specified by  has a list of  field name and length as specification, some field may need to trim the leading space,please update the function accordingly


    def to_fixed_length(recordline):
        return recordline.ljust(recordline_length)[:recordline_length]



    to_fixed_length_udf = spark.udf.register("to_fixed_length", to_fixed_length)
    fixed_length_df = sorted_df.withColumn("recordline", to_fixed_length_udf(col("recordline")))
    # Collect the recordlines and write to cp1047 file
    recordlines = fixed_length_df.select("recordline").rdd.map(lambda row: row.recordline).collect()
    with open(output_path, "wb") as f:
        for line in recordlines:
            f.write(line.encode("cp1047"))
            f.write(b"\n")  # Newline after each recordline
# Example usage:
if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("SQL to CP1047 Pipeline") \
        .getOrCreate()
    process_sql_to_cp1047(
        spark,
        jdbc_url="jdbc:your_database_url",
        user="your_username",
        password="your_password",
        table_name="your_table_name",
        recordline_length=100,  # Example fixed length
        output_path="output_cp1047.txt"
    )
    spark.stop()


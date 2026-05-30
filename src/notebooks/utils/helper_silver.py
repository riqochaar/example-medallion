from pyspark.sql import functions as F

def apply_schema_types(df, config):

    """
    Cast columns in a DataFrame to the types specified in a config dictionary.

    Args:
        df (DataFrame): Input DataFrame with columns to be cast.
        config (dict): Configuration dictionary containing column names and their target types.

    Returns:
        df (DataFrame): DataFrame with columns cast to the specified types.
    """

    schema = config["target"]["table_schema"]

    for col_name, props in schema.items():
        col_type = props["type"]

        if col_type == "timestamp":
            df = df.withColumn(col_name, F.to_timestamp(F.col(col_name)))

        elif col_type == "integer":
            df = df.withColumn(col_name, F.col(col_name).cast("int"))

        elif col_type.startswith("decimal"):
            df = df.withColumn(col_name, F.col(col_name).cast(col_type))

        else:
            # fallback for anything else
            df = df.withColumn(col_name, F.col(col_name).cast(col_type))

    return df
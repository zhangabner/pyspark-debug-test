import pyspark.sql.functions as F
from functools import reduce

def remove_non_word_characters(col):
    return F.regexp_replace(col, "[^\\w\\s]+", "")

def rename_chars(column_name):
    chars = ((' ', '_&'), ('.', '_$'))
    new_cols = reduce(lambda a, kv: a.replace(*kv), chars, column_name)
    return new_cols

def dots_to_underscores(s):
    return s.replace(".", "_", 1)
    
def divide_by_three(col):
    return col / 3

def divide_by_zero(col):
    return col / 0

def sort_columns(df, sort_order):
    sorted_col_names = None
    if sort_order == "asc":
        sorted_col_names = sorted(df.columns)
    elif sort_order == "desc":
        sorted_col_names = sorted(df.columns, reverse=True)
    else:
        raise ValueError("['asc', 'desc'] are the only valid sort orders and you entered a sort order of '{sort_order}'".format(
            sort_order=sort_order
        ))
    return df.select(*sorted_col_names)

def change_column_names(df):
    changed_col_names = df.schema.names
    for cols in changed_col_names:
        df = df.withColumnRenamed(cols, rename_chars(cols))
    return df

def modify_column_names(df, fun):
    for col_name in df.columns:
        df = df.withColumnRenamed(col_name, fun(col_name))
    return df
    


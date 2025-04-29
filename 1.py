from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def get_product_category_pairs(products_df, categories_df, product_category_df):
    # Соединяем продукты с таблицей связей
    prod_cat_joined = products_df.join(product_category_df, on="product_id", how="left")
    
    # Соединяем с категориями
    full_join = prod_cat_joined.join(categories_df, on="category_id", how="left")
    
    # Выбираем нужные колонки
    result_df = full_join.select(
        col("product_name"),
        col("category_name")
    )
    
    return result_df

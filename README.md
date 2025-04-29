# Для решения задачи в PySpark, нужно:

## Иметь три датафрейма:
```
products(product_id, product_name)

categories(category_id, category_name)

product_category(product_id, category_id) – связи между продуктами и категориями.
```
## Выполнить left join products с product_category, а затем – с categories, чтобы сохранить продукты без категорий.

## Вернуть один датафрейм с колонками:
```
product_name

category_name (будет null, если категория отсутствует)
```

## Пример данных
```
products = spark.createDataFrame([
    (1, "Apple"), (2, "Milk"), (3, "Soap")
], ["product_id", "product_name"])

categories = spark.createDataFrame([
    (10, "Fruits"), (20, "Dairy")
], ["category_id", "category_name"])

product_category = spark.createDataFrame([
    (1, 10), (2, 20)
], ["product_id", "category_id"])
```

## Пример вывода
```
df = get_product_category_pairs(products, categories, product_category)
df.show()
```

## Результат:
```
|product_name|category_name|
|Apple|Fruits|
|Milk|Dairy|
|Soap|null|
```

from pyspark.sql import SparkSession

# Crear una sesión de Spark
spark = SparkSession.builder \
    .appName("Simple Spark App") \
    .getOrCreate()

# Leer archivo CSV
df = spark.read.options(header=True, inferSchema=True).csv("/opt/app/data.csv")

# Mostrar el esquema del DataFrame
print("Esquema del DataFrame:")
df.printSchema()

# Contar el número de filas en el DataFrame
row_count = df.count()
print(f"Número de filas: {row_count}")

# Mostrar las primeras 5 filas
print("Primeras 5 filas:")
df.show(5)

# Agrupar por una columna (por ejemplo, "categoría") y contar el número de filas en cada grupo
if "categories" in df.columns:
    df.groupBy("categories").count().show()

# Finalizar la sesión de Spark
spark.stop()

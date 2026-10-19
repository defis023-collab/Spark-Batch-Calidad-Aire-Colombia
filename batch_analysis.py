from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg
import matplotlib.pyplot as plt

# 1. Inicializar la sesión de Spark
spark = SparkSession.builder \
    .appName("CalidadAireBatch") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("\n=== INICIANDO PROCESAMIENTO EN BATCH ===")

# 2. Crear conjunto de datos histórico (Simulación de Datos Abiertos Colombia)
data = [
    ("Sensor_Bogota_1", "Bogota", 45.2, 350.0, "2026-10-18 08:00:00"),
    ("Sensor_Medellin_1", "Medellin", 55.8, 420.0, "2026-10-18 08:05:00"),
    ("Sensor_Cali_1", "Cali", 30.1, 290.0, "2026-10-18 08:10:00"),
    ("Sensor_Bogota_1", "Bogota", 48.9, 365.0, "2026-10-18 09:00:00"),
    ("Sensor_Medellin_1", "Medellin", 62.4, 450.0, "2026-10-18 09:05:00"),
    ("Sensor_Cali_1", "Cali", 28.5, 280.0, "2026-10-18 09:10:00"),
    ("Sensor_Bogota_1", "Bogota", None, 340.0, "2026-10-18 10:00:00"), 
    ("Sensor_Barranquilla_1", "Barranquilla", 22.3, 210.0, "2026-10-18 10:05:00")
]

columns = ["sensor_id", "ciudad", "PM25", "CO2", "timestamp"]

# Cargar los datos en un DataFrame de Spark
df_historico = spark.createDataFrame(data, schema=columns)

# 3. Limpieza y Transformación de Datos
df_limpio = df_historico.na.drop(subset=["PM25"])

# 4. Análisis Exploratorio (EDA): Calcular el promedio de contaminación por ciudad
df_resultado = df_limpio.groupBy("ciudad") \
    .agg(avg("PM25").alias("Promedio_PM25"), avg("CO2").alias("Promedio_CO2")) \
    .orderBy(col("Promedio_PM25").desc())

print("[INFO] Resultados del Análisis (Promedios de contaminación por ciudad):")
df_resultado.show()

# 5. Visualización de Resultados
pandas_df = df_resultado.toPandas()
plt.figure(figsize=(8, 4))
plt.bar(pandas_df['ciudad'], pandas_df['Promedio_PM25'], color=['red', 'orange', 'yellow', 'green'])
plt.title('Calidad del Aire en Colombia - Promedio Histórico de PM2.5')
plt.xlabel('Ciudad')
plt.ylabel('Material Particulado PM2.5 (µg/m³)')
plt.grid(axis='y', linestyle='--', alpha=0.7)

output_image = "reporte_calidad_aire.png"
plt.savefig(output_image)
print(f"\n[ÉXITO] Gráfica estadística generada y guardada en: {output_image}")

spark.stop()
print("=== PROCESAMIENTO EN BATCH FINALIZADO ===\n")

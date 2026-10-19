# Procesamiento en Batch de Calidad del Aire en Colombia con Apache Spark

Este repositorio contiene la solución desarrollada para la Tarea 3, enfocada en el análisis por lotes (Batch) de datos meteorológicos y ambientales utilizando Spark DataFrames en Python.

## Descripción de la Solución
La aplicación carga un conjunto de datos históricos de sensores automatizados en Colombia que miden variables de salud pública: Material Particulado (PM2.5) y Monóxido de Carbono (CO2). El script implementa un pipeline de datos distribuido que realiza:
1. Inicialización de la arquitectura con `SparkSession`.
2. Ingeniería de datos y limpieza mediante la eliminación de registros corruptos o nulos con `.na.drop()`.
3. Análisis Exploratorio de Datos (EDA) agrupando por ciudades (`groupBy`) para calcular los promedios de contaminación de forma paralela.
4. Generación automática de gráficos estadísticos con `Matplotlib` y `Pandas`.

## Instrucciones de Ejecución
Para correr este script en un entorno con Apache Spark configurado, ejecute el siguiente comando en su terminal:

```bash
spark-submit batch_analysis.py
```

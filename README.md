
# Análisis de mortalidad por accidentes de tránsito en Bogotá

## Descripción

Proyecto académico de la Tarea 3 de Big Data de la UNAD.

Se utiliza Apache Spark con Python (PySpark) para procesar información pública sobre mortalidad por accidentes de tránsito en Bogotá.

## Fuente de datos

- Portal: Datos Abiertos Bogotá
- Entidad: Secretaría Distrital de Salud
- Conjunto: Mortalidad por accidentes de tránsito
- Archivo: osb_evento_transporte.csv
- Registros analizados: 13.138

## Tecnologías

- Ubuntu 22.04
- Python 3.10
- Apache Spark 3.5.8
- PySpark

## Archivos del proyecto

- analisis_accidentes.py: análisis por año, localidad, sexo y tipo de accidente.
- limpieza_accidentes.py: validación de valores nulos, vacíos y duplicados.
- transformacion_accidentes.py: limpieza de espacios en campos de texto y exportación de datos.

## Resultados principales

- Se procesaron 13.138 registros.
- Se identificaron 42 filas duplicadas exactas, conservadas por falta de evidencia para eliminarlas.
- No se encontraron valores nulos ni vacíos en las cuatro columnas principales evaluadas.
- Se generaron archivos CSV con los resultados estadísticos.

## Ejecución

Los programas utilizan la ruta de entrada:

/home/vboxuser/osb_evento_transporte.csv

Antes de ejecutarlos en otro equipo, se debe ajustar esta ruta según la ubicación del archivo.

Ejecutar:

```bash
spark-submit analisis_accidentes.py
spark-submit limpieza_accidentes.py
spark-submit transformacion_accidentes.py
```

Los resultados se guardan en:

/home/vboxuser/resultados_accidentes/

## Consideraciones

El año 2026 puede contener información parcial.

Las categorías sin información se conservan y los registros repetidos no se eliminan automáticamente para evitar alterar los datos originales.

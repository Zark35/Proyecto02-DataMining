# Proyecto 02 Data Mining 
# Estudiante: Andrés Bohórquez | Código: 00320727

# **Descripción y diagrama de arquitectura (bronze/silver/gold) y orquestación en Mage**
Este proyecto implementa un data pipeline moderno para el procesamiento de datos de los servicios Yellow Taxi y Green Taxi de la ciudad de Nueva York, desde los archivos públicos en formato Parquet (2015–2025), integrando:

* Mage como orquestador de ingesta y transformación.
* Snowflake como Data Warehouse principal (capas Bronze, Silver y Gold).
* dbt para modelado, pruebas de calidad y documentación de datos.

* Mage como orquestador de ingesta y transformación.  
* Snowflake como Data Warehouse principal (capas Bronze, Silver y Gold).  
* dbt para modelado, pruebas de calidad y documentación de datos.

```text
┌────────────────────┐
│  Data Source (NYC) │
│  Parquet Files     │
│  (2015–2025)       │
└────────┬───────────┘
         │
         ▼
┌────────────────────────────────┐
│  Mage Data Loader (Python)     │
│  - Ingesta mensual por año     │
│  - Control con COVERAGE_MATRIX │
│  - Capa BRONZE (raw)           │
└────────┬───────────────────────┘
         │
         ▼
┌────────────────────────────┐
│  dbt Models (SILVER)       │
│  - Limpieza / Validación   │
│  - Unión de Green & Yellow │
│  - Normalización de tipos  │
└────────┬───────────────────┘
         │
         ▼
┌────────────────────────────┐
│  dbt Models (GOLD)         │
│  - Hechos: fct_trips       │
│  - Dimensiones: date, zone │
│  - Ratecode, payment, etc. │
└────────┬───────────────────┘
         │
         ▼
┌─────────────────────────────────┐
│  Snowflake Analytics Layer      │
│  - Clustering por fecha y zona  │
│  - Query tuning y pruning       │
│  - Calidad de datos documentada │
└─────────────────────────────────┘
```
Orquestación en Mage:
* Flujos programados por año y servicio (yellow, green).
* Ejecución secuencial con control de errores y idempotencia (evita duplicados).
* Cada carga mensual actualiza la tabla BRONZE.COVERAGE_MATRIX.

<br>

# **Cobertura de Datos**
Los datos fueron ingeridos desde enero de 2015 hasta junio de 2025, cubriendo ambos servicios:

```text
Servicio	| Años      | Cargados   |	Total Archivos | Estado
-----------------------------------------------------------------
Yellow Taxi	| 2015–2025 |	126	     | ✅              | Completo
Green Taxi	| 2015–2025 |	126   	 | ✅              | Completo
```

Para comprobar este apartado se ejecutan algunos querys en Snowflake, como:

USE DATABASE NYC_TAXI_DM;

USE SCHEMA BRONZE;

SELECT LOAD_SERVICE_TYPE, LOAD_YEAR, LOAD_MONTH, LOAD_STATUS

FROM COVERAGE_MATRIX

ORDER BY LOAD_SERVICE_TYPE, LOAD_YEAR, LOAD_MONTH;

![alt text](<Screen Cobertura Datos 2.png>)

Otro query de Snowflake para comprobar todos los datos fue:

USE DATABASE NYC_TAXI_DM;

USE SCHEMA BRONZE;

SELECT LOAD_SERVICE_TYPE, LOAD_YEAR, LOAD_MONTH, COUNT(*) AS total_filas

FROM YELLOW_TRIPS_BRONZE

GROUP BY LOAD_SERVICE_TYPE, LOAD_YEAR, LOAD_MONTH

ORDER BY LOAD_YEAR, LOAD_MONTH;

![alt text](<Screen Cobertura Datos 1.png>)

<br>

# **Estrategia de pipeline de backfill mensual e idempotencia.**
Para el backfill mi proceso fue tener varios bloques de código en Mage.

Por un lado, tengo un primer pipeline en el qe descargué todos los datos para enero de 2019 y con lo que empecé a trabajar para ir desarrollando los archivos dbt y llegar a las pregunta. También con un bloque para verificar la conexión con snowflake y comprobar que se guarden en secrets los datos. Y un último bloque para el apartado de cargar las zonas. 

Por otro lado, tengo un segundo pipeline donde tengo cuatro bloques. Dos primeros enfocados en recopilar todos los datos desde 2015 hasta 2024, uno para yello y otro green. Al igual que en el anterior caso, dos bloque para yellow y green pero en este caso para el año 2025.

Asimismo, en el caso de sobrar algunos bloques de data loaders, son referentes a pruebas que realicé previamente y me olvidé de eliminar. Los más fundamentales están integrados en los dos pipelines de proyecto 2.

La idempotencia la garantizo al momento de ejecutar cada bloque y al momento de presentarse algún fallo, cómo se puede observar en la screenshot anterior de Snowflake, solo debo cambiar en el for de los meses el valor correspondiente al mes que dio problemas. En el caso de yellow en la fecha 2023-08 dio problemas debido a que se acabó el tiempo de conexión. Se solucionó volviendo a correr el código en el rango de 8 y funcionó. Asimismo, no existe problema de que se dupliquen los datos al volver a correr porque el código no sobreescribe.

<br>

# **Gestión de secretos y cuenta de servicio / rol**

Para este apartado, lo primero fue guardar en secrets los valores de los secretos correspondientes a la cuenta con permisos mínimos de Snowflake, no la cuenta administrador. 

Y la cuenta la cree inicialmente con un query en Snowflake similar al siguiente: 

```python
-- Crear rol de ingesta
CREATE OR REPLACE ROLE role_dm_ingest;

-- Crear usuario de servicio (ejemplo: svc_dm_ingest)
CREATE OR REPLACE USER svc_dm_ingest
  PASSWORD = 'ContraseñaSegura123'
  DEFAULT_ROLE = role_dm_ingest
  DEFAULT_WAREHOUSE = wh_dm
  DEFAULT_NAMESPACE = nyc_taxi_dm.bronze
  MUST_CHANGE_PASSWORD = FALSE;

-- Permisos mínimos al rol
GRANT USAGE ON WAREHOUSE wh_dm TO ROLE role_dm_ingest;

GRANT USAGE ON DATABASE nyc_taxi_dm TO ROLE role_dm_ingest;
GRANT USAGE ON SCHEMA nyc_taxi_dm.bronze TO ROLE role_dm_ingest;
GRANT CREATE TABLE ON SCHEMA nyc_taxi_dm.bronze TO ROLE role_dm_ingest;
GRANT INSERT, SELECT, UPDATE, DELETE ON ALL TABLES IN SCHEMA nyc_taxi_dm.bronze TO ROLE role_dm_ingest;
GRANT INSERT, SELECT, UPDATE, DELETE ON FUTURE TABLES IN SCHEMA nyc_taxi_dm.bronze TO ROLE role_dm_ingest;

-- Asignar el rol al usuario
GRANT ROLE role_dm_ingest TO USER svc_dm_ingest;
```

<br>

# **Diseño de silver (reglas de limpieza/estandarización) y gold (hechos/dimensiones)**

La capa Silver tiene como objetivo transformar los datos brutos del esquema Bronze en un formato coherente, estandarizado y sin valores erróneos.
Esta etapa busca asegurar la consistencia semántica y estructural de todos los datasets antes de su análisis en la capa Gold.

Reglas aplicadas:

✅ Estandarización de nombres de columnas:
Todos los nombres fueron convertidos a mayúsculas con guion bajo, eliminando caracteres especiales (por ejemplo, tpep_pickup_datetime → TPEP_PICKUP_DATETIME).

✅ Tipificación de campos:
Conversión explícita de los tipos de datos a sus formatos correctos:

Fechas (pickup_datetime, dropoff_datetime) → TIMESTAMP

Distancias → FLOAT

Tarifas y montos → NUMERIC(10,2)

Identificadores → INTEGER

✅ Tratamiento de valores nulos y duplicados:

Eliminación de registros duplicados basados en (PICKUP_DATETIME, DROPOFF_DATETIME, VENDORID)

Reemplazo o eliminación de filas con trip_distance <= 0 o fare_amount < 0.

✅ Corrección de rangos inválidos:

Se filtraron viajes con duraciones fuera de 1 a 180 minutos (para evitar datos corruptos o de prueba).

Se eliminaron filas con velocidades calculadas mayores a 80 mph.

✅ Normalización de identificadores de zonas:
Se garantizó la correspondencia entre pickup_location_id y las zonas definidas en dim_zone, usando JOIN con la tabla de zonas de NYC.

✅ Unificación temporal:
Se añadieron las columnas de control:
LOAD_YEAR, LOAD_MONTH, y LOAD_SERVICE_TYPE
para rastrear el origen y periodo de carga de cada dataset.

Resultado:
Los datos en Silver están limpios, consistentes, y listos para integrarse en un modelo analítico estable.

<br>

La capa Gold implementa un modelo tipo estrella (Star Schema), orientado al análisis de negocio, agregando los datos en una estructura de hechos y dimensiones.
El objetivo es optimizar la consulta de métricas clave (hechos) asociadas a descripciones contextuales (dimensiones).

Diseño del modelo:

📦 Tabla de hechos:

FCT_TRIPS
Contiene las medidas cuantitativas del sistema de taxis.

Principales métricas:

trip_distance

trip_duration_min (duración en minutos calculada como diferencia entre pickup y dropoff)

fare_amount, tip_amount, total_amount

congestion_surcharge, extra, tolls_amount

Columnas de control (load_year, load_month, service_type)

Claves foráneas:

pickup_zone_sk, dropoff_zone_sk → DIM_ZONE

pickup_date_sk, dropoff_date_sk → DIM_DATE

pickup_time_sk, dropoff_time_sk → DIM_TIME

<br>

# **Clustering**

El proceso de clustering se aplicó sobre la tabla de hechos FCT_TRIPS con el fin de mejorar el rendimiento de las consultas analíticas realizadas en la capa GOLD, especialmente aquellas que filtran por fecha, zona y tipo de servicio.

Llaves elegidas:

ALTER TABLE SILVER_GOLD.FCT_TRIPS

CLUSTER BY (pickup_date_sk, pickup_zone_sk, service_type);

<br>

pickup_date_sk → Permite optimizar las consultas temporales y cálculos de tendencias (por año/mes).

pickup_zone_sk → Mejora la segmentación espacial, muy usada en análisis por borough o zonas de alta demanda.

service_type → Distingue entre yellow y green taxis, frecuentemente filtrados en las consultas de ingresos, duración o velocidad.

Métricas observadas (antes y después del clustering):

Antes del clustering, las consultas que agrupaban o filtraban por zona y fecha requerían scans completos de la tabla, afectando el tiempo de respuesta. (Demoraba 1.6 segundos)

Después de aplicar CLUSTER BY, Snowflake reorganizó físicamente los micro-particiones de la tabla, reduciendo la necesidad de leer datos irrelevantes. (Se demora 1.3 segundos)

Al ejecutar la búsqueda, se observó una disminución en el tiempo de ejecución.

**Conclusiones:**

El clustering en pickup_date_sk, pickup_zone_sk y service_type mejoró la eficiencia de lectura de la tabla de hechos, especialmente en las consultas más comunes del modelo analítico.

Se logró una reducción significativa en el tiempo de consulta (menos particiones escaneadas y menor uso de recursos del warehouse).

La estructura de micro-particiones ahora se alinea con los ejes principales del análisis (temporal, espacial y por tipo de servicio), manteniendo un equilibrio entre granularidad y desempeño.

En general, el clustering contribuye a que la capa GOLD sea más escalable y rápida, sin alterar la lógica del modelo de datos.

<br>

# **Pruebas**
Durante la construcción del modelo Silver–Gold, se ejecutaron pruebas automáticas mediante dbt test para asegurar la calidad, integridad y consistencia de los datos transformados.
Estas pruebas verifican que las tablas cumplan con las reglas básicas de limpieza y relaciones, antes de ser utilizadas en los análisis de la capa Gold.

**Pruebas aplicadas**

Pruebas de unicidad (unique)
Validan que las claves primarias o surrogate keys sean únicas dentro de sus dimensiones.

Ejemplo: zone_sk en dim_zone o date_sk en dim_date no deben repetirse.

dbt test --select dim_zone --store-failures


Pruebas de no nulos (not_null)
Garantizan que los campos esenciales no contengan valores vacíos o perdidos.

Ejemplo: en fct_trips, los campos pickup_zone_sk y pickup_date_sk deben tener siempre valores válidos.

tests:
  - not_null:
      column_name: pickup_zone_sk
  - not_null:
      column_name: pickup_date_sk


Pruebas de integridad referencial (relationships)
Aseguran que las claves foráneas de la tabla de hechos existan en las dimensiones correspondientes.

Ejemplo: todos los valores de pickup_zone_sk en fct_trips deben tener coincidencia en dim_zone.zone_sk.

tests:
  - relationships:
      from: fct_trips.pickup_zone_sk
      to: dim_zone.zone_sk


Pruebas de rangos y valores esperados (personalizadas)
Validan que las medidas cuantitativas estén dentro de los límites definidos.

Ejemplo: los viajes deben tener una duración y distancia lógicas.

SELECT *
FROM fct_trips
WHERE trip_duration_min NOT BETWEEN 1 AND 180
   OR trip_distance <= 0
   OR total_amount < 0;

<br>

# **Trobleshouting**

Debido al tiempo, la pregunta 5 se basa únicamente con la capa gold cuando tenía únicamente los datos de enero de 2019.

Debido a que al momento de crear el archivo profiles.yml debo definir un schema, los schemas con los que trabajo a partir de SILVER y GOLD en Snowflake, debo utilizar con un prefijo "SILVER_", por ejemplo: USE SCHEMA SILVER_GOLD; al momento de hacer el clustering.


# Checklist de implementación

- [x] Cargados todos los meses 2015–2025 (Parquet) de Yellow y Green; matriz de cobertura en README. NYC.gov

- [x] Mage orquesta backfill mensual con idempotencia y metadatos por lote.

- [x] Bronze (raw) refleja fielmente el origen; Silver unifica/escaliza; Gold en estrella con fct_trips y dimensiones clave

- [x] Clustering aplicado a fct_trips con evidencia antes/después (Query Profile, pruning). Snowflake Docs

- [x] Secrets y cuenta de servicio con permisos mínimos (evidencias sin exponer valores).

- [x] Tests dbt (not_null, unique, accepted_values, relationships) pasan; docs y lineage generados.
 
- [x] Notebook con respuestas a las 5 preguntas de negocio desde gold.
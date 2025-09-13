USE PRUEBA_LIDER_BI;
-- creo la tabla
CREATE TABLE telemetry_events
(
    trip_id                        INT           NOT NULL,
    driver_id                      INT           NOT NULL,
    vehicle_id                     INT           NOT NULL,
    [timestamp]                    DATETIME2(0)  NOT NULL, 
    latitude                       FLOAT         NULL,
    longitude                      FLOAT         NULL,
    speed                          FLOAT         NULL,
    acceleration                   FLOAT         NULL,
    steering_angle                 INT           NULL,
    heading                        FLOAT         NULL,
    trip_duration                  FLOAT         NULL,
    trip_distance                  FLOAT         NULL,
    fuel_consumption               FLOAT         NULL,
    rpm                            FLOAT         NULL,
    brake_usage                    INT           NULL,
    lane_deviation                 FLOAT         NULL,
    weather_conditions             NVARCHAR(30)  NULL,
    road_type                      NVARCHAR(30)  NULL,
    traffic_condition              NVARCHAR(30)  NULL,
    stop_events                    INT           NULL,
    geofencing_violation           BIT           NULL,
    anomalous_event                BIT           NULL,
    route_anomaly                  BIT           NULL,
    route_deviation_score          FLOAT         NULL,
    acceleration_variation         FLOAT         NULL,
    behavioral_consistency_index   FLOAT         NULL
);
--- Cargos los datos a la tabla
BULK INSERT dbo.telemetry_events
FROM 'C:\Users\User\Downloads\driver_behavior_route_anomaly_dataset_with_derived_features.csv\telemetry_events.csv'
WITH (
  FIRSTROW = 2,
  FIELDTERMINATOR = ',',
  ROWTERMINATOR  = '0x0a',
  TABLOCK,
  CODEPAGE = '65001', BATCHSIZE       = 20000,           -- carga en lotes de 20k
	ROWS_PER_BATCH  = 20000,
	MAXERRORS       = 1000,            -- tolera filas defectuosas
	KEEPNULLS 
);
--Se crea la funcion para calcular los indice /100 km
CREATE OR ALTER FUNCTION dbo.fnIndiceEventos0
(
    @numerador   DECIMAL(38,6),
    @denominador DECIMAL(38,6)
)
RETURNS DECIMAL(38,6)
AS
BEGIN
    RETURN CASE
        WHEN @denominador IS NULL OR @denominador = 0 THEN 0
        ELSE (@numerador * 100) / @denominador
    END;
END;
GO
-- Se crea la vista para poder traer los indices en una tabla 

CREATE VIEW VIEW_INDICES AS

	SELECT 

	  DRIVER_ID,
	  VEHICLE_ID,
	  COUNT(*)                         AS TOTAL_REGISTROS,
	  MIN([timestamp])                 AS PRIMER_TIMESTAMP,
	  MAX([timestamp])                 AS ULTIMO_TIMESTAMP,
	  SUM(speed)                       AS SUMA_VELOCIDAD,
	  SUM(stop_events)                 AS SUMAVENTOS,           
	  SUM(trip_distance)               AS SUMATOTALDISTANCIA,
 
	  -- indice Stop events 

	  dbo.fnIndiceEventos0(

		  SUM(CAST(stop_events   AS DECIMAL(18,4))),--Convierte los valores en decimales , 18 ent, 4 decimales y los ma

		  SUM(CAST(trip_distance AS DECIMAL(18,4)))

	  )                                AS INDICEEVENTOS,
 
	  -- indice geo

	   dbo.fnIndiceEventos0(

		  SUM(CAST(geofencing_violation   AS DECIMAL(18,4))),

		  SUM(CAST(trip_distance AS DECIMAL(18,4)))

	  ) AS INDEXGEOVIOLATION,


	  -- indice Anomalia

	   dbo.fnIndiceEventos0(

		  SUM(CAST(anomalous_event   AS DECIMAL(18,4))),

		  SUM(CAST(trip_distance AS DECIMAL(18,4)))

	  )  as INDEXNOMALIAEVENTS,

	  -- Indice route anomaly

	   dbo.fnIndiceEventos0(

		  SUM(CAST(route_anomaly   AS DECIMAL(18,4))),

		  SUM(CAST(trip_distance AS DECIMAL(18,4)))

	  ) as INDEXROUTEANOMALY,
 
	 
	  dbo.fnIndiceEventos0(

		  SUM(CAST(fuel_consumption   AS DECIMAL(18,4))),

		  SUM(CAST(trip_distance AS DECIMAL(18,4)))

	  ) as INDEXFUELCONSUMPTION
 
 	FROM dbo.Telemetry_events AS r -- Origen de los datos
--Se agrupa en similares por conductor y vehiculo
	GROUP BY driver_id, vehicle_id

SELECT * FROM VIEW_INDICES
select * from telemetry_events
select * from vehicle_fleet
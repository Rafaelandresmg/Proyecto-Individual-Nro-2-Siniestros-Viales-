-- Crear la tabla historial_kpi_m
CREATE TABLE historial_kpi_m (
    reduccion_porcentual DECIMAL(10, 2),
    reduccion_porcentualAA DECIMAL(10, 2),
    nro_siniestros_actual INT, -- Nueva columna para el número de siniestros del año actual
    nro_siniestros_anterior INT, -- Nueva columna para el número de siniestros del año anterior
    year_actual INT,
    year_anterior INT
);

-- Insertar datos en la tabla historial_kpi_m
INSERT INTO historial_kpi_m (reduccion_porcentual, reduccion_porcentualAA, nro_siniestros_actual, nro_siniestros_anterior, year_actual, year_anterior)
SELECT
    COALESCE(
        CASE
            WHEN SUM(CASE WHEN year = s.year_anterior AND gravedad = 'fatal' AND vehiculo_victima = 'moto' THEN 1 ELSE 0 END) = 0 THEN NULL
            ELSE (SUM(CASE WHEN year = s.year_actual AND gravedad = 'fatal' AND vehiculo_victima = 'moto' THEN 1 ELSE 0 END) -
                  SUM(CASE WHEN year = s.year_anterior AND gravedad = 'fatal' AND vehiculo_victima = 'moto' THEN 1 ELSE 0 END)) /
                 NULLIF(SUM(CASE WHEN year = s.year_anterior AND gravedad = 'fatal' AND vehiculo_victima = 'moto' THEN 1 ELSE 0 END), 0) * 100
        END,
        0
    ) AS reduccion_porcentual,
    COALESCE(
        LAG(
            CASE
                WHEN SUM(CASE WHEN year = s.year_anterior AND gravedad = 'fatal' AND vehiculo_victima = 'moto' THEN 1 ELSE 0 END) = 0 THEN NULL
                ELSE (SUM(CASE WHEN year = s.year_actual AND gravedad = 'fatal' AND vehiculo_victima = 'moto' THEN 1 ELSE 0 END) -
                      SUM(CASE WHEN year = s.year_anterior AND gravedad = 'fatal' AND vehiculo_victima = 'moto' THEN 1 ELSE 0 END)) /
                     NULLIF(SUM(CASE WHEN year = s.year_anterior AND gravedad = 'fatal' AND vehiculo_victima = 'moto' THEN 1 ELSE 0 END), 0) * 100
            END
        ) OVER (ORDER BY s.year_actual),
        0
    ) AS reduccion_porcentualAA,
    SUM(CASE WHEN year = s.year_actual AND gravedad = 'fatal' AND vehiculo_victima = 'moto' THEN 1 ELSE 0 END) AS nro_siniestros_actual,
    SUM(CASE WHEN year = s.year_anterior AND gravedad = 'fatal' AND vehiculo_victima = 'moto' THEN 1 ELSE 0 END) AS nro_siniestros_anterior,
    s.year_actual,
    s.year_anterior
FROM (
    SELECT distinct
        year AS year_actual,
        year - 1 AS year_anterior
    FROM db_pi_da.siniestros
    WHERE comuna BETWEEN 1 AND 15
    GROUP BY gravedad, vehiculo_victima, year
) AS s,
db_pi_da.siniestros
WHERE db_pi_da.siniestros.comuna BETWEEN 1 AND 15
GROUP BY s.year_actual, s.year_anterior
ORDER BY s.year_actual DESC;  -- Ordenar por year_actual

-- Seleccionar los datos de la tabla historial_kpi_m
SELECT * FROM historial_kpi_m;

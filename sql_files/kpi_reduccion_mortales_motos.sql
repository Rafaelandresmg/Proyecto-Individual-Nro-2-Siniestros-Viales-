-- Crear la tabla historial_kpi con mes
CREATE TABLE historial_kpi (
    id INT AUTO_INCREMENT PRIMARY KEY,
    reduccion_porcentual DECIMAL(10, 2),
    accidentes_anterior INT,
    accidentes_actual INT,
    year_actual INT,
    month_actual INT,
    year_anterior INT,
    reduccion_porcentual_entre_anios DECIMAL(10, 2)
);

-- Insertar datos en la tabla historial_kpi
INSERT INTO historial_kpi (reduccion_porcentual, accidentes_anterior, accidentes_actual, year_actual, month_actual, year_anterior, reduccion_porcentual_entre_anios)

SELECT
    -- Calcular la reducción porcentual para el mes actual
    (COUNT(CASE WHEN year = s.year_anterior AND month = s.month_actual AND gravedad = 'fatal' AND vehiculo_victima = 'moto' THEN id_siniestro END) -
     COUNT(CASE WHEN year = s.year_actual AND month = s.month_actual AND gravedad = 'fatal' AND vehiculo_victima = 'moto' THEN id_siniestro END)) /
    NULLIF(COUNT(CASE WHEN year = s.year_anterior AND month = s.month_actual AND gravedad = 'fatal' AND vehiculo_victima = 'moto' THEN id_siniestro END), 0) * 100 AS reduccion_porcentual,
    
    -- Contar el número de accidentes mortales de motociclistas en el año/mes anterior y actual
    (SELECT COUNT(*) FROM db_pi_da.siniestros WHERE year = s.year_anterior AND month = s.month_actual AND gravedad = 'fatal' AND vehiculo_victima = 'moto') AS accidentes_anterior,
    (SELECT COUNT(*) FROM db_pi_da.siniestros WHERE year = s.year_actual AND month = s.month_actual AND gravedad = 'fatal' AND vehiculo_victima = 'moto') AS accidentes_actual,
    
    -- Seleccionar el año actual, el mes actual y el año anterior
    s.year_actual,
    s.month_actual,
    s.year_anterior,
    
    -- Calcular la reducción porcentual entre los años actual y anterior agrupando por mes
    (SUM(CASE WHEN year = s.year_actual AND gravedad = 'fatal' AND vehiculo_victima = 'moto' THEN 1 ELSE 0 END) -
     SUM(CASE WHEN year = s.year_anterior AND gravedad = 'fatal' AND vehiculo_victima = 'moto' THEN 1 ELSE 0 END)) /
    NULLIF(SUM(CASE WHEN year = s.year_anterior AND gravedad = 'fatal' AND vehiculo_victima = 'moto' THEN 1 ELSE 0 END), 0) * 100 AS reduccion_porcentual_entre_anios
FROM (
    -- Subconsulta para obtener el año actual, el mes actual y el año anterior
    SELECT
        year AS year_actual,
        MONTH(fecha) AS month_actual,
        year - 1 AS year_anterior
    FROM db_pi_da.siniestros
    WHERE comuna BETWEEN 1 AND 15
    GROUP BY gravedad, vehiculo_victima, year, MONTH(fecha) -- Agregamos la columna fecha a GROUP BY
) AS s,
db_pi_da.siniestros
WHERE db_pi_da.siniestros.comuna BETWEEN 1 AND 15
GROUP BY s.year_actual, s.month_actual, s.year_anterior;

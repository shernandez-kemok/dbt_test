
{{ config(materialized='table') }}


WITH periodos AS (
    SELECT CURRENT_DATE::timestamp with time zone - '1 mon'::interval AS fecha_inicio1,
        CURRENT_DATE::timestamp with time zone - '2 mons'::interval AS fecha_inicio2
    ), sesiones_actuales AS (
    SELECT metabase_transacciones_de_uso.nombre_cliente,
        count(DISTINCT metabase_transacciones_de_uso.inicio_de_actividad::date) AS s_actual
    FROM metabase_transacciones_de_uso LEFT JOIN periodos ON true
    WHERE metabase_transacciones_de_uso.inicio_de_actividad >= periodos.fecha_inicio1
    GROUP BY metabase_transacciones_de_uso.nombre_cliente
    ), sesiones_anteriores AS (
    SELECT metabase_transacciones_de_uso.nombre_cliente,
        count(DISTINCT metabase_transacciones_de_uso.inicio_de_actividad::date) AS s_anterior
    FROM metabase_transacciones_de_uso LEFT JOIN periodos ON true
    WHERE metabase_transacciones_de_uso.inicio_de_actividad >= periodos.fecha_inicio2 AND metabase_transacciones_de_uso.inicio_de_actividad < periodos.fecha_inicio1
    GROUP BY metabase_transacciones_de_uso.nombre_cliente
    ), sesiones_por_cliente AS (
    SELECT a_1.nombre_cliente,
        a_1.s_actual,
        b.s_anterior,
        f_margen(b.s_anterior::numeric, a_1.s_actual::numeric) AS margen_sesiones,
            CASE
                WHEN a_1.s_actual >= 10 THEN 1
                WHEN a_1.s_actual >= 8 AND a_1.s_actual <= 9 THEN 2
                WHEN a_1.s_actual >= 6 AND a_1.s_actual <= 7 THEN 3
                WHEN a_1.s_actual >= 4 AND a_1.s_actual <= 5 THEN 4
                WHEN a_1.s_actual <= 3 THEN 5
                WHEN a_1.s_actual IS NULL THEN 5
                ELSE NULL::integer
            END AS puntaje
    FROM sesiones_actuales a_1 LEFT JOIN sesiones_anteriores b ON a_1.nombre_cliente = b.nombre_cliente
    ORDER BY a_1.nombre_cliente DESC
    ), notificaciones AS (
    SELECT kpis_notificaciones.nit,
        kpis_notificaciones.notificaciones_actuales,
        kpis_notificaciones.notificaciones_anteriores,
        kpis_notificaciones.riesgo_notificaciones,
        kpis_notificaciones.puntaje
    FROM kpis_notificaciones
    ), ofertas AS (
    SELECT kpis_ofertas.nit,
        kpis_ofertas.ofertas_actuales,
        kpis_ofertas.ofertas_anteriores,
        kpis_ofertas.riesgo_oferta,
        kpis_ofertas.puntaje
    FROM kpis_ofertas
    ), adjudicacion AS (
    SELECT kpis_adjudicaciones.nit,
        kpis_adjudicaciones.adjudicaciones_actuales,
        kpis_adjudicaciones.adjudicaciones_anteriores,
        kpis_adjudicaciones.riesgo_adjudicacion,
        kpis_adjudicaciones.puntaje_adjudicacion,
        kpis_adjudicaciones.monto_actual,
        kpis_adjudicaciones.monto_anterior,
        kpis_adjudicaciones.riesgo_adjudicacion_monto,
        kpis_adjudicaciones.puntaje_monto
    FROM kpis_adjudicaciones
    ), base_clientes AS (
    SELECT cliente.nit,
        max(btrim(split_part(cliente.nombre::text, '-'::text, 1))) AS nombre,
        max(cliente.estatus::text) AS estatus,
        max(p.s_actual) AS sesiones_actuales,
        max(p.s_anterior) AS sesiones_anteriores,
        COALESCE(max(p.puntaje), 5) AS puntaje_sesiones
    FROM cliente LEFT JOIN sesiones_por_cliente p
    ON f_limpiar_texto(ARRAY[cliente.nombre::text], ' '::text) = f_limpiar_texto(ARRAY[p.nombre_cliente], ' '::text)
    WHERE clean(cliente.estatus)::text ~* 'gratis|intro|basica|avanzada|premium|plus|prime'::text
    GROUP BY cliente.nit
)
SELECT cl.nit,
    cl.nombre,
    cl.estatus,
    cl.sesiones_actuales,
    cl.sesiones_anteriores,
    COALESCE(cl.puntaje_sesiones, 5) AS puntaje_sesiones,
    n.notificaciones_actuales,
    n.notificaciones_anteriores,
    n.riesgo_notificaciones,
    COALESCE(n.puntaje, 5) AS puntaje_notificaciones,
    o.ofertas_actuales,
    o.ofertas_anteriores,
    o.riesgo_oferta,
    COALESCE(o.puntaje, 5) AS puntaje_oferta,
    a.adjudicaciones_actuales,
    a.adjudicaciones_anteriores,
    a.riesgo_adjudicacion,
    COALESCE(a.puntaje_adjudicacion, 5) AS puntaje_adjudicacion,
    a.monto_actual,
    a.monto_anterior,
    a.riesgo_adjudicacion_monto AS riesgo_monto,
    COALESCE(a.puntaje_monto, 5) AS puntaje_monto,
    round((COALESCE(cl.puntaje_sesiones, 5)::numeric + COALESCE(n.puntaje, 5)::numeric + COALESCE(o.puntaje, 5)::numeric + COALESCE(a.puntaje_adjudicacion, 5)::numeric + COALESCE(a.puntaje_monto, 5)::numeric) / 5::numeric) AS puntaje,
    now() AS fecha_ultima_actualizacion
FROM base_clientes cl
    LEFT JOIN notificaciones n ON n.nit = cl.nit::text
    LEFT JOIN ofertas o ON o.nit::text = cl.nit::text
    LEFT JOIN adjudicacion a ON a.nit::text = cl.nit::text
ORDER BY cl.nit
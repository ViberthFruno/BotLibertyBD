-- ============================================================================
-- SCRIPT DE MIGRACIÓN: BotLibertyBD → Sistema Trustonic
-- ============================================================================
-- Este script migra el proyecto BotLibertyBD para usar el sistema del compañero
-- basado en tipos compuestos PostgreSQL y funciones PL/pgSQL
-- ============================================================================

-- ============================================================================
-- PASO 1: Crear schema 'canje' si no existe
-- ============================================================================
CREATE SCHEMA IF NOT EXISTS canje;

COMMENT ON SCHEMA canje IS 'Schema para gestión de lista negra Trustonic Trade-In';


-- ============================================================================
-- PASO 2: Crear tabla en schema 'canje' (migración desde automatizacion)
-- ============================================================================
-- OPCIÓN A: Si quieres mantener ambas tablas (recomendado para testing)
CREATE TABLE IF NOT EXISTS canje.lista_negra_tradein_trustonic (
    id SERIAL PRIMARY KEY,
    imei_serie VARCHAR(255) NOT NULL UNIQUE,
    fecha_cliente TIMESTAMPTZ,  -- ⚠️ CAMBIO: ahora con timezone
    creado TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,  -- ⚠️ CAMBIO: con timezone
    actualizado TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,  -- ⚠️ CAMBIO: con timezone
    activo BOOLEAN NOT NULL DEFAULT TRUE,
    detalle VARCHAR(255),
    usuario_creador UUID,  -- Nuevo: quién creó el registro
    usuario_actualizo UUID  -- Nuevo: quién actualizó por última vez
);

-- Índices para optimizar búsquedas
CREATE INDEX IF NOT EXISTS idx_lista_negra_imei_serie ON canje.lista_negra_tradein_trustonic(imei_serie);
CREATE INDEX IF NOT EXISTS idx_lista_negra_activo ON canje.lista_negra_tradein_trustonic(activo);
CREATE INDEX IF NOT EXISTS idx_lista_negra_fecha_cliente ON canje.lista_negra_tradein_trustonic(fecha_cliente);

COMMENT ON TABLE canje.lista_negra_tradein_trustonic IS 'Lista negra de IMEIs/Series para Trade-In Trustonic';
COMMENT ON COLUMN canje.lista_negra_tradein_trustonic.imei_serie IS 'IMEI o número de serie del dispositivo (único)';
COMMENT ON COLUMN canje.lista_negra_tradein_trustonic.fecha_cliente IS 'Fecha de registro del cliente (con zona horaria)';
COMMENT ON COLUMN canje.lista_negra_tradein_trustonic.activo IS 'Estado del registro (true=activo, false=desactivado/obsoleto)';
COMMENT ON COLUMN canje.lista_negra_tradein_trustonic.detalle IS 'Información adicional (ej: traiding_trustonic)';


-- ============================================================================
-- PASO 2B (OPCIONAL): Migrar datos existentes desde automatizacion.datos_excel_doforms
-- ============================================================================
-- ⚠️ EJECUTAR SOLO SI QUIERES MIGRAR DATOS EXISTENTES
-- ⚠️ Descomenta las siguientes líneas si necesitas migrar:
/*
INSERT INTO canje.lista_negra_tradein_trustonic (
    imei_serie,
    fecha_cliente,
    creado,
    actualizado,
    activo,
    detalle
)
SELECT
    imei_serie,
    fecha_cliente::TIMESTAMPTZ,  -- Convertir a timezone-aware (asume UTC)
    creado::TIMESTAMPTZ,
    actualizado::TIMESTAMPTZ,
    activo,
    detalle
FROM automatizacion.datos_excel_doforms
ON CONFLICT (imei_serie) DO NOTHING;  -- Evitar duplicados
*/


-- ============================================================================
-- PASO 3: Crear tipo compuesto para la función
-- ============================================================================
DROP TYPE IF EXISTS canje.registro_texto__fecha_tz CASCADE;

CREATE TYPE canje.registro_texto__fecha_tz AS (
    texto TEXT,
    fecha TIMESTAMPTZ
);

COMMENT ON TYPE canje.registro_texto__fecha_tz IS 'Tipo compuesto para pasar registros IMEI+fecha con timezone a la función de actualización';


-- ============================================================================
-- PASO 4: Crear función PL/pgSQL principal
-- ============================================================================
-- Esta función replica la lógica del método sync_imeis() de Python
-- pero se ejecuta completamente en PostgreSQL para mayor rendimiento
-- ============================================================================

CREATE OR REPLACE FUNCTION canje.upd_lista_negra_tradein_trustonic_pa(
    p_usuario_id UUID,
    p_registros canje.registro_texto__fecha_tz[]
)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    -- Contadores
    v_actualizados INT := 0;
    v_insertados INT := 0;
    v_desactivados INT := 0;
    v_activos_final INT := 0;

    -- Conflictos y errores
    v_conflictos JSONB := '[]'::JSONB;

    -- Variables de procesamiento
    v_registro RECORD;
    v_existe BOOLEAN;
    v_coincide BOOLEAN;
    v_imei_serie TEXT;
    v_fecha_cliente TIMESTAMPTZ;

    -- Variables de control de errores
    v_error_code TEXT;
    v_error_message TEXT;
    v_error_detail TEXT;

    -- Arrays para tracking
    v_imeis_excel TEXT[];
    v_imeis_bd TEXT[];
    v_imeis_nuevos TEXT[];
    v_imeis_existentes TEXT[];
    v_imeis_obsoletos TEXT[];
BEGIN
    -- ========================================================================
    -- VALIDACIONES INICIALES
    -- ========================================================================

    -- Validar que se proporcionó un usuario
    IF p_usuario_id IS NULL THEN
        RETURN jsonb_build_object(
            'exito', FALSE,
            'error', 'Parámetro requerido',
            'mensaje', 'usuario_id no puede ser NULL'
        );
    END IF;

    -- Validar que se proporcionaron registros
    IF p_registros IS NULL OR array_length(p_registros, 1) IS NULL THEN
        RETURN jsonb_build_object(
            'exito', FALSE,
            'error', 'Parámetro requerido',
            'mensaje', 'Se requiere al menos un registro para procesar'
        );
    END IF;


    -- ========================================================================
    -- PREPARAR ARRAYS DE IMEIs
    -- ========================================================================

    -- Extraer IMEIs del array de registros (filtrar NULLs y vacíos)
    SELECT ARRAY_AGG(DISTINCT (r).texto)
    INTO v_imeis_excel
    FROM unnest(p_registros) r
    WHERE (r).texto IS NOT NULL
      AND trim((r).texto) <> '';

    -- Si no hay IMEIs válidos, retornar error
    IF v_imeis_excel IS NULL OR array_length(v_imeis_excel, 1) IS NULL THEN
        RETURN jsonb_build_object(
            'exito', FALSE,
            'error', 'Datos inválidos',
            'mensaje', 'No se encontraron IMEIs válidos en los registros proporcionados'
        );
    END IF;

    -- Obtener IMEIs existentes en la base de datos
    SELECT ARRAY_AGG(imei_serie)
    INTO v_imeis_bd
    FROM canje.lista_negra_tradein_trustonic;

    -- Si es NULL (tabla vacía), inicializar como array vacío
    IF v_imeis_bd IS NULL THEN
        v_imeis_bd := ARRAY[]::TEXT[];
    END IF;


    -- ========================================================================
    -- CALCULAR DIFERENCIAS (SET OPERATIONS)
    -- ========================================================================

    -- IMEIs nuevos: en Excel pero NO en BD
    SELECT ARRAY_AGG(imei)
    INTO v_imeis_nuevos
    FROM unnest(v_imeis_excel) imei
    WHERE imei <> ALL(v_imeis_bd);

    IF v_imeis_nuevos IS NULL THEN
        v_imeis_nuevos := ARRAY[]::TEXT[];
    END IF;

    -- IMEIs existentes: en Excel Y en BD
    SELECT ARRAY_AGG(imei)
    INTO v_imeis_existentes
    FROM unnest(v_imeis_excel) imei
    WHERE imei = ANY(v_imeis_bd);

    IF v_imeis_existentes IS NULL THEN
        v_imeis_existentes := ARRAY[]::TEXT[];
    END IF;

    -- IMEIs obsoletos: en BD pero NO en Excel
    SELECT ARRAY_AGG(imei)
    INTO v_imeis_obsoletos
    FROM unnest(v_imeis_bd) imei
    WHERE imei <> ALL(v_imeis_excel);

    IF v_imeis_obsoletos IS NULL THEN
        v_imeis_obsoletos := ARRAY[]::TEXT[];
    END IF;


    -- ========================================================================
    -- CASO 1: INSERTAR NUEVOS IMEIs
    -- ========================================================================

    FOR v_registro IN
        SELECT (r).texto as imei_serie, (r).fecha as fecha_cliente
        FROM unnest(p_registros) r
        WHERE (r).texto = ANY(v_imeis_nuevos)
    LOOP
        BEGIN
            INSERT INTO canje.lista_negra_tradein_trustonic (
                imei_serie,
                fecha_cliente,
                creado,
                actualizado,
                activo,
                detalle,
                usuario_creador,
                usuario_actualizo
            ) VALUES (
                v_registro.imei_serie,
                v_registro.fecha_cliente,
                CURRENT_TIMESTAMP,
                CURRENT_TIMESTAMP,
                TRUE,
                'traiding_trustonic',
                p_usuario_id,
                p_usuario_id
            );

            v_insertados := v_insertados + 1;

        EXCEPTION
            WHEN unique_violation THEN
                -- Conflicto: IMEI duplicado (race condition)
                v_conflictos := v_conflictos || jsonb_build_object(
                    'imei_serie', v_registro.imei_serie,
                    'motivo', 'Violación de unicidad - IMEI ya existe',
                    'operacion', 'INSERT'
                );
            WHEN OTHERS THEN
                -- Otro error
                GET STACKED DIAGNOSTICS
                    v_error_code = RETURNED_SQLSTATE,
                    v_error_message = MESSAGE_TEXT,
                    v_error_detail = PG_EXCEPTION_DETAIL;

                v_conflictos := v_conflictos || jsonb_build_object(
                    'imei_serie', v_registro.imei_serie,
                    'motivo', 'Error al insertar: ' || v_error_message,
                    'codigo', v_error_code,
                    'detalle', v_error_detail,
                    'operacion', 'INSERT'
                );
        END;
    END LOOP;


    -- ========================================================================
    -- CASO 2: ACTUALIZAR IMEIs EXISTENTES
    -- ========================================================================

    FOR v_registro IN
        SELECT (r).texto as imei_serie, (r).fecha as fecha_cliente
        FROM unnest(p_registros) r
        WHERE (r).texto = ANY(v_imeis_existentes)
    LOOP
        BEGIN
            -- Actualizar solo si hay cambios (fecha diferente o estaba inactivo)
            UPDATE canje.lista_negra_tradein_trustonic
            SET
                fecha_cliente = v_registro.fecha_cliente,
                actualizado = CURRENT_TIMESTAMP,
                activo = TRUE,
                detalle = 'traiding_trustonic',
                usuario_actualizo = p_usuario_id
            WHERE imei_serie = v_registro.imei_serie
              AND (
                  -- Fecha cambió (comparando fechas sin hora)
                  DATE(fecha_cliente) <> DATE(v_registro.fecha_cliente)
                  -- O estaba inactivo
                  OR activo = FALSE
                  -- O fecha era NULL
                  OR fecha_cliente IS NULL
              );

            -- Verificar si se actualizó algo
            IF FOUND THEN
                v_actualizados := v_actualizados + 1;
            END IF;

        EXCEPTION
            WHEN OTHERS THEN
                GET STACKED DIAGNOSTICS
                    v_error_code = RETURNED_SQLSTATE,
                    v_error_message = MESSAGE_TEXT,
                    v_error_detail = PG_EXCEPTION_DETAIL;

                v_conflictos := v_conflictos || jsonb_build_object(
                    'imei_serie', v_registro.imei_serie,
                    'motivo', 'Error al actualizar: ' || v_error_message,
                    'codigo', v_error_code,
                    'detalle', v_error_detail,
                    'operacion', 'UPDATE'
                );
        END;
    END LOOP;


    -- ========================================================================
    -- CASO 3: DESACTIVAR IMEIs OBSOLETOS (SOFT DELETE)
    -- ========================================================================
    -- 🔥 IMPORTANTE: NUNCA SE ELIMINAN REGISTROS, SOLO SE MARCAN COMO INACTIVOS
    -- ========================================================================

    BEGIN
        UPDATE canje.lista_negra_tradein_trustonic
        SET
            activo = FALSE,
            actualizado = CURRENT_TIMESTAMP,
            usuario_actualizo = p_usuario_id
        WHERE imei_serie = ANY(v_imeis_obsoletos)
          AND activo = TRUE;  -- Solo desactivar los que están activos

        GET DIAGNOSTICS v_desactivados = ROW_COUNT;

    EXCEPTION
        WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS
                v_error_code = RETURNED_SQLSTATE,
                v_error_message = MESSAGE_TEXT,
                v_error_detail = PG_EXCEPTION_DETAIL;

            v_conflictos := v_conflictos || jsonb_build_object(
                'motivo', 'Error al desactivar IMEIs obsoletos: ' || v_error_message,
                'codigo', v_error_code,
                'detalle', v_error_detail,
                'operacion', 'UPDATE (desactivar)'
            );
    END;


    -- ========================================================================
    -- CALCULAR TOTALES FINALES
    -- ========================================================================

    SELECT COUNT(*)
    INTO v_activos_final
    FROM canje.lista_negra_tradein_trustonic
    WHERE activo = TRUE;


    -- ========================================================================
    -- RETORNAR RESULTADO
    -- ========================================================================

    RETURN jsonb_build_object(
        'exito', TRUE,
        'insertados', v_insertados,
        'actualizados', v_actualizados,
        'desactivados', v_desactivados,
        'activos_final', v_activos_final,
        'conflictos_count', jsonb_array_length(v_conflictos),
        'conflictos', v_conflictos,
        'procesados_total', array_length(v_imeis_excel, 1),
        'timestamp', CURRENT_TIMESTAMP
    );

EXCEPTION
    -- Capturar cualquier error no manejado
    WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS
            v_error_code = RETURNED_SQLSTATE,
            v_error_message = MESSAGE_TEXT,
            v_error_detail = PG_EXCEPTION_DETAIL;

        RETURN jsonb_build_object(
            'exito', FALSE,
            'error', 'Error inesperado en función',
            'codigo', v_error_code,
            'mensaje', v_error_message,
            'detalle', v_error_detail,
            'timestamp', CURRENT_TIMESTAMP
        );
END;
$$;

-- Comentarios de la función
COMMENT ON FUNCTION canje.upd_lista_negra_tradein_trustonic_pa IS
'Sincroniza lista negra de IMEIs/Series para Trade-In Trustonic.
Implementa lógica de INSERT (nuevos), UPDATE (existentes), y SOFT-DELETE (obsoletos).
NUNCA elimina registros físicamente, solo marca como activo=FALSE.';


-- ============================================================================
-- PASO 5: Crear función de testing (útil para verificar funcionamiento)
-- ============================================================================

CREATE OR REPLACE FUNCTION canje.test_upd_lista_negra()
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_test_usuario UUID := '00000000-0000-0000-0000-000000000001';
    v_test_registros canje.registro_texto__fecha_tz[];
    v_resultado JSONB;
BEGIN
    -- Crear registros de prueba
    v_test_registros := ARRAY[
        ROW('123456789012345', '2024-01-15 10:30:00-06'::TIMESTAMPTZ)::canje.registro_texto__fecha_tz,
        ROW('234567890123456', '2024-01-16 11:45:00-06'::TIMESTAMPTZ)::canje.registro_texto__fecha_tz,
        ROW('345678901234567', '2024-01-17 14:20:00-06'::TIMESTAMPTZ)::canje.registro_texto__fecha_tz
    ];

    -- Ejecutar función
    SELECT canje.upd_lista_negra_tradein_trustonic_pa(v_test_usuario, v_test_registros)
    INTO v_resultado;

    RETURN v_resultado;
END;
$$;

COMMENT ON FUNCTION canje.test_upd_lista_negra IS
'Función de prueba para verificar el funcionamiento de upd_lista_negra_tradein_trustonic_pa';


-- ============================================================================
-- PASO 6: Grants de permisos (ajustar según necesidades)
-- ============================================================================

-- IMPORTANTE: Reemplaza 'tu_usuario_app' con el usuario real de tu aplicación
-- GRANT USAGE ON SCHEMA canje TO tu_usuario_app;
-- GRANT SELECT, INSERT, UPDATE ON canje.lista_negra_tradein_trustonic TO tu_usuario_app;
-- GRANT USAGE ON SEQUENCE canje.lista_negra_tradein_trustonic_id_seq TO tu_usuario_app;
-- GRANT EXECUTE ON FUNCTION canje.upd_lista_negra_tradein_trustonic_pa TO tu_usuario_app;


-- ============================================================================
-- VERIFICACIÓN FINAL
-- ============================================================================

-- Verificar que todo se creó correctamente
DO $$
BEGIN
    RAISE NOTICE '============================================================';
    RAISE NOTICE 'Verificación de Migración Completada';
    RAISE NOTICE '============================================================';

    IF EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'canje') THEN
        RAISE NOTICE '✓ Schema "canje" creado';
    END IF;

    IF EXISTS (SELECT 1 FROM pg_type WHERE typname = 'registro_texto__fecha_tz') THEN
        RAISE NOTICE '✓ Tipo compuesto "registro_texto__fecha_tz" creado';
    END IF;

    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'canje' AND tablename = 'lista_negra_tradein_trustonic') THEN
        RAISE NOTICE '✓ Tabla "canje.lista_negra_tradein_trustonic" creada';
    END IF;

    IF EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'upd_lista_negra_tradein_trustonic_pa') THEN
        RAISE NOTICE '✓ Función "upd_lista_negra_tradein_trustonic_pa" creada';
    END IF;

    RAISE NOTICE '============================================================';
    RAISE NOTICE 'Migración SQL completada exitosamente';
    RAISE NOTICE 'Siguiente paso: Actualizar código Python';
    RAISE NOTICE '============================================================';
END $$;


-- ============================================================================
-- NOTAS IMPORTANTES
-- ============================================================================
--
-- 1. Este script crea una NUEVA tabla en schema "canje"
-- 2. La tabla anterior "automatizacion.datos_excel_doforms" NO se modifica
-- 3. Si quieres migrar datos, descomenta la sección PASO 2B
-- 4. Todos los timestamps ahora son TIMESTAMPTZ (con zona horaria)
-- 5. La función retorna JSONB con estadísticas detalladas
-- 6. NUNCA se eliminan registros (solo soft-delete con activo=FALSE)
-- 7. Ejecutar este script con un usuario que tenga permisos de CREATE
--
-- ============================================================================

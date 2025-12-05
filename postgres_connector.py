# postgres_connector.py
"""
Conector de PostgreSQL para la aplicación EnlaceDB.

Este archivo proporciona funcionalidades para conectarse a una base de datos PostgreSQL,
ejecutar consultas y gestionar las conexiones de manera segura con manejo de errores
apropiado y registros detallados, incluyendo soporte para diferentes configuraciones de SSL
y un sistema robusto de detección automática de la configuración óptima para cada servidor.
"""

import traceback
from logger import logger
from datetime import date, datetime, time

# Intentar importar el módulo de PostgreSQL con manejo de errores
try:
    import psycopg2
    from psycopg2 import OperationalError, DatabaseError
    from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT, register_adapter
    import psycopg2.extras


    # Registrar adaptadores para tipos de datos comunes
    def adapt_date(d):
        return psycopg2.extensions.AsIs("'%s'::date" % d.isoformat())


    def adapt_datetime(dt):
        return psycopg2.extensions.AsIs("'%s'::timestamp" % dt.isoformat(sep=' ', timespec='seconds'))


    def adapt_time(t):
        return psycopg2.extensions.AsIs("'%s'::time" % t.strftime('%H:%M:%S'))


    # Registrar adaptadores para tipos más específicos
    register_adapter(date, adapt_date)
    register_adapter(datetime, adapt_datetime)
    register_adapter(time, adapt_time)

    PSYCOPG2_AVAILABLE = True
except ImportError:
    logger.warning("Módulo psycopg2 no disponible. La funcionalidad de PostgreSQL estará limitada.")
    PSYCOPG2_AVAILABLE = False


class PostgresConnector:
    """Clase para manejar la conexión y operaciones con bases de datos PostgreSQL."""

    def __init__(self, host="localhost", port="5432", database="postgres", username="postgres", password="",
                 sslmode="require"):
        """
        Inicializa el conector de PostgreSQL.

        Args:
            host (str): Nombre o dirección IP del servidor PostgreSQL.
            port (str): Puerto del servidor PostgreSQL.
            database (str): Nombre de la base de datos.
            username (str): Nombre de usuario para la conexión.
            password (str): Contraseña para la conexión.
            sslmode (str): Modo de SSL para la conexión (por defecto "require").
        """
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password
        self.connection = None
        self.cursor = None

        # Permitir que el usuario defina un modo SSL inicial explícito
        normalized_sslmode = sslmode.strip() if isinstance(sslmode, str) else sslmode
        self.ssl_mode = normalized_sslmode if normalized_sslmode else None
        if self.ssl_mode:
            self.ssl_mode = self._normalize_connection_value(self.ssl_mode)

    @staticmethod
    def _normalize_connection_value(value):
        """Normaliza los valores de conexión para manejar tipos y codificaciones."""
        if value is None:
            return None

        if isinstance(value, bytes):
            for encoding in ("utf-8", "latin-1"):
                try:
                    return value.decode(encoding)
                except UnicodeDecodeError:
                    continue
            return value.decode("utf-8", errors="ignore")

        if isinstance(value, (int, float)):
            return str(value)

        return value

    def _build_connection_parameters(self, extra_params=None):
        """Construye los parámetros de conexión preparados para psycopg2."""
        params = {
            "host": self.host,
            "port": self.port,
            "dbname": self.database,
            "user": self.username,
            "password": self.password,
            "connect_timeout": 10,  # Timeout de 10 segundos para evitar bloqueos
        }

        if extra_params:
            params.update(extra_params)

        if self.ssl_mode and "sslmode" not in params:
            params["sslmode"] = self.ssl_mode

        prepared_params = {}
        for key, value in params.items():
            normalized = self._normalize_connection_value(value)
            if normalized is not None:
                prepared_params[key] = normalized

        return prepared_params

    def test_connection(self):
        """
        Prueba la conexión a la base de datos PostgreSQL intentando diferentes configuraciones.

        Returns:
            tuple: (bool, str) - Éxito de la conexión y mensaje descriptivo.
        """
        if not PSYCOPG2_AVAILABLE:
            return False, "El módulo psycopg2 no está instalado. Por favor, instálelo para usar esta funcionalidad."

        # Lista de configuraciones SSL a probar
        ssl_configs = []

        # Intentar primero con el modo SSL configurado manualmente (si existe)
        if self.ssl_mode:
            ssl_configs.append({"sslmode": self._normalize_connection_value(self.ssl_mode)})

        # Añadir modos adicionales asegurando que no se repitan
        for mode in ("prefer", "require", "disable", "allow"):
            if not any(cfg.get("sslmode") == mode for cfg in ssl_configs):
                ssl_configs.append({"sslmode": mode})

        # Finalmente probar sin especificar SSL para utilizar la configuración por defecto del servidor
        ssl_configs.append({})

        last_error = None
        original_ssl_mode = self.ssl_mode
        for ssl_config in ssl_configs:
            try:
                # Intentar conectarse con esta configuración
                logger.info(f"Probando conexión a PostgreSQL con configuración: {ssl_config}")

                conn_params = self._build_connection_parameters(ssl_config)

                log_conn_params = conn_params.copy()
                if "password" in log_conn_params:
                    log_conn_params["password"] = "*****"
                logger.debug(f"Parámetros de conexión: {log_conn_params}")

                # Establecer la conexión utilizando parámetros explícitos para manejar codificaciones
                connection = psycopg2.connect(**conn_params)
                connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
                cursor = connection.cursor()

                # Ejecutar una consulta simple para verificar la conexión
                cursor.execute("SELECT version();")
                version = cursor.fetchone()

                # Cerrar cursor y conexión
                cursor.close()
                connection.close()

                # Guardar la configuración SSL exitosa
                if "sslmode" in ssl_config and ssl_config["sslmode"]:
                    self.ssl_mode = self._normalize_connection_value(ssl_config["sslmode"])
                else:
                    fallback_mode = original_ssl_mode or "prefer"
                    self.ssl_mode = self._normalize_connection_value(fallback_mode)

                # Registro exitoso
                logger.info(f"Conexión a PostgreSQL exitosa con configuración: {ssl_config}")
                return True, f"Conexión exitosa. Versión del servidor: {version[0]}"

            except (OperationalError, DatabaseError) as e:
                error_msg = f"Error con configuración {ssl_config}: {str(e)}"
                logger.warning(error_msg)
                last_error = str(e)
                continue
            except Exception as e:
                error_msg = f"Error inesperado con configuración {ssl_config}: {str(e)}"
                logger.warning(error_msg)
                last_error = str(e)
                continue

        # Si llegamos aquí, ninguna configuración funcionó
        detailed_error = (
            f"No se pudo conectar a la base de datos PostgreSQL con ninguna configuración. "
            f"Último error: {last_error}\n\n"
            f"POSIBLES SOLUCIONES:\n"
            f"1. Verifique que el servidor PostgreSQL permita conexiones desde su dirección IP.\n"
            f"2. El error 'no pg_hba.conf entry' significa que el administrador del servidor debe "
            f"autorizar su dirección IP en el archivo pg_hba.conf del servidor.\n"
            f"3. Confirme que las credenciales (usuario/contraseña) sean correctas.\n"
            f"4. Verifique que el nombre de la base de datos sea correcto.\n"
            f"5. Si está intentando usar SSL, asegúrese de que el servidor lo soporte."
        )

        # Restaurar el modo SSL original si no se logró establecer conexión
        self.ssl_mode = original_ssl_mode

        logger.error(detailed_error)
        logger.debug(traceback.format_exc())
        return False, detailed_error

    def connect(self):
        """
        Establece una conexión a la base de datos PostgreSQL usando la configuración SSL
        determinada previamente durante la prueba de conexión.

        Returns:
            bool: True si la conexión fue exitosa, False en caso contrario.
        """
        if not PSYCOPG2_AVAILABLE:
            logger.error("No se puede conectar: el módulo psycopg2 no está disponible")
            return False

        try:
            # Cerrar cualquier conexión existente primero
            self.disconnect()

            conn_params = self._build_connection_parameters()

            logger.info(
                f"Conectando a PostgreSQL: {self.host}:{self.port}/{self.database} (SSL: {self.ssl_mode or 'prefer'})")

            # Establecer la conexión con soporte para tipos de datos adicionales
            self.connection = psycopg2.connect(
                cursor_factory=psycopg2.extras.DictCursor,  # Permite acceder a las columnas por nombre
                **conn_params
            )
            self.connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            self.cursor = self.connection.cursor()

            logger.info("Conexión establecida con éxito")
            return True

        except Exception as e:
            error_msg = (
                f"Error al conectar a PostgreSQL: {str(e)}\n\n"
                f"Por favor, pruebe primero la conexión usando el botón 'Probar Conexión' "
                f"para determinar la configuración de conexión adecuada."
            )
            logger.error(error_msg)
            logger.debug(traceback.format_exc())
            return False

    def disconnect(self):
        """
        Cierra la conexión a la base de datos PostgreSQL.
        """
        try:
            if self.cursor:
                self.cursor.close()
                self.cursor = None
                logger.debug("Cursor cerrado")

            if self.connection:
                self.connection.close()
                self.connection = None
                logger.debug("Conexión cerrada")

        except Exception as e:
            logger.error(f"Error al desconectar de PostgreSQL: {str(e)}")
            logger.debug(traceback.format_exc())

    def execute_query(self, query, params=None):
        """
        Ejecuta una consulta SQL en la base de datos.

        Args:
            query (str): Consulta SQL a ejecutar.
            params (tuple, optional): Parámetros para la consulta. Por defecto es None.

        Returns:
            list: Resultados de la consulta, o None si hay error.
        """
        if not PSYCOPG2_AVAILABLE:
            logger.error("No se puede ejecutar consulta: el módulo psycopg2 no está disponible")
            return None

        if not self.connection or not self.cursor:
            if not self.connect():
                return None

        try:
            # Ejecutar la consulta
            if params:
                # Registrar la consulta sin mostrar valores sensibles
                safe_query = query
                if isinstance(params, (list, tuple)) and len(params) > 0:
                    safe_query = f"{query} [con {len(params)} parámetros]"
                logger.debug(f"Ejecutando consulta: {safe_query}")

                self.cursor.execute(query, params)
            else:
                logger.debug(f"Ejecutando consulta: {query}")
                self.cursor.execute(query)

            # Si es una consulta SELECT, retornar los resultados
            if query.strip().upper().startswith("SELECT"):
                results = self.cursor.fetchall()
                logger.debug(f"Consulta SELECT completada, {len(results)} filas obtenidas")
                return results

            # Si es una consulta de modificación, retornar True para indicar éxito
            logger.debug("Consulta de modificación completada")
            return True

        except Exception as e:
            logger.error(f"Error al ejecutar consulta: {str(e)}")
            logger.debug(f"Consulta: {query}")
            if params:
                try:
                    # Intentar registrar los parámetros sin revelar datos sensibles
                    param_count = len(params) if isinstance(params, (list, tuple)) else 1
                    logger.debug(f"Con {param_count} parámetros")
                except:
                    pass

            logger.debug(traceback.format_exc())

            # Intentar hacer rollback en caso de error
            try:
                if self.connection:
                    self.connection.rollback()
                    logger.debug("Rollback ejecutado")
            except Exception as rollback_error:
                logger.error(f"Error al hacer rollback: {str(rollback_error)}")

            return None

    def table_exists(self, schema, table):
        """
        Verifica si una tabla existe en la base de datos.

        Args:
            schema (str): Nombre del esquema.
            table (str): Nombre de la tabla.

        Returns:
            bool: True si la tabla existe, False en caso contrario.
        """
        query = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = %s
            AND table_name = %s
        );
        """

        result = self.execute_query(query, (schema, table))
        return result and result[0][0]

    def column_exists(self, schema, table, column):
        """
        Verifica si una columna existe en una tabla.

        Args:
            schema (str): Nombre del esquema.
            table (str): Nombre de la tabla.
            column (str): Nombre de la columna.

        Returns:
            bool: True si la columna existe, False en caso contrario.
        """
        query = """
        SELECT EXISTS (
            SELECT FROM information_schema.columns 
            WHERE table_schema = %s
            AND table_name = %s
            AND column_name = %s
        );
        """

        result = self.execute_query(query, (schema, table, column))
        return result and result[0][0]

    def schema_exists(self, schema):
        """Verifica si un esquema existe en la base de datos."""
        query = """
        SELECT EXISTS (
            SELECT FROM information_schema.schemata WHERE schema_name = %s
        );
        """
        result = self.execute_query(query, (schema,))
        return result and result[0][0]

    def get_column_type(self, schema, table, column):
        """
        Obtiene el tipo de datos de una columna.

        Args:
            schema (str): Nombre del esquema.
            table (str): Nombre de la tabla.
            column (str): Nombre de la columna.

        Returns:
            str: Tipo de datos de la columna o None si no se encuentra.
        """
        query = """
        SELECT data_type
        FROM information_schema.columns
        WHERE table_schema = %s
        AND table_name = %s
        AND column_name = %s;
        """

        result = self.execute_query(query, (schema, table, column))
        return result[0][0] if result and result[0] else None

    def ensure_imei_table_exists(self, schema, table):
        """
        Asegura que la tabla de IMEIs exista con la estructura correcta.

        Args:
            schema (str): Nombre del esquema.
            table (str): Nombre de la tabla.

        Returns:
            bool: True si la tabla existe o fue creada exitosamente.
        """
        try:
            # Verificar si el esquema existe, si no, crearlo
            if not self.schema_exists(schema):
                logger.info(f"Creando esquema: {schema}")
                create_schema_query = f'CREATE SCHEMA "{schema}";'
                if not self.execute_query(create_schema_query):
                    logger.error(f"No se pudo crear el esquema {schema}")
                    return False

            # Verificar si la tabla existe
            if self.table_exists(schema, table):
                logger.info(f"La tabla {schema}.{table} ya existe")
                return True

            # Crear la tabla con la estructura necesaria
            logger.info(f"Creando tabla: {schema}.{table}")
            create_table_query = f"""
            CREATE TABLE "{schema}"."{table}" (
                id SERIAL PRIMARY KEY,
                imei_serie VARCHAR(255) NOT NULL UNIQUE,
                fecha_cliente TIMESTAMP,
                creado TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                actualizado TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                activo BOOLEAN NOT NULL DEFAULT TRUE,
                detalle VARCHAR(255)
            );
            """

            result = self.execute_query(create_table_query)
            if result is not None:
                logger.info(f"Tabla {schema}.{table} creada exitosamente")
                return True
            else:
                logger.error(f"Error al crear la tabla {schema}.{table}")
                return False

        except Exception as e:
            logger.error(f"Error al asegurar existencia de tabla: {str(e)}")
            logger.debug(traceback.format_exc())
            return False

    def sync_imeis(self, schema, table, excel_data):
        """
        Sincroniza los IMEIs del Excel con la base de datos.

        Implementa 3 casos:
        1. IMEIs nuevos (en Excel, no en BD) -> INSERT
        2. IMEIs existentes (en Excel y en BD) -> UPDATE
        3. IMEIs obsoletos (en BD, no en Excel) -> UPDATE activo=false

        Args:
            schema (str): Nombre del esquema.
            table (str): Nombre de la tabla.
            excel_data (list): Lista de diccionarios con keys 'imei' y 'fecha_cliente'.

        Returns:
            dict: Resumen de operaciones realizadas.
        """
        result = {
            'success': False,
            'nuevos': 0,
            'actualizados': 0,
            'desactivados': 0,
            'errors': []
        }

        try:
            # Asegurar que la tabla existe
            if not self.ensure_imei_table_exists(schema, table):
                result['errors'].append("No se pudo asegurar la existencia de la tabla")
                return result

            # Obtener todos los IMEIs activos de la base de datos
            query_get_all = f'SELECT imei_serie FROM "{schema}"."{table}" WHERE activo = TRUE;'
            db_imeis_result = self.execute_query(query_get_all)

            db_imeis = set()
            if db_imeis_result:
                db_imeis = {row[0] for row in db_imeis_result}

            # Obtener IMEIs del Excel
            excel_imeis = {item['imei'] for item in excel_data if item.get('imei')}

            # Caso 1: IMEIs nuevos (en Excel, no en BD)
            nuevos_imeis = excel_imeis - db_imeis
            for imei_data in excel_data:
                imei = imei_data.get('imei')
                if imei and imei in nuevos_imeis:
                    fecha_cliente = imei_data.get('fecha_cliente')
                    insert_query = f"""
                    INSERT INTO "{schema}"."{table}"
                    (imei_serie, fecha_cliente, creado, actualizado, activo, detalle)
                    VALUES (%s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, TRUE, %s);
                    """
                    if self.execute_query(insert_query, (imei, fecha_cliente, 'traiding_trustonic')):
                        result['nuevos'] += 1
                        logger.debug(f"IMEI nuevo insertado: {imei}")
                    else:
                        result['errors'].append(f"Error al insertar IMEI: {imei}")

            # Caso 2: IMEIs existentes (en Excel y en BD) -> Actualizar
            existentes_imeis = excel_imeis & db_imeis
            for imei_data in excel_data:
                imei = imei_data.get('imei')
                if imei and imei in existentes_imeis:
                    fecha_cliente = imei_data.get('fecha_cliente')
                    update_query = f"""
                    UPDATE "{schema}"."{table}"
                    SET fecha_cliente = %s,
                        actualizado = CURRENT_TIMESTAMP,
                        activo = TRUE,
                        detalle = %s
                    WHERE imei_serie = %s;
                    """
                    if self.execute_query(update_query, (fecha_cliente, 'traiding_trustonic', imei)):
                        result['actualizados'] += 1
                        logger.debug(f"IMEI actualizado: {imei}")
                    else:
                        result['errors'].append(f"Error al actualizar IMEI: {imei}")

            # Caso 3: IMEIs obsoletos (en BD, no en Excel) -> Marcar como inactivos
            obsoletos_imeis = db_imeis - excel_imeis
            for imei in obsoletos_imeis:
                update_query = f"""
                UPDATE "{schema}"."{table}"
                SET activo = FALSE,
                    actualizado = CURRENT_TIMESTAMP
                WHERE imei_serie = %s;
                """
                if self.execute_query(update_query, (imei,)):
                    result['desactivados'] += 1
                    logger.debug(f"IMEI desactivado: {imei}")
                else:
                    result['errors'].append(f"Error al desactivar IMEI: {imei}")

            result['success'] = True
            logger.info(f"Sincronización completada: {result['nuevos']} nuevos, {result['actualizados']} actualizados, {result['desactivados']} desactivados")

        except Exception as e:
            error_msg = f"Error durante la sincronización: {str(e)}"
            result['errors'].append(error_msg)
            logger.error(error_msg)
            logger.debug(traceback.format_exc())

        return result


def is_psycopg2_installed():
    """
    Verifica si el módulo psycopg2 está instalado.

    Returns:
        bool: True si está instalado, False en caso contrario.
    """
    return PSYCOPG2_AVAILABLE

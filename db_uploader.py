# db_uploader.py
"""
Gestor de carga de datos a PostgreSQL para EnlaceDB.

Este módulo proporciona funcionalidades para verificar la estructura de la base de datos,
crear o alterar tablas según sea necesario, y cargar datos procesados a PostgreSQL.
Está diseñado para ser reutilizable para distintos tipos de carga en el sistema.
"""

import traceback
from datetime import datetime
import pandas as pd
from logger import logger


class DBUploader:
    """Clase para gestionar la carga de datos a PostgreSQL."""

    def __init__(self, connector, schema="automatizacion", table="datos_excel_doforms"):
        """
        Inicializa el cargador de base de datos.

        Args:
            connector: Conector de PostgreSQL.
            schema (str): Esquema de la base de datos.
            table (str): Tabla de la base de datos.
        """
        self.connector = connector
        self.schema = schema
        self.table = table
        self.progress_callback = None
        self.message_callback = None

    def set_callbacks(self, progress_callback=None, message_callback=None):
        """
        Establece callbacks para informar sobre el progreso y mensajes.

        Args:
            progress_callback (callable): Función para informar sobre el progreso (0-1).
            message_callback (callable): Función para enviar mensajes informativos.
        """
        self.progress_callback = progress_callback
        self.message_callback = message_callback

    def _send_progress(self, progress):
        """Envía una actualización de progreso si hay un callback configurado."""
        if self.progress_callback:
            self.progress_callback(progress)

    def _send_message(self, message, level="INFO"):
        """Envía un mensaje si hay un callback configurado."""
        if self.message_callback:
            self.message_callback(message, level)

        log_method = {"WARNING": logger.warning, "ERROR": logger.error}.get(level, logger.info)
        log_method(f"SUCCESS: {message}" if level == "SUCCESS" else message)

    def verify_table_structure(self, column_types):
        """Verifica si la tabla existe y tiene la estructura correcta."""
        try:
            if not self.connector.connect():
                self._send_message("No se pudo establecer conexión con la base de datos", "ERROR")
                return False
            self._send_message(f"Verificando si la tabla {self.schema}.{self.table} existe...")
            if hasattr(self.connector, 'schema_exists') and not self.connector.schema_exists(self.schema):
                self._send_message(f"El esquema {self.schema} no existe. Creando...", "WARNING")
                if self.connector.execute_query(f"CREATE SCHEMA IF NOT EXISTS {self.schema};") is None:
                    self._send_message(f"Error al crear el esquema {self.schema}", "ERROR")
                    return False
            if not self.connector.table_exists(self.schema, self.table):
                self._send_message(f"La tabla {self.schema}.{self.table} no existe. Creando...", "WARNING")
                columns_sql = ",\n    ".join(f"{c} {t}" for c, t in column_types.items())
                create_query = f"""
                CREATE TABLE {self.schema}.{self.table} (
                    id SERIAL PRIMARY KEY,
                    {columns_sql},
                    fecha_insercion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """
                if self.connector.execute_query(create_query) is None:
                    self._send_message(f"Error al crear la tabla {self.schema}.{self.table}", "ERROR")
                    return False
                self._send_message(f"Tabla {self.schema}.{self.table} creada exitosamente", "SUCCESS")
            else:
                self._send_message(f"La tabla {self.schema}.{self.table} existe. Verificando estructura...", "INFO")
                for column, col_type in column_types.items():
                    if not self.connector.column_exists(self.schema, self.table, column):
                        self._send_message(f"La columna {column} no existe. Agregando...", "WARNING")
                        if self.connector.execute_query(
                                f"ALTER TABLE {self.schema}.{self.table} ADD COLUMN {column} {col_type};") is None:
                            self._send_message(f"Error al agregar la columna {column}", "ERROR")
                        else:
                            self._send_message(f"Columna {column} agregada exitosamente", "SUCCESS")
                self._send_message(f"Estructura de la tabla {self.schema}.{self.table} verificada", "SUCCESS")
            return True
        except Exception as e:
            self._send_message(f"Error al verificar la estructura de la tabla: {str(e)}", "ERROR")
            logger.error(traceback.format_exc())
            return False

    def upload_data(self, data):
        """
        Carga datos a la base de datos.

        Args:
            data (list): Lista de diccionarios con los datos a cargar.

        Returns:
            tuple: (éxito, resultados)
                - éxito (bool): True si la carga fue exitosa, False si hubo errores críticos
                - resultados (dict): Información sobre éxitos, errores, etc.
        """
        if not data:
            self._send_message("No hay datos para cargar", "WARNING")
            return False, {"success_count": 0, "error_count": 0, "errors": []}

        if not self.connector.connect():
            self._send_message("No se pudo establecer conexión con la base de datos", "ERROR")
            return False, {"success_count": 0, "error_count": 0, "errors": ["Error de conexión"]}

        total_records = len(data)
        success_count = 0
        error_count = 0
        errors = []

        self._send_message(f"Iniciando carga de {total_records} registros a la base de datos...", "INFO")

        try:
            fields = list(data[0].keys())
            placeholders = ", ".join(["%s"] * len(fields))
            fields_str = ", ".join(fields)
            query = f"""
            INSERT INTO {self.schema}.{self.table} ({fields_str})
            VALUES ({placeholders})
            """
            for i, record in enumerate(data):
                try:
                    values = []
                    for db_field in fields:
                        value = record.get(db_field)
                        if db_field in ["fecha_reporte", "fecha_compromiso"] and isinstance(value, (datetime, pd.Timestamp)):
                            value = value.date()
                        values.append(value)
                    result = self.connector.execute_query(query, tuple(values))
                    if result is not None:
                        success_count += 1
                    else:
                        error_count += 1
                        errors.append(f"Error al insertar registro {i + 1}")
                except Exception as e:
                    error_count += 1
                    errors.append(f"Error en registro {i + 1}: {str(e)}")
                    logger.error(f"Error al insertar registro {i + 1}: {str(e)}")
                self._send_progress((i + 1) / total_records)
                if (i + 1) % 10 == 0 or i == total_records - 1:
                    self._send_message(
                        f"Procesados: {i + 1}/{total_records} | Éxitos: {success_count} | Errores: {error_count}", "INFO")

            self._send_message(
                f"Carga completada: {success_count} éxitos, {error_count} errores",
                "SUCCESS" if error_count == 0 else "WARNING")

            return True, {
                "success_count": success_count,
                "error_count": error_count,
                "errors": errors,
                "total_records": total_records
            }

        except Exception as e:
            self._send_message(f"Error crítico durante la carga de datos: {str(e)}", "ERROR")
            logger.error(traceback.format_exc())
            return False, {
                "success_count": success_count,
                "error_count": error_count + (total_records - (success_count + error_count)),
                "errors": errors + [f"Error crítico: {str(e)}"],
                "total_records": total_records
            }
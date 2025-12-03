# excel_automation_processor.py
"""
Procesador de automatización de archivos Excel para EnlaceDB.

Este módulo proporciona funcionalidades específicas para el procesamiento automático
de múltiples archivos Excel descargados desde correos electrónicos, incluyendo validación,
procesamiento por lotes y limpieza de archivos temporales, respetando la configuración
de esquema y tabla definida por el usuario.
"""

import os
import shutil
import tempfile
from logger import logger
from excel_processor import ExcelProcessor
from db_uploader import DBUploader


class ExcelAutomationProcessor:
    """Clase para procesar automáticamente múltiples archivos Excel."""

    def __init__(self, connector, schema="automatizacion", table="datos_excel_doforms",
                 progress_callback=None, message_callback=None):
        """
        Inicializa el procesador de automatización.

        Args:
            connector: Conector de base de datos PostgreSQL.
            schema (str): Esquema de destino en la base de datos.
            table (str): Tabla de destino en la base de datos.
            progress_callback: Función para reportar progreso (0-1).
            message_callback: Función para enviar mensajes.
        """
        self.connector = connector
        self.schema = schema
        self.table = table
        self.progress_callback = progress_callback
        self.message_callback = message_callback
        self.processed_files = []
        self.failed_files = []
        self.total_records_processed = 0

    def _send_progress(self, progress):
        """Envía una actualización de progreso si hay un callback configurado."""
        if self.progress_callback:
            self.progress_callback(progress)

    def _send_message(self, message, level="INFO"):
        """Envía un mensaje si hay un callback configurado."""
        if self.message_callback:
            self.message_callback(message, level)

        # También registrar en el logger
        if level == "INFO":
            logger.info(message)
        elif level == "SUCCESS":
            logger.info(f"SUCCESS: {message}")
        elif level == "WARNING":
            logger.warning(message)
        elif level == "ERROR":
            logger.error(message)

    def process_excel_files(self, excel_files, temp_dir=None):
        """
        Procesa múltiples archivos Excel y los carga a la base de datos.

        Args:
            excel_files (list): Lista de rutas de archivos Excel a procesar.
            temp_dir (str): Directorio temporal donde están los archivos.

        Returns:
            dict: Resumen de resultados del procesamiento.
        """
        if not excel_files:
            self._send_message("No hay archivos Excel para procesar", "WARNING")
            return {
                "success": True,
                "total_files": 0,
                "processed_files": 0,
                "failed_files": 0,
                "total_records": 0,
                "summary": "No hay archivos para procesar"
            }

        total_files = len(excel_files)
        self._send_message(f"Iniciando procesamiento de {total_files} archivos Excel...", "INFO")
        self._send_message(f"Destino: {self.schema}.{self.table}", "INFO")

        # Resetear contadores
        self.processed_files = []
        self.failed_files = []
        self.total_records_processed = 0

        # Crear procesador de Excel y configurar DB uploader
        excel_processor = ExcelProcessor()
        db_uploader = DBUploader(self.connector, schema=self.schema, table=self.table)

        # Configurar callbacks para el DB uploader
        db_uploader.set_callbacks(
            progress_callback=None,  # No usar progreso individual para cada archivo
            message_callback=self._send_message
        )

        # Verificar estructura de tabla una sola vez
        self._send_message("Verificando estructura de la base de datos...", "INFO")
        column_types = excel_processor.get_column_types()
        if not db_uploader.verify_table_structure(column_types):
            self._send_message("Error al verificar estructura de la base de datos", "ERROR")
            return {
                "success": False,
                "total_files": total_files,
                "processed_files": 0,
                "failed_files": total_files,
                "total_records": 0,
                "summary": "Error en la estructura de la base de datos"
            }

        # Procesar cada archivo Excel
        for i, excel_file in enumerate(excel_files):
            try:
                filename = os.path.basename(excel_file)
                self._send_message(f"Procesando archivo {i + 1}/{total_files}: {filename}", "INFO")

                # Actualizar progreso general
                file_progress = (i / total_files) * 0.8  # 80% para procesamiento de archivos
                self._send_progress(file_progress)

                # Cargar y procesar el archivo Excel
                success, message, data = excel_processor.load_file(excel_file)

                if not success:
                    self._send_message(f"Error al cargar {filename}: {message}", "ERROR")
                    self.failed_files.append({"file": filename, "error": message})
                    continue

                # Obtener datos procesados
                processed_data = excel_processor.get_processed_data()

                if not processed_data:
                    self._send_message(f"No se encontraron datos válidos en {filename}", "WARNING")
                    self.failed_files.append({"file": filename, "error": "Sin datos válidos"})
                    continue

                # Cargar datos a la base de datos
                self._send_message(f"Cargando {len(processed_data)} registros de {filename} a {self.schema}.{self.table}...",
                                   "INFO")

                upload_success, upload_results = db_uploader.upload_data(processed_data)

                if upload_success:
                    success_count = upload_results["success_count"]
                    error_count = upload_results["error_count"]

                    self.processed_files.append({
                        "file": filename,
                        "records": success_count,
                        "errors": error_count
                    })

                    self.total_records_processed += success_count

                    if error_count == 0:
                        self._send_message(f"✓ {filename}: {success_count} registros cargados correctamente", "SUCCESS")
                    else:
                        self._send_message(f"⚠ {filename}: {success_count} registros cargados, {error_count} errores",
                                           "WARNING")
                else:
                    error_msg = f"Error al cargar {filename} a la base de datos"
                    self._send_message(error_msg, "ERROR")
                    self.failed_files.append({"file": filename, "error": error_msg})

            except Exception as e:
                error_msg = f"Error inesperado al procesar {os.path.basename(excel_file)}: {str(e)}"
                self._send_message(error_msg, "ERROR")
                self.failed_files.append({"file": os.path.basename(excel_file), "error": str(e)})
                logger.error(f"Error en procesamiento de archivo: {str(e)}")

        # Progreso final
        self._send_progress(1.0)

        # Limpiar archivos temporales si se proporcionó el directorio
        if temp_dir and os.path.exists(temp_dir):
            try:
                shutil.rmtree(temp_dir)
                self._send_message("Archivos temporales limpiados", "INFO")
            except Exception as cleanup_error:
                self._send_message(f"Error al limpiar archivos temporales: {str(cleanup_error)}", "WARNING")

        # Preparar resumen de resultados
        processed_count = len(self.processed_files)
        failed_count = len(self.failed_files)

        # Crear resumen detallado
        summary_lines = []
        summary_lines.append(f"Procesamiento completado:")
        summary_lines.append(f"- Total de archivos: {total_files}")
        summary_lines.append(f"- Archivos procesados exitosamente: {processed_count}")
        summary_lines.append(f"- Archivos con errores: {failed_count}")
        summary_lines.append(f"- Total de registros cargados: {self.total_records_processed}")
        summary_lines.append(f"- Destino: {self.schema}.{self.table}")

        if self.processed_files:
            summary_lines.append("\nArchivos procesados:")
            for file_info in self.processed_files:
                if file_info["errors"] == 0:
                    summary_lines.append(f"  ✓ {file_info['file']}: {file_info['records']} registros")
                else:
                    summary_lines.append(
                        f"  ⚠ {file_info['file']}: {file_info['records']} registros, {file_info['errors']} errores")

        if self.failed_files:
            summary_lines.append("\nArchivos con errores:")
            for file_info in self.failed_files:
                summary_lines.append(f"  ✗ {file_info['file']}: {file_info['error']}")

        summary = "\n".join(summary_lines)

        # Determinar nivel de mensaje final
        if failed_count == 0:
            self._send_message(summary, "SUCCESS")
            final_success = True
        elif processed_count > 0:
            self._send_message(summary, "WARNING")
            final_success = True
        else:
            self._send_message(summary, "ERROR")
            final_success = False

        return {
            "success": final_success,
            "total_files": total_files,
            "processed_files": processed_count,
            "failed_files": failed_count,
            "total_records": self.total_records_processed,
            "summary": summary,
            "details": {
                "processed": self.processed_files,
                "failed": self.failed_files
            }
        }
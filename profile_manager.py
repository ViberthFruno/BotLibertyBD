# profile_manager.py
"""
Gestor de perfiles de automatización para EnlaceDB.

Este módulo se encarga de gestionar todos los aspectos relacionados con los perfiles
de automatización, incluyendo la persistencia en archivos JSON, validación de horarios,
programación de tareas automáticas y ejecución de perfiles. Los archivos de configuración
se gestionan de manera centralizada sin duplicación de código.
"""

import os
import json
import datetime
import threading
from tkinter import messagebox
from logger import logger
from perfil_dialog import PerfilDialog
from excel_automation_processor import ExcelAutomationProcessor


DEFAULT_MAILBOX = "INBOX"


class ProfileManager:
    """Clase que gestiona los perfiles de automatización."""

    def __init__(self, parent, ui_manager, get_connector_callback=None):
        """
        Inicializa el gestor de perfiles.

        Args:
            parent: Widget padre para diálogos.
            ui_manager: Gestor de interfaz de usuario.
            get_connector_callback: Función para obtener el conector de base de datos.
        """
        self.parent = parent
        self.ui_manager = ui_manager
        self.get_connector_callback = get_connector_callback

        # Configuración de archivos
        self.profiles_file = self._get_profiles_file_path()
        self.profiles = []
        self.profile_check_interval = 30  # Segundos entre verificaciones de perfiles
        self.profile_timer = None

        # Estado de procesamiento
        self.is_processing = False
        self.processing_thread = None

        # Configuración de correo
        self.default_mailbox = DEFAULT_MAILBOX

    def _get_profiles_file_path(self):
        """
        Obtiene la ruta absoluta para el archivo de perfiles de automatización.
        Usa la misma lógica de directorio que la aplicación principal.

        Returns:
            str: Ruta absoluta del archivo de perfiles.
        """
        try:
            # Obtener el directorio donde está la aplicación principal
            import sys
            if getattr(sys, 'frozen', False):
                app_dir = os.path.dirname(sys.executable)
            else:
                app_dir = os.path.dirname(os.path.abspath(__file__))

            profiles_path = os.path.join(app_dir, "automatizacion_perfiles.json")
            logger.debug(f"Ruta de archivo de perfiles: {profiles_path}")
            return profiles_path

        except Exception as e:
            logger.error(f"Error al obtener ruta de perfiles: {e}")
            # Fallback a directorio actual
            return os.path.join(os.getcwd(), "automatizacion_perfiles.json")

    def initialize(self):
        """Inicializa el gestor de perfiles."""
        try:
            # Cargar perfiles guardados
            self.load_profiles()

            # Verificar configuración de correo disponible
            if not self.ui_manager.get_email_connector():
                self.ui_manager.add_log(
                    "Advertencia: no hay configuración de correo disponible.",
                    "WARNING"
                )

            # Iniciar temporizador de verificación
            self.start_profile_timer()

            logger.info("Gestor de perfiles inicializado correctamente")

        except Exception as e:
            error_msg = f"Error al inicializar gestor de perfiles: {str(e)}"
            logger.error(error_msg)
            self.ui_manager.add_log(error_msg, "ERROR")

    def load_profiles(self):
        """Carga los perfiles guardados en el archivo."""
        try:
            if os.path.exists(self.profiles_file):
                with open(self.profiles_file, 'r', encoding='utf-8') as f:
                    self.profiles = json.load(f)
                logger.info(f"Perfiles cargados: {len(self.profiles)} perfiles desde {self.profiles_file}")
            else:
                logger.info(f"No se encontró archivo de perfiles: {self.profiles_file}")
                self.profiles = []

            if self._ensure_default_mailbox():
                logger.info("Perfiles actualizados para usar la bandeja de entrada predeterminada")
                self.save_profiles()

            # Actualizar UI
            self.ui_manager.refresh_profiles_table(self.profiles)

        except json.JSONDecodeError as e:
            logger.error(f"Error de formato JSON en archivo de perfiles: {e}")
            self._handle_corrupted_profiles_file()

        except Exception as e:
            logger.error(f"Error al cargar perfiles: {str(e)}")
            self.profiles = []
            self.ui_manager.refresh_profiles_table(self.profiles)

    def _handle_corrupted_profiles_file(self):
        """Maneja el caso de archivo de perfiles corrupto."""
        try:
            # Crear backup del archivo corrupto
            backup_name = f"{self.profiles_file}.backup"
            if os.path.exists(self.profiles_file):
                os.rename(self.profiles_file, backup_name)
                logger.warning(f"Archivo de perfiles corrupto respaldado como: {backup_name}")

            # Notificar al usuario
            messagebox.showwarning(
                "Archivo de Perfiles Corrupto",
                f"El archivo de perfiles estaba corrupto y fue respaldado.\n\n"
                f"Se creará un nuevo archivo de perfiles.\n"
                f"Respaldo guardado en: {os.path.basename(backup_name)}"
            )

            # Reinicializar con lista vacía
            self.profiles = []
            self.ui_manager.refresh_profiles_table(self.profiles)

        except Exception as e:
            logger.error(f"Error al manejar archivo corrupto: {e}")

    def _ensure_default_mailbox(self):
        """Garantiza que todos los perfiles utilicen la bandeja de entrada por defecto."""
        updated = False
        for profile in self.profiles:
            current_folder = (profile.get("folder_path") or "").strip()
            if current_folder.upper() != self.default_mailbox.upper():
                profile["folder_path"] = self.default_mailbox
                updated = True

        return updated

    def _get_mailbox_details(self, profile):
        """Obtiene la ruta y etiqueta legible de la bandeja utilizada por el perfil."""
        folder_path = (profile.get("folder_path") or "").strip()
        if not folder_path or folder_path.upper() != self.default_mailbox.upper():
            folder_path = self.default_mailbox
            profile["folder_path"] = folder_path

        normalized = folder_path.replace("\\", "/")
        display_name = "Bandeja de entrada" if normalized.upper() == "INBOX" else normalized.split("/")[-1]
        return folder_path, display_name

    def save_profiles(self):
        """
        Guarda los perfiles en el archivo.

        Returns:
            bool: True si se guardó correctamente, False en caso contrario.
        """
        try:
            # Asegurar que el directorio existe
            profiles_dir = os.path.dirname(self.profiles_file)
            if profiles_dir and not os.path.exists(profiles_dir):
                os.makedirs(profiles_dir, exist_ok=True)

            # Guardar perfiles
            with open(self.profiles_file, 'w', encoding='utf-8') as f:
                json.dump(self.profiles, f, indent=4, ensure_ascii=False)

            logger.info(f"Perfiles guardados: {len(self.profiles)} perfiles en {self.profiles_file}")
            return True

        except PermissionError:
            error_msg = f"Sin permisos para escribir archivo de perfiles: {self.profiles_file}"
            logger.error(error_msg)
            messagebox.showerror("Error de Permisos",
                                 f"No se pudo guardar los perfiles.\n\n"
                                 f"Verifique los permisos del directorio.")
            return False

        except Exception as e:
            error_msg = f"Error al guardar perfiles: {str(e)}"
            logger.error(error_msg)
            messagebox.showerror("Error al Guardar",
                                 f"No se pudieron guardar los perfiles:\n\n{str(e)}")
            return False

    def add_profile(self):
        """Abre el diálogo para añadir un nuevo perfil."""
        if not self.ui_manager.get_email_connector():
            messagebox.showerror("Error", "La configuración de correo no está disponible.")
            return

        try:
            # Crear diálogo de perfil
            dialog = PerfilDialog(self.parent, default_folder=self.default_mailbox)

            # Esperar a que se cierre el diálogo
            self.parent.wait_window(dialog)

            # Procesar resultado
            if dialog.result:
                # Añadir perfil a la lista
                self.profiles.append(dialog.result)

                # Refrescar tabla
                self.ui_manager.refresh_profiles_table(self.profiles)

                # Guardar perfiles
                if self.save_profiles():
                    logger.info(f"Perfil añadido: {dialog.result['name']}")
                    self.ui_manager.add_log(f"Perfil '{dialog.result['name']}' añadido correctamente", "SUCCESS")

        except Exception as e:
            error_msg = f"Error al añadir perfil: {str(e)}"
            logger.error(error_msg)
            self.ui_manager.add_log(error_msg, "ERROR")

    def edit_profile(self):
        """Abre el diálogo para editar el perfil seleccionado."""
        selected_index = self.ui_manager.get_selected_profile_index()
        if selected_index is None:
            messagebox.showwarning("Sin selección", "Seleccione un perfil para editar.")
            return

        try:
            # Obtener perfil seleccionado
            profile = self.profiles[selected_index]

            # Crear diálogo con datos existentes
            dialog = PerfilDialog(
                self.parent,
                default_folder=self.default_mailbox,
                existing_data=profile
            )

            # Esperar a que se cierre el diálogo
            self.parent.wait_window(dialog)

            # Procesar resultado
            if dialog.result:
                # Actualizar perfil
                old_name = self.profiles[selected_index].get("name", "Sin nombre")
                self.profiles[selected_index] = dialog.result

                # Refrescar tabla
                self.ui_manager.refresh_profiles_table(self.profiles)

                # Guardar perfiles
                if self.save_profiles():
                    logger.info(f"Perfil editado: {old_name} -> {dialog.result['name']}")
                    self.ui_manager.add_log(f"Perfil '{dialog.result['name']}' editado correctamente", "SUCCESS")

        except Exception as e:
            error_msg = f"Error al editar perfil: {str(e)}"
            logger.error(error_msg)
            self.ui_manager.add_log(error_msg, "ERROR")

    def delete_profile(self):
        """Elimina el perfil seleccionado."""
        selected_index = self.ui_manager.get_selected_profile_index()
        if selected_index is None:
            messagebox.showwarning("Sin selección", "Seleccione un perfil para eliminar.")
            return

        try:
            # Confirmar eliminación
            profile_name = self.profiles[selected_index].get("name", "Sin nombre")
            if not messagebox.askyesno("Confirmar Eliminación",
                                       f"¿Está seguro de eliminar el perfil '{profile_name}'?"):
                return

            # Eliminar perfil
            deleted_profile = self.profiles.pop(selected_index)

            # Refrescar tabla
            self.ui_manager.refresh_profiles_table(self.profiles)

            # Guardar perfiles
            if self.save_profiles():
                logger.info(f"Perfil eliminado: {deleted_profile.get('name')}")
                self.ui_manager.add_log(f"Perfil '{profile_name}' eliminado correctamente", "SUCCESS")

        except Exception as e:
            error_msg = f"Error al eliminar perfil: {str(e)}"
            logger.error(error_msg)
            self.ui_manager.add_log(error_msg, "ERROR")

    def execute_profile_manually(self):
        """Ejecuta manualmente el perfil seleccionado."""
        selected_index = self.ui_manager.get_selected_profile_index()
        if selected_index is None:
            messagebox.showwarning("Sin selección", "Seleccione un perfil para ejecutar.")
            return

        if self.is_processing:
            messagebox.showinfo("En progreso", "Ya hay un procesamiento en curso. Por favor espere.")
            return

        # Verificar conexión a la base de datos
        connector = self._get_database_connector()
        if not connector:
            messagebox.showerror("Error de conexión",
                                 "No hay una conexión activa a la base de datos.\n"
                                 "Por favor, configure y pruebe la conexión en la configuración PostgreSQL.")
            return

        # Obtener configuración de esquema y tabla
        schema, table = self._get_schema_table_config()

        # Obtener perfil seleccionado
        profile = self.profiles[selected_index]
        profile_name = profile.get("name", "Sin nombre")

        # Confirmar ejecución
        if not messagebox.askyesno("Confirmar Ejecución",
                                   f"¿Está seguro de ejecutar el perfil '{profile_name}' manualmente?\n\n"
                                   f"Esto buscará correos, descargará archivos Excel y los procesará.\n"
                                   f"Destino: {schema}.{table}"):
            return

        # Ejecutar perfil
        self.execute_profile(profile, is_manual=True)

    def execute_profile(self, profile, is_manual=False):
        """
        Ejecuta un perfil de automatización.

        Args:
            profile (dict): Perfil a ejecutar.
            is_manual (bool): Si es ejecución manual o automática.
        """
        # Verificar conexión a la base de datos
        connector = self._get_database_connector()
        if not connector:
            self.ui_manager.add_log("Error: No hay conexión a la base de datos", "ERROR")
            if is_manual:
                messagebox.showerror("Error de conexión",
                                     "No se pudo obtener conexión a la base de datos.")
            return

        # Obtener configuración de esquema y tabla
        schema, table = self._get_schema_table_config()

        # Extraer información del perfil
        folder_path, folder_name = self._get_mailbox_details(profile)
        title_filter = profile.get("title_filter", "")
        today_only = profile.get("today_only", True)
        profile_name = profile.get("name", "Sin nombre")

        # Registrar ejecución
        execution_type = "manual" if is_manual else "automática"
        self.ui_manager.add_log(f"=== Ejecutando perfil ({execution_type}): {profile_name} ===", "INFO")
        self.ui_manager.add_log(f"Bandeja: {folder_name} ({folder_path})", "INFO")
        self.ui_manager.add_log(f"Filtro de título: {title_filter}", "INFO")
        self.ui_manager.add_log(f"Solo correos del día actual: {'Sí' if today_only else 'No'}", "INFO")
        self.ui_manager.add_log(f"Destino: {schema}.{table}", "INFO")

        # Limpiar actividad anterior si es ejecución manual
        if is_manual:
            self.ui_manager.clear_activity()
            self.ui_manager.add_log(f"=== Ejecución manual del perfil: {profile_name} ===", "INFO")

        # Iniciar procesamiento en un hilo
        self.is_processing = True
        self.processing_thread = threading.Thread(
            target=self._process_profile_thread,
            args=(profile, connector, schema, table, is_manual),
            daemon=True
        )
        self.processing_thread.start()

    def _process_profile_thread(self, profile, connector, schema, table, is_manual):
        """
        Procesa un perfil en un hilo separado.

        Args:
            profile (dict): Perfil a procesar.
            connector: Conector de base de datos.
            schema (str): Esquema de destino.
            table (str): Tabla de destino.
            is_manual (bool): Si es ejecución manual.
        """
        try:
            # Extraer información del perfil
            folder_path, folder_name = self._get_mailbox_details(profile)
            title_filter = profile.get("title_filter", "")
            today_only = profile.get("today_only", True)
            profile_name = profile.get("name", "Sin nombre")

            # Crear función de callback para actualizar interfaz
            def status_callback(message, level="INFO"):
                if level == "_PROGRESS_":  # Caso especial para barra de progreso
                    self.parent.after(0, lambda: self.ui_manager.update_progress(message))
                else:
                    self.parent.after(0, lambda m=message, l=level: self.ui_manager.add_log(m, l))

            def result_callback(result_text):
                self.parent.after(0, lambda r=result_text: self.ui_manager.add_result(r))

            # Fase 1: Buscar correos y descargar archivos Excel
            self.parent.after(0, lambda: self.ui_manager.add_log(
                "Fase 1: Buscando correos y descargando archivos Excel...", "INFO"))

            connector_email = self.ui_manager.get_email_connector()
            if not connector_email:
                self.parent.after(0, lambda: self.ui_manager.add_log(
                    "No hay conector de correo disponible", "ERROR"))
                return

            search_results = connector_email.search_emails_and_download_excel(
                folder_path=folder_path,
                title_filter=title_filter,
                today_only=today_only,
                status_callback=status_callback,
                result_callback=result_callback
            )

            if not search_results["success"]:
                error_msg = f"Error en búsqueda: {search_results['message']}"
                self.parent.after(0, lambda: self.ui_manager.add_log(error_msg, "ERROR"))

                # Solo mostrar pop-up si es ejecución manual
                if is_manual:
                    self.parent.after(0, lambda: messagebox.showerror(
                        "Error de Búsqueda",
                        f"Se produjo un error durante la búsqueda:\n\n{search_results['message']}"
                    ))
                return

            excel_files = search_results.get("excel_files", [])
            temp_dir = search_results.get("temp_dir")

            if not excel_files:
                warning_msg = "No se encontraron archivos Excel en los correos"
                self.parent.after(0, lambda: self.ui_manager.add_log(warning_msg, "WARNING"))

                # Solo mostrar pop-up si es ejecución manual
                if is_manual:
                    self.parent.after(0, lambda: messagebox.showinfo(
                        "Sin archivos Excel",
                        f"No se encontraron archivos Excel en los correos de la bandeja '{folder_name}'."
                    ))
                return

            # Fase 2: Procesar archivos Excel y cargar a la base de datos
            self.parent.after(0, lambda: self.ui_manager.add_log(
                f"Fase 2: Procesando {len(excel_files)} archivos Excel...", "INFO"))

            # Crear procesador de automatización con esquema y tabla configurados
            automation_processor = ExcelAutomationProcessor(
                connector=connector,
                schema=schema,
                table=table,
                progress_callback=lambda progress: self.parent.after(0,
                                                                     lambda: self.ui_manager.update_progress(progress)),
                message_callback=lambda msg, level: self.parent.after(0,
                                                                      lambda m=msg, l=level: self.ui_manager.add_log(m,
                                                                                                                     l))
            )

            # Procesar archivos Excel
            processing_results = automation_processor.process_excel_files(excel_files, temp_dir)

            # Mostrar resumen final
            final_level = "SUCCESS" if processing_results["success"] and processing_results["failed_files"] == 0 else \
                "WARNING" if processing_results["success"] else "ERROR"

            self.parent.after(0, lambda: self.ui_manager.add_log("=== RESUMEN DE PROCESAMIENTO ===", final_level))
            self.parent.after(0, lambda: self.ui_manager.add_log(processing_results["summary"], final_level))

            # Solo mostrar pop-ups para ejecuciones manuales
            if is_manual:
                if processing_results["success"]:
                    self.parent.after(0, lambda: messagebox.showinfo(
                        "Procesamiento Completado",
                        f"Se ha completado el procesamiento del perfil '{profile_name}'.\n\n"
                        f"Archivos procesados: {processing_results['processed_files']}\n"
                        f"Registros cargados: {processing_results['total_records']}\n"
                        f"Destino: {schema}.{table}\n\n"
                        f"Revise los detalles en el área de actividad."
                    ))
                else:
                    self.parent.after(0, lambda: messagebox.showerror(
                        "Error en Procesamiento",
                        f"Se produjeron errores durante el procesamiento del perfil '{profile_name}'.\n\n"
                        f"Consulte los logs para más detalles."
                    ))
            else:
                # Para ejecuciones automáticas, solo registrar en logs
                if processing_results["total_records"] > 0:
                    logger.info(f"Procesamiento automático completado - Perfil: {profile_name}, "
                                f"Registros: {processing_results['total_records']}, Destino: {schema}.{table}")
                else:
                    logger.warning(f"Procesamiento automático sin resultados - Perfil: {profile_name}")

        except Exception as e:
            error_msg = f"Error crítico durante el procesamiento: {str(e)}"
            self.parent.after(0, lambda: self.ui_manager.add_log(error_msg, "ERROR"))
            logger.error(f"Error en procesamiento de perfil: {str(e)}")

            # Solo mostrar pop-up de error si es ejecución manual
            if is_manual:
                self.parent.after(0, lambda: messagebox.showerror(
                    "Error Crítico",
                    f"Se produjo un error crítico durante el procesamiento:\n\n{str(e)}"
                ))

        finally:
            self.is_processing = False
            # Resetear barra de progreso
            self.parent.after(0, lambda: self.ui_manager.update_progress(0))

    def start_profile_timer(self):
        """Inicia el temporizador para verificar perfiles periódicamente."""

        def check_profiles():
            try:
                if self.profiles:
                    self.check_profiles_execution()
            except Exception as e:
                logger.error(f"Error en verificación de perfiles: {str(e)}")
            finally:
                # Programar próxima verificación
                self.profile_timer = self.parent.after(
                    self.profile_check_interval * 1000,
                    check_profiles
                )

        # Iniciar primera verificación
        self.profile_timer = self.parent.after(
            self.profile_check_interval * 1000,
            check_profiles
        )

    def check_profiles_execution(self):
        """Verifica si algún perfil debe ejecutarse según la hora programada."""
        # No ejecutar si ya hay un procesamiento en curso
        if self.is_processing:
            return

        now = datetime.datetime.now()
        current_hour = now.hour
        current_minute = now.minute

        # Verificar cada perfil activo
        for profile in self.profiles:
            if not profile.get("enabled", False):
                continue

            profile_hour = profile.get("hour", 0)
            profile_minute = profile.get("minute", 0)

            # Obtener última ejecución
            last_run = profile.get("last_run")
            if last_run:
                try:
                    last_run = datetime.datetime.strptime(last_run, "%Y-%m-%d %H:%M:%S")

                    # Evitar ejecutar dos veces en el mismo día
                    if (last_run.day == now.day and
                            last_run.month == now.month and
                            last_run.year == now.year):
                        continue
                except ValueError:
                    logger.warning(f"Formato de fecha inválido en perfil: {last_run}")

            # Comprobar si es hora de ejecutar
            if current_hour == profile_hour and current_minute == profile_minute:
                # Actualizar última ejecución
                profile["last_run"] = now.strftime("%Y-%m-%d %H:%M:%S")

                # Guardar cambios
                self.save_profiles()

                # Ejecutar perfil programado (is_manual=False para ejecución automática)
                logger.info(f"Ejecutando perfil programado: {profile.get('name')}")
                self.execute_profile(profile, is_manual=False)
                break  # Solo ejecutar un perfil a la vez

    def _get_database_connector(self):
        """Obtiene el conector de base de datos con manejo de errores."""
        try:
            if self.get_connector_callback:
                return self.get_connector_callback()
            else:
                logger.error("No hay callback para obtener conector de base de datos")
                return None
        except Exception as e:
            logger.error(f"Error al obtener conector de base de datos: {str(e)}")
            return None

    def _get_schema_table_config(self):
        """
        Obtiene la configuración de esquema y tabla desde el UI manager.

        Returns:
            tuple: (esquema, tabla) configurados o valores por defecto seguros.
        """
        try:
            # Intentar obtener desde el UI manager
            if hasattr(self.ui_manager, 'get_schema_table_config'):
                return self.ui_manager.get_schema_table_config()

            # Obtener desde la aplicación padre
            app = self.parent
            while hasattr(app, 'master') and app.master:
                app = app.master

            if hasattr(app, 'get_schema_table_config'):
                return app.get_schema_table_config()

            # Valores por defecto seguros si no hay configuración
            logger.warning("No se pudo obtener configuración de esquema/tabla, usando valores por defecto")
            return "automatizacion", "datos_excel_doforms"

        except Exception as e:
            logger.error(f"Error al obtener configuración de esquema/tabla: {str(e)}")
            return "automatizacion", "datos_excel_doforms"

    def get_profiles(self):
        """Obtiene la lista de perfiles."""
        return self.profiles

    def is_email_available(self):
        """Verifica si el conector de correo está disponible."""
        return self.ui_manager.get_email_connector() is not None

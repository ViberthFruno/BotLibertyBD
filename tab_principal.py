# tab_principal.py
"""
Pestaña Principal para la aplicación EnlaceDB.

Este módulo implementa la pestaña principal optimizada de la aplicación, combinando
la gestión de perfiles de automatización y la configuración de conexión PostgreSQL
y correo electrónico en una sola vista. La configuración se maneja completamente
a través de archivos JSON sin valores hardcodeados.

OPTIMIZADO: Usa threading para operaciones de red sin bloquear la UI.
"""

import tkinter as tk
from tkinter import ttk, messagebox
from datetime import datetime
import threading
import queue

from logger import logger
from postgres_connector import PostgresConnector
from conexion_dialog import ConexionDialog
from correo_dialog import CorreoDialog
from email_connector import EmailConnector


class PrincipalTab:
    """Clase que implementa la pestaña principal optimizada de EnlaceDB."""

    def __init__(self, parent_frame, save_config_callback=None):
        """
        Inicializa el componente de la pestaña principal.

        Args:
            parent_frame: Frame contenedor donde se añadirán los componentes.
            save_config_callback: Función para guardar la configuración.
        """
        self.parent = parent_frame
        self.save_config_callback = save_config_callback

        # Conectores
        self.postgres_connector = None
        self.email_connector = None

        # Configuraciones (inicialmente vacías)
        self.postgres_config = {}
        self.email_config = {}
        self.search_params = {"titles": []}
        self.notify_users = []

        # Variables para la UI
        self.log_text = None

        # Variables para el monitoreo
        self.monitoring_active = False
        self.monitoring_job = None

        # Threading para operaciones de red sin bloquear UI
        self.monitoring_thread = None
        self.message_queue = queue.Queue()
        self.stop_monitoring_event = threading.Event()

        # Crear la estructura de la pestaña
        self._create_principal_tab()

        # Iniciar el procesador de mensajes de la cola
        self._process_queue()

    def _create_principal_tab(self):
        """Crea los widgets de la pestaña principal."""
        # Frame principal
        main_frame = ttk.Frame(self.parent)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        # ========== PANEL PRINCIPAL (ARRIBA) ==========
        panel_frame = ttk.LabelFrame(main_frame, text="Panel Principal", padding=20)
        panel_frame.pack(fill=tk.X, padx=5, pady=(5, 15))

        # Título del panel
        panel_title = ttk.Label(panel_frame, text="Control de Monitoreo", font=("Arial", 14, "bold"))
        panel_title.pack(pady=(0, 15))

        # Botón grande de monitoreo
        self.monitoring_button = tk.Button(
            panel_frame,
            text="▶ Iniciar Monitoreo",
            font=("Arial", 14, "bold"),
            bg="#4CAF50",
            fg="white",
            activebackground="#45a049",
            activeforeground="white",
            relief=tk.RAISED,
            bd=3,
            padx=30,
            pady=15,
            cursor="hand2",
            command=self._toggle_monitoring
        )
        self.monitoring_button.pack()

        # Estado del monitoreo
        self.monitoring_status_label = ttk.Label(
            panel_frame,
            text="Estado: Detenido",
            font=("Arial", 10),
            foreground="red"
        )
        self.monitoring_status_label.pack(pady=(10, 0))

        # ========== CONTENIDO INFERIOR (CONFIG + LOGS) ==========
        content_frame = ttk.Frame(main_frame)
        content_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        # Columna izquierda: Configuración
        left_frame = ttk.LabelFrame(content_frame, text="Configuración")
        left_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(0, 5))

        # Columna derecha: Registro de actividad
        right_frame = ttk.LabelFrame(content_frame, text="Registro de Actividad")
        right_frame.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True, padx=(5, 0))

        # Crear componentes de cada sección
        self._create_configuration_section(left_frame)
        self._create_activity_section(right_frame)


    def _create_configuration_section(self, parent):
        """Crea la sección de configuración simplificada."""
        # Frame principal para configuración
        config_frame = ttk.Frame(parent)
        config_frame.pack(fill=tk.X, pady=5, padx=5)

        # Botón para configurar PostgreSQL
        self.postgres_config_button = ttk.Button(
            config_frame,
            text="🔧 Configurar PostgreSQL",
            command=self._open_postgres_config,
            width=25
        )
        self.postgres_config_button.pack(pady=5)

        # Botón para configurar correo
        self.email_config_button = ttk.Button(
            config_frame,
            text="📧 Configuración de Correo",
            command=self._open_email_config,
            width=25
        )
        self.email_config_button.pack(pady=5)

        # Separador
        ttk.Separator(config_frame, orient=tk.HORIZONTAL).pack(fill=tk.X, pady=10)

        # Botón para Parámetros de Búsqueda
        self.search_params_button = ttk.Button(
            config_frame,
            text="🔍 Parámetros de Búsqueda",
            command=self._open_search_params,
            width=25
        )
        self.search_params_button.pack(pady=5)

        # Botón para Usuarios a Notificar
        self.notify_users_button = ttk.Button(
            config_frame,
            text="👥 Usuarios a Notificar",
            command=self._open_notify_users,
            width=25
        )
        self.notify_users_button.pack(pady=5)

        # Información de estado
        self.status_info = ttk.Label(
            config_frame,
            text="Estado: Sin configuración",
            foreground="orange"
        )
        self.status_info.pack(pady=10)

    def _create_activity_section(self, parent):
        """Crea la sección de registro de actividad."""
        # TextBox para los logs
        log_frame = ttk.Frame(parent)
        log_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        self.log_text = tk.Text(log_frame, wrap=tk.WORD, height=20)

        # Scrollbar para el texto
        log_scrollbar = ttk.Scrollbar(log_frame, command=self.log_text.yview)
        self.log_text.config(yscrollcommand=log_scrollbar.set)

        # Empaquetar
        self.log_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        log_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        self.log_text.configure(state=tk.DISABLED)

    # ===============================
    # GESTIÓN DE CONFIGURACIÓN
    # ===============================

    def _open_postgres_config(self):
        """Abre el diálogo de configuración PostgreSQL."""
        try:
            # Crear y mostrar el diálogo con configuración existente
            dialog = ConexionDialog(self.parent, existing_config=self.postgres_config)
            self.parent.wait_window(dialog)

            # Procesar resultado
            if dialog.result:
                self.postgres_config = dialog.result.copy()
                self._create_postgres_connector()
                self._save_all_config()
                self._update_status_info()
                self.add_log("Configuración PostgreSQL actualizada", "SUCCESS")
                logger.info("Configuración PostgreSQL actualizada por usuario")
            else:
                self.add_log("Configuración PostgreSQL cancelada", "INFO")

        except Exception as e:
            error_msg = f"Error al abrir configuración PostgreSQL: {str(e)}"
            self.add_log(error_msg, "ERROR")
            messagebox.showerror("Error", f"No se pudo abrir la configuración:\n\n{str(e)}")

    def _open_email_config(self):
        """Abre el diálogo de configuración de correo."""
        try:
            dialog = CorreoDialog(self.parent, existing_config=self.email_config)
            self.parent.wait_window(dialog)

            if dialog.result:
                self.email_config = dialog.result.copy()
                self._create_email_connector()
                self._save_all_config()
                self._update_status_info()
                self.add_log("Configuración de correo actualizada", "SUCCESS")
                logger.info("Configuración de correo actualizada por usuario")
            else:
                self.add_log("Configuración de correo cancelada", "INFO")

        except Exception as e:
            error_msg = f"Error al abrir configuración de correo: {str(e)}"
            self.add_log(error_msg, "ERROR")
            messagebox.showerror("Error", f"No se pudo abrir la configuración:\n\n{str(e)}")

    def _create_postgres_connector(self):
        """Crea un nuevo conector PostgreSQL basado en la configuración actual."""
        if not self.postgres_config:
            self.postgres_connector = None
            return

        try:
            self.postgres_connector = PostgresConnector(
                host=self.postgres_config.get("host", ""),
                port=self.postgres_config.get("port", "5432"),
                database=self.postgres_config.get("database", ""),
                username=self.postgres_config.get("username", ""),
                password=self.postgres_config.get("password", "")
            )
            logger.debug("Conector PostgreSQL creado")

        except Exception as e:
            self.add_log(f"Error al crear conector PostgreSQL: {str(e)}", "ERROR")
            self.postgres_connector = None

    def _create_email_connector(self):
        """Crea un nuevo conector de correo basado en la configuración actual."""
        if not self.email_config:
            self.email_connector = None
            return

        try:
            self.email_connector = EmailConnector(
                smtp_server=self.email_config.get("smtp_server", ""),
                smtp_port=self.email_config.get("smtp_port", 587),
                imap_server=self.email_config.get("imap_server", ""),
                imap_port=self.email_config.get("imap_port", 993),
                email_address=self.email_config.get("email", ""),
                password=self.email_config.get("password", ""),
            )
            logger.debug("Conector de correo creado")

        except Exception as e:
            self.add_log(f"Error al crear conector de correo: {str(e)}", "ERROR")
            self.email_connector = None

    def _save_all_config(self):
        """Guarda toda la configuración usando el callback."""
        if not self.save_config_callback:
            logger.warning("No hay callback para guardar configuración")
            return False

        try:
            full_config = {
                "postgres": self.postgres_config,
                "email": self.email_config,
                "search_params": self.search_params,
                "notify_users": self.notify_users,
            }

            if self.save_config_callback(full_config):
                logger.debug("Configuración completa guardada")
                return True
            else:
                self.add_log("Error al guardar configuración", "ERROR")
                return False

        except Exception as e:
            self.add_log(f"Error crítico al guardar: {str(e)}", "ERROR")
            return False

    def _update_status_info(self):
        """Actualiza la información de estado de configuración."""
        postgres_ok = bool(self.postgres_config)
        email_ok = bool(self.email_config)

        if postgres_ok and email_ok:
            status_text = "Estado: ✓ PostgreSQL y Correo configurados"
            color = "green"
        elif postgres_ok:
            status_text = "Estado: ⚠ Solo PostgreSQL configurado"
            color = "orange"
        elif email_ok:
            status_text = "Estado: ⚠ Solo Correo configurado"
            color = "orange"
        else:
            status_text = "Estado: ✗ Sin configuración"
            color = "red"

        self.status_info.configure(text=status_text, foreground=color)

    def _open_search_params(self):
        """Abre el diálogo de Parámetros de Búsqueda."""
        try:
            from parametros_dialog import ParametrosDialog
            dialog = ParametrosDialog(self.parent, existing_params=self.search_params)
            self.parent.wait_window(dialog)

            if dialog.result:
                self.search_params = dialog.result.copy()
                self._save_all_config()
                self.add_log(f"Parámetros de búsqueda actualizados: {len(self.search_params.get('titles', []))} título(s)", "SUCCESS")
                logger.info("Parámetros de búsqueda actualizados por usuario")
            else:
                self.add_log("Configuración de parámetros cancelada", "INFO")

        except Exception as e:
            error_msg = f"Error al abrir parámetros de búsqueda: {str(e)}"
            self.add_log(error_msg, "ERROR")
            messagebox.showerror("Error", f"No se pudo abrir la configuración:\n\n{str(e)}")

    def _open_notify_users(self):
        """Abre el diálogo de Usuarios a Notificar."""
        try:
            from usuarios_dialog import UsuariosDialog
            dialog = UsuariosDialog(self.parent, existing_users=self.notify_users)
            self.parent.wait_window(dialog)

            if dialog.result is not None:
                self.notify_users = dialog.result.copy()
                self._save_all_config()
                self.add_log(f"Usuarios a notificar actualizados: {len(self.notify_users)} usuario(s)", "SUCCESS")
                logger.info("Usuarios a notificar actualizados por usuario")
            else:
                self.add_log("Configuración de usuarios cancelada", "INFO")

        except Exception as e:
            error_msg = f"Error al abrir usuarios a notificar: {str(e)}"
            self.add_log(error_msg, "ERROR")
            messagebox.showerror("Error", f"No se pudo abrir la configuración:\n\n{str(e)}")

    def _toggle_monitoring(self):
        """Inicia o detiene el monitoreo de correos."""
        if not self.monitoring_active:
            # Iniciar monitoreo
            if not self._validate_monitoring_config():
                return

            self.monitoring_active = True
            self.stop_monitoring_event.clear()

            self.monitoring_button.configure(
                text="⏸ Detener Monitoreo",
                bg="#f44336"
            )
            self.monitoring_status_label.configure(
                text="Estado: Activo",
                foreground="green"
            )
            self.add_log("Monitoreo iniciado", "SUCCESS")
            self._start_monitoring_cycle()

        else:
            # Detener monitoreo
            self.monitoring_active = False
            self.stop_monitoring_event.set()

            if self.monitoring_job:
                self.parent.after_cancel(self.monitoring_job)
                self.monitoring_job = None

            # Esperar a que el hilo termine (si está corriendo)
            if self.monitoring_thread and self.monitoring_thread.is_alive():
                self.add_log("Esperando finalización del monitoreo actual...", "INFO")
                # No bloqueamos esperando, el hilo se detendrá solo

            self.monitoring_button.configure(
                text="▶ Iniciar Monitoreo",
                bg="#4CAF50"
            )
            self.monitoring_status_label.configure(
                text="Estado: Detenido",
                foreground="red"
            )
            self.add_log("Monitoreo detenido", "INFO")

    def _validate_monitoring_config(self):
        """Valida que la configuración necesaria esté completa para el monitoreo."""
        if not self.email_config:
            messagebox.showwarning(
                "Configuración Incompleta",
                "Debe configurar el correo electrónico antes de iniciar el monitoreo."
            )
            self.add_log("Intento de iniciar monitoreo sin configuración de correo", "WARNING")
            return False

        if not self.search_params.get("titles"):
            messagebox.showwarning(
                "Configuración Incompleta",
                "Debe configurar al menos un parámetro de búsqueda antes de iniciar el monitoreo."
            )
            self.add_log("Intento de iniciar monitoreo sin parámetros de búsqueda", "WARNING")
            return False

        if not self.notify_users:
            messagebox.showwarning(
                "Configuración Incompleta",
                "Debe configurar al menos un usuario a notificar antes de iniciar el monitoreo."
            )
            self.add_log("Intento de iniciar monitoreo sin usuarios a notificar", "WARNING")
            return False

        return True

    def _start_monitoring_cycle(self):
        """Inicia un ciclo de monitoreo usando threading para no bloquear la UI."""
        if not self.monitoring_active:
            return

        # Verificar que no haya un monitoreo ya corriendo
        if self.monitoring_thread and self.monitoring_thread.is_alive():
            self.add_log("Monitoreo anterior aún en ejecución, esperando...", "WARNING")
            # Programar para verificar más tarde
            self.monitoring_job = self.parent.after(10000, self._start_monitoring_cycle)
            return

        self.add_log("Iniciando ciclo de monitoreo...", "INFO")

        # Crear y ejecutar el hilo de monitoreo
        self.monitoring_thread = threading.Thread(
            target=self._execute_monitoring_thread,
            daemon=True
        )
        self.monitoring_thread.start()

        # Programar el próximo ciclo (cada 5 minutos = 300000 ms)
        self.monitoring_job = self.parent.after(300000, self._start_monitoring_cycle)

    def _execute_monitoring_thread(self):
        """
        Ejecuta el proceso de monitoreo de correos en un hilo separado.
        Usa la cola de mensajes para comunicarse con el hilo principal de Tkinter.
        """
        if not self.email_connector:
            self.message_queue.put(("log", "Error: No hay conector de correo disponible", "ERROR"))
            return

        if self.stop_monitoring_event.is_set():
            return

        try:
            # Buscar correos con los títulos configurados
            for title in self.search_params.get("titles", []):
                # Verificar si se solicitó detener
                if self.stop_monitoring_event.is_set():
                    self.message_queue.put(("log", "Monitoreo cancelado por usuario", "INFO"))
                    return

                self.message_queue.put(("log", f"Buscando correos con título: '{title}'", "INFO"))

                # Callback seguro para threading
                def thread_safe_callback(msg, level="INFO"):
                    self.message_queue.put(("log", msg, level))

                result = self.email_connector.monitor_and_notify(
                    title_filter=title,
                    notify_emails=self.notify_users,
                    status_callback=thread_safe_callback
                )

                if result.get("success"):
                    matching = result.get("matching_items", 0)
                    if matching > 0:
                        self.message_queue.put(("log", f"✓ {matching} correo(s) encontrado(s) y procesado(s)", "SUCCESS"))
                    else:
                        self.message_queue.put(("log", "No se encontraron correos nuevos", "INFO"))
                else:
                    self.message_queue.put(("log", f"Error en monitoreo: {result.get('message', 'Error desconocido')}", "ERROR"))

            self.message_queue.put(("log", "Ciclo de monitoreo completado", "SUCCESS"))

        except Exception as e:
            error_msg = f"Error durante el monitoreo: {str(e)}"
            self.message_queue.put(("log", error_msg, "ERROR"))
            logger.error(error_msg)

    def _process_queue(self):
        """
        Procesa mensajes de la cola en el hilo principal de Tkinter.
        Esta función se llama periódicamente para actualizar la UI con mensajes
        de hilos secundarios de forma segura.
        """
        try:
            # Procesar todos los mensajes disponibles en la cola
            while True:
                try:
                    msg_type, *args = self.message_queue.get_nowait()

                    if msg_type == "log":
                        # args[0] = message, args[1] = level
                        message = args[0]
                        level = args[1] if len(args) > 1 else "INFO"
                        self.add_log(message, level)

                except queue.Empty:
                    break

        except Exception as e:
            logger.error(f"Error procesando cola de mensajes: {e}")

        # Programar la próxima verificación (cada 100ms)
        self.parent.after(100, self._process_queue)

    # ===============================
    # INTERFAZ PARA OTROS MÓDULOS
    # ===============================

    def add_log(self, message, level="INFO"):
        """Añade un mensaje al área de logs."""
        colors = {
            "INFO": "black",
            "SUCCESS": "dark green",
            "WARNING": "orange",
            "ERROR": "red"
        }

        color = colors.get(level, "black")

        # Habilitar edición
        self.log_text.configure(state=tk.NORMAL)

        # Agregar fecha y hora
        timestamp = datetime.now().strftime("%H:%M:%S")

        # Insertar el mensaje con formato
        self.log_text.insert(tk.END, f"{timestamp} - {level}: ", "timestamp")
        self.log_text.insert(tk.END, f"{message}\n", level)

        # Configurar el color
        self.log_text.tag_config("timestamp", foreground="gray")
        self.log_text.tag_config(level, foreground=color)

        # Desplazar al final
        self.log_text.see(tk.END)

        # Deshabilitar edición
        self.log_text.configure(state=tk.DISABLED)

        # También registrar en el logger
        log_methods = {
            "INFO": logger.info,
            "SUCCESS": lambda msg: logger.info(f"SUCCESS: {msg}"),
            "WARNING": logger.warning,
            "ERROR": logger.error
        }
        log_methods.get(level, logger.info)(message)

    def add_result(self, text):
        """Añade un resultado al área de logs con formato especial."""
        self.log_text.configure(state=tk.NORMAL)

        # Insertar separador
        self.log_text.insert(tk.END, "-" * 50 + "\n", "separator")

        # Insertar el texto del resultado
        self.log_text.insert(tk.END, text + "\n", "result")

        # Insertar separador final
        self.log_text.insert(tk.END, "-" * 50 + "\n\n", "separator")

        # Configurar estilos
        self.log_text.tag_config("separator", foreground="blue")
        self.log_text.tag_config("result", foreground="black")

        # Desplazar al final
        self.log_text.see(tk.END)
        self.log_text.configure(state=tk.DISABLED)

    def clear_activity(self):
        """Limpia el área de logs."""
        self.log_text.configure(state=tk.NORMAL)
        self.log_text.delete(1.0, tk.END)
        self.log_text.configure(state=tk.DISABLED)


    def get_connector(self):
        """Proporciona acceso al conector PostgreSQL."""
        if not self.postgres_config:
            self.add_log("No hay configuración PostgreSQL disponible", "WARNING")
            return None

        if self.postgres_connector is None:
            self._create_postgres_connector()

        return self.postgres_connector

    def get_email_connector(self):
        """Proporciona acceso al conector de correo."""
        return self.email_connector

    def get_schema_table_config(self):
        """Obtiene la configuración actual de esquema y tabla."""
        if not self.postgres_config:
            # Valores por defecto SOLO cuando no hay configuración
            return "automatizacion", "datos_excel_doforms"

        return (
            self.postgres_config.get("schema", "automatizacion"),
            self.postgres_config.get("table", "datos_excel_doforms")
        )

    def apply_config(self, config):
        """Aplica la configuración cargada desde el archivo JSON."""
        if not config:
            self.add_log("No hay configuración previa para cargar", "INFO")
            self._update_status_info()
            return

        try:
            # Extraer configuraciones
            self.postgres_config = config.get("postgres", {}).copy()
            self.email_config = config.get("email", {}).copy()
            self.search_params = config.get("search_params", {"titles": []}).copy()
            self.notify_users = config.get("notify_users", []).copy()

            # Crear conectores con las configuraciones
            if self.postgres_config:
                self._create_postgres_connector()
                self.add_log("Configuración PostgreSQL cargada", "SUCCESS")

            if self.email_config:
                self._create_email_connector()
                self.add_log("Configuración de correo cargada", "SUCCESS")

            # Informar sobre parámetros de búsqueda y usuarios
            if self.search_params.get("titles"):
                self.add_log(f"Parámetros de búsqueda cargados: {len(self.search_params['titles'])} título(s)", "SUCCESS")

            if self.notify_users:
                self.add_log(f"Usuarios a notificar cargados: {len(self.notify_users)} usuario(s)", "SUCCESS")

            # Actualizar estado
            self._update_status_info()

            logger.info(
                f"Configuración aplicada - PostgreSQL: {bool(self.postgres_config)}, Correo: {bool(self.email_config)}, "
                f"Parámetros: {len(self.search_params.get('titles', []))}, Usuarios: {len(self.notify_users)}")

        except Exception as e:
            error_msg = f"Error al aplicar configuración: {str(e)}"
            self.add_log(error_msg, "ERROR")
            logger.error(error_msg)


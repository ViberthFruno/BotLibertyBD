# tab_principal.py
"""
Pestaña Principal para la aplicación EnlaceDB.

Este módulo implementa la pestaña principal optimizada de la aplicación, combinando
la gestión de perfiles de automatización y la configuración de conexión PostgreSQL
y correo electrónico en una sola vista. La configuración se maneja completamente
a través de archivos JSON sin valores hardcodeados.
"""

import tkinter as tk
from tkinter import ttk, messagebox
from datetime import datetime

from logger import logger
from postgres_connector import PostgresConnector
from profile_manager import ProfileManager
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

        # Variables para la UI
        self.profiles_table = None
        self.log_text = None
        self.progress_bar = None
        self.edit_button = None
        self.delete_button = None
        self.execute_button = None
        self.selected_profile_index = None

        # Crear la estructura de la pestaña
        self._create_principal_tab()

        # Crear gestor de perfiles
        self.profile_manager = ProfileManager(
            parent_frame,
            self,
            self.get_connector
        )

        # Inicializar el gestor de perfiles
        self.profile_manager.initialize()

    def _create_principal_tab(self):
        """Crea los widgets de la pestaña principal."""
        # Frame principal
        main_frame = ttk.Frame(self.parent)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        # Frame superior para perfiles de automatización
        top_frame = ttk.LabelFrame(main_frame, text="Perfiles de Automatización")
        top_frame.pack(fill=tk.X, expand=False, padx=5, pady=5)

        # Frame inferior dividido en dos columnas
        bottom_frame = ttk.Frame(main_frame)
        bottom_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        # Columna izquierda: Configuración (SIMPLIFICADA)
        left_frame = ttk.LabelFrame(bottom_frame, text="Configuración")
        left_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(0, 5))

        # Columna derecha: Registro de actividad
        right_frame = ttk.LabelFrame(bottom_frame, text="Registro de Actividad")
        right_frame.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True, padx=(5, 0))

        # Crear componentes de cada sección
        self._create_profiles_section(top_frame)
        self._create_configuration_section(left_frame)
        self._create_activity_section(right_frame)

    def _create_profiles_section(self, parent):
        """Crea la sección de perfiles de automatización."""
        # Frame para tabla de perfiles
        table_frame = ttk.Frame(parent)
        table_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        # Crear tabla de perfiles con Treeview
        columns = ('nombre', 'carpeta', 'hora', 'estado')
        self.profiles_table = ttk.Treeview(table_frame, columns=columns, show='headings',
                                           selectmode='browse', height=6)

        # Configurar encabezados
        self.profiles_table.heading('nombre', text='Nombre')
        self.profiles_table.heading('carpeta', text='Bandeja')
        self.profiles_table.heading('hora', text='Hora')
        self.profiles_table.heading('estado', text='Estado')

        # Configurar columnas
        self.profiles_table.column('nombre', width=200, minwidth=150)
        self.profiles_table.column('carpeta', width=200, minwidth=150)
        self.profiles_table.column('hora', width=70, minwidth=70, anchor=tk.CENTER)
        self.profiles_table.column('estado', width=70, minwidth=70, anchor=tk.CENTER)

        # Scrollbar para la tabla
        scrollbar = ttk.Scrollbar(table_frame, orient="vertical", command=self.profiles_table.yview)
        self.profiles_table.configure(yscrollcommand=scrollbar.set)

        # Empaquetar tabla y scrollbar
        self.profiles_table.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        # Configurar evento de selección
        self.profiles_table.bind('<<TreeviewSelect>>', self._on_profile_selected)

        # Frame para botones
        buttons_frame = ttk.Frame(parent)
        buttons_frame.pack(fill=tk.X, padx=5, pady=5)

        # Botones de acción
        add_button = ttk.Button(
            buttons_frame,
            text="Añadir Perfil",
            command=self._on_add_profile
        )
        add_button.pack(side=tk.LEFT, padx=5)

        self.edit_button = ttk.Button(
            buttons_frame,
            text="Editar Perfil",
            command=self._on_edit_profile,
            state="disabled"
        )
        self.edit_button.pack(side=tk.LEFT, padx=5)

        self.delete_button = ttk.Button(
            buttons_frame,
            text="Eliminar Perfil",
            command=self._on_delete_profile,
            state="disabled"
        )
        self.delete_button.pack(side=tk.LEFT, padx=5)

        self.execute_button = ttk.Button(
            buttons_frame,
            text="Ejecutar Perfil",
            command=self._on_execute_profile,
            state="disabled"
        )
        self.execute_button.pack(side=tk.LEFT, padx=5)

        # Barra de progreso
        progress_frame = ttk.Frame(parent)
        progress_frame.pack(fill=tk.X, padx=5, pady=5)

        ttk.Label(progress_frame, text="Progreso:").pack(anchor=tk.W, pady=(0, 5))

        self.progress_bar = ttk.Progressbar(progress_frame, orient="horizontal", mode="determinate")
        self.progress_bar.pack(fill=tk.X)

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

    # ===============================
    # GESTIÓN DE PERFILES
    # ===============================

    def _on_profile_selected(self, event):
        """Maneja la selección de un perfil en la tabla."""
        selection = self.profiles_table.selection()
        if selection:
            item = selection[0]
            item_values = self.profiles_table.item(item, 'values')
            if len(item_values) > 4:  # El índice es el quinto valor (oculto)
                self.selected_profile_index = int(item_values[4])

                # Activar botones
                self.edit_button.configure(state="normal")
                self.delete_button.configure(state="normal")
                self.execute_button.configure(state="normal")

    def _on_add_profile(self):
        """Maneja el evento de añadir perfil."""
        self.profile_manager.add_profile()

    def _on_edit_profile(self):
        """Maneja el evento de editar perfil."""
        self.profile_manager.edit_profile()

    def _on_delete_profile(self):
        """Maneja el evento de eliminar perfil."""
        self.profile_manager.delete_profile()

    def _on_execute_profile(self):
        """Maneja el evento de ejecutar perfil manualmente."""
        self.profile_manager.execute_profile_manually()

    # ===============================
    # INTERFAZ PARA OTROS MÓDULOS
    # ===============================

    def refresh_profiles_table(self, profiles):
        """Actualiza la tabla de perfiles con los datos actuales."""
        # Limpiar tabla actual
        for item in self.profiles_table.get_children():
            self.profiles_table.delete(item)

        # Resetear selección
        self.selected_profile_index = None
        self.edit_button.configure(state="disabled")
        self.delete_button.configure(state="disabled")
        self.execute_button.configure(state="disabled")

        # Añadir perfiles a la tabla
        for i, profile in enumerate(profiles):
            name = profile.get("name", "Sin nombre")
            folder_path = profile.get("folder_path", "INBOX")
            folder_name = self._format_mailbox_name(folder_path)

            hour = profile.get("hour", 0)
            minute = profile.get("minute", 0)
            time_text = f"{hour:02d}:{minute:02d}"

            enabled = profile.get("enabled", False)
            status_text = "Activo" if enabled else "Inactivo"

            # Añadir a la tabla (con el índice como último valor)
            self.profiles_table.insert("", tk.END, values=(name, folder_name, time_text, status_text, i))

            # Aplicar colores según estado
            if enabled:
                item = self.profiles_table.get_children()[-1]
                self.profiles_table.item(item, tags=("enabled",))

        # Configurar tags para colores
        self.profiles_table.tag_configure("enabled", foreground="green")

    @staticmethod
    def _format_mailbox_name(folder_path):
        """Devuelve un nombre legible para la bandeja utilizada por el perfil."""
        normalized = (folder_path or "").strip()
        if not normalized:
            return "Bandeja de entrada"

        normalized = normalized.replace("\\", "/")
        if normalized.upper() == "INBOX":
            return "Bandeja de entrada"

        return normalized.split("/")[-1] or "Bandeja de entrada"

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

    def update_progress(self, progress):
        """Actualiza la barra de progreso."""
        if isinstance(progress, float) and 0 <= progress <= 1:
            self.progress_bar["value"] = progress * 100
        else:
            # Si progress es un valor entero entre 0 y 100
            self.progress_bar["value"] = progress

    def get_selected_profile_index(self):
        """Obtiene el índice del perfil seleccionado."""
        return self.selected_profile_index

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

            # Crear conectores con las configuraciones
            if self.postgres_config:
                self._create_postgres_connector()
                self.add_log("Configuración PostgreSQL cargada", "SUCCESS")

            if self.email_config:
                self._create_email_connector()
                self.add_log("Configuración de correo cargada", "SUCCESS")

            # Actualizar estado
            self._update_status_info()

            logger.info(
                f"Configuración aplicada - PostgreSQL: {bool(self.postgres_config)}, Correo: {bool(self.email_config)}")

        except Exception as e:
            error_msg = f"Error al aplicar configuración: {str(e)}"
            self.add_log(error_msg, "ERROR")
            logger.error(error_msg)

    def stop_profile_timer(self):
        """Detiene el temporizador de verificación de perfiles."""
        if hasattr(self.profile_manager, 'profile_timer') and self.profile_manager.profile_timer:
            self.parent.after_cancel(self.profile_manager.profile_timer)
            logger.info("Temporizador de perfiles detenido")

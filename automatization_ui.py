# automatization_ui.py
"""
Componente de interfaz de usuario para la automatización de EnlaceDB.

Este módulo se encarga de crear y gestionar todos los elementos de la interfaz
gráfica para la pestaña de subida automática, incluyendo la tabla de perfiles,
botones de acción, área de actividad y controles de progreso. Separa la lógica
de presentación de la lógica de negocio para mejor mantenibilidad del código.
Optimizado para evitar problemas de transparencia en ejecutables de PyInstaller.
Diseño optimizado para distribución horizontal.
"""

import tkinter as tk
import customtkinter as ctk
import datetime
from logger import logger


class AutomatizationUI:
    """Clase que maneja la interfaz de usuario para la automatización con diseño horizontal."""

    def __init__(self, parent_frame):
        """
        Inicializa la interfaz de usuario.

        Args:
            parent_frame: Frame contenedor donde se añadirán los componentes.
        """
        self.parent = parent_frame
        self.selected_profile_index = None

        # Referencias a widgets que necesitarán ser actualizados
        self.profiles_container = None
        self.activity_text = None
        self.progress_bar = None
        self.edit_button = None
        self.delete_button = None
        self.execute_button = None

        # Callbacks que serán establecidos por la clase principal
        self.add_profile_callback = None
        self.edit_profile_callback = None
        self.delete_profile_callback = None
        self.execute_profile_callback = None
        self.select_profile_callback = None

    def create_interface(self):
        """Crea todos los elementos de la interfaz de usuario con diseño horizontal."""
        # Frame principal con disposición horizontal
        main_container = ctk.CTkFrame(self.parent, fg_color="transparent")
        main_container.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        # Frame izquierdo para gestión de perfiles
        left_frame = ctk.CTkFrame(main_container, fg_color="#2b2b2b", corner_radius=10)
        left_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(0, 10))

        # Frame derecho para actividad y procesamiento
        right_frame = ctk.CTkFrame(main_container, fg_color="#2b2b2b", corner_radius=10)
        right_frame.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True)

        # Crear contenido del frame izquierdo
        self._create_profiles_section(left_frame)

        # Crear contenido del frame derecho
        self._create_activity_section(right_frame)

        return main_container

    def _create_profiles_section(self, parent):
        """Crea la sección de gestión de perfiles."""
        # Título de la sección
        title_label = ctk.CTkLabel(parent, text="🔄 Automatización de Carga",
                                   font=ctk.CTkFont(size=18, weight="bold"))
        title_label.pack(pady=(15, 15))

        # Línea separadora
        separator = ctk.CTkFrame(parent, height=2, fg_color="#3a7ebf")
        separator.pack(fill=tk.X, padx=20, pady=(0, 15))

        # Crear tabla de perfiles
        self._create_profiles_table(parent)

        # Crear botones de acción
        self._create_action_buttons(parent)

        # Crear panel de ayuda
        self._create_help_panel(parent)

    def _create_profiles_table(self, parent):
        """Crea la tabla de perfiles optimizada para diseño horizontal."""
        # Frame principal de la tabla
        table_frame = ctk.CTkFrame(parent, corner_radius=10,
                                   border_width=1, border_color="#4a8ec8",
                                   fg_color="#1a1a1a")
        table_frame.pack(padx=15, pady=(0, 15), fill=tk.BOTH, expand=True)

        # Título de la tabla
        table_title_frame = ctk.CTkFrame(table_frame, fg_color="#2a6eaf", corner_radius=8)
        table_title_frame.pack(padx=8, pady=(8, 0), fill=tk.X)

        table_title = ctk.CTkLabel(table_title_frame, text="📋 Perfiles de Automatización",
                                   font=ctk.CTkFont(size=14, weight="bold"),
                                   text_color="white")
        table_title.pack(pady=6)

        # Frame para encabezados más compactos
        headers_frame = ctk.CTkFrame(table_frame, fg_color="#2a6eaf", corner_radius=6)
        headers_frame.pack(padx=8, pady=(4, 0), fill=tk.X)

        # Configurar grid para columnas ajustadas al espacio horizontal
        headers_frame.grid_columnconfigure(0, weight=3)  # Nombre
        headers_frame.grid_columnconfigure(1, weight=2)  # Bandeja
        headers_frame.grid_columnconfigure(2, weight=1)  # Hora
        headers_frame.grid_columnconfigure(3, weight=1)  # Estado

        # Encabezados más compactos
        headers = [
            ("👤 Nombre", 0),
            ("📁 Bandeja", 1),
            ("🕐 Hora", 2),
            ("🔧 Estado", 3)
        ]

        for header_text, col in headers:
            header_label = ctk.CTkLabel(headers_frame, text=header_text,
                                        font=ctk.CTkFont(size=11, weight="bold"),
                                        text_color="white")
            header_label.grid(row=0, column=col, padx=4, pady=6, sticky="ew")

        # Frame contenedor para la lista de perfiles (más compacto)
        self.profiles_container = ctk.CTkScrollableFrame(table_frame, height=200,
                                                         fg_color="#2b2b2b",
                                                         corner_radius=6)
        self.profiles_container.pack(padx=8, pady=(4, 8), fill=tk.BOTH, expand=True)

        # Configurar el grid del contenedor de perfiles
        for i in range(4):  # 4 columnas
            self.profiles_container.grid_columnconfigure(i, weight=[3, 2, 1, 1][i])

    def _create_action_buttons(self, parent):
        """Crea los botones de acción más compactos."""
        # Frame para botones
        buttons_frame = ctk.CTkFrame(parent, fg_color="transparent")
        buttons_frame.pack(padx=15, pady=(0, 15), fill=tk.X)

        # Configurar grid para 4 botones en 2 filas
        buttons_frame.grid_columnconfigure(0, weight=1)
        buttons_frame.grid_columnconfigure(1, weight=1)

        # Primera fila de botones
        add_button = ctk.CTkButton(
            buttons_frame,
            text="➕ Añadir",
            command=self._on_add_profile,
            fg_color="#4CAF50",
            hover_color="#45a049",
            height=35,
            corner_radius=8,
            font=ctk.CTkFont(size=12, weight="bold")
        )
        add_button.grid(row=0, column=0, padx=(0, 4), pady=(0, 4), sticky="ew")

        self.edit_button = ctk.CTkButton(
            buttons_frame,
            text="✏️ Editar",
            command=self._on_edit_profile,
            fg_color="#FF9800",
            hover_color="#F57C00",
            height=35,
            corner_radius=8,
            state="disabled",
            font=ctk.CTkFont(size=12, weight="bold")
        )
        self.edit_button.grid(row=0, column=1, padx=(4, 0), pady=(0, 4), sticky="ew")

        # Segunda fila de botones
        self.delete_button = ctk.CTkButton(
            buttons_frame,
            text="🗑️ Eliminar",
            command=self._on_delete_profile,
            fg_color="#F44336",
            hover_color="#D32F2F",
            height=35,
            corner_radius=8,
            state="disabled",
            font=ctk.CTkFont(size=12, weight="bold")
        )
        self.delete_button.grid(row=1, column=0, padx=(0, 4), sticky="ew")

        self.execute_button = ctk.CTkButton(
            buttons_frame,
            text="▶️ Ejecutar",
            command=self._on_execute_profile,
            fg_color="#2196F3",
            hover_color="#1976D2",
            height=35,
            corner_radius=8,
            state="disabled",
            font=ctk.CTkFont(size=12, weight="bold")
        )
        self.execute_button.grid(row=1, column=1, padx=(4, 0), sticky="ew")

    def _create_help_panel(self, parent):
        """Crea el panel de ayuda más compacto."""
        help_frame = ctk.CTkFrame(parent, corner_radius=8, fg_color="#262626",
                                  border_width=1, border_color="#3a7ebf")
        help_frame.pack(padx=15, pady=(0, 15), fill=tk.X)

        help_content = ctk.CTkFrame(help_frame, fg_color="transparent")
        help_content.pack(padx=10, pady=8, fill=tk.X)

        help_icon = ctk.CTkLabel(help_content, text="💡", font=ctk.CTkFont(size=14))
        help_icon.pack(side=tk.LEFT, padx=(0, 8))

        help_text = ("Los perfiles automatizan la búsqueda de correos electrónicos, "
                     "descargan archivos Excel adjuntos y procesan los datos automáticamente.")
        help_label = ctk.CTkLabel(help_content, text=help_text, wraplength=350,
                                  justify="left", font=ctk.CTkFont(size=10),
                                  text_color="#CCCCCC")
        help_label.pack(side=tk.LEFT, fill=tk.X, expand=True)

    def _create_activity_section(self, parent):
        """Crea la sección de actividad y procesamiento."""
        # Título
        title_label = ctk.CTkLabel(parent, text="📊 Actividad y Procesamiento",
                                   font=ctk.CTkFont(size=18, weight="bold"))
        title_label.pack(pady=(15, 15))

        # Línea separadora
        separator = ctk.CTkFrame(parent, height=2, fg_color="#3a7ebf")
        separator.pack(fill=tk.X, padx=20, pady=(0, 15))

        # Frame para el área de texto de actividad
        activity_frame = ctk.CTkFrame(parent, corner_radius=10,
                                      border_width=1, border_color="#4a8ec8",
                                      fg_color="#1a1a1a")
        activity_frame.pack(padx=15, pady=(0, 15), fill=tk.BOTH, expand=True)

        # Header del área de actividad
        activity_header = ctk.CTkFrame(activity_frame, fg_color="#2a6eaf",
                                       corner_radius=8, height=35)
        activity_header.pack(padx=8, pady=(8, 0), fill=tk.X)
        activity_header.pack_propagate(False)

        header_label = ctk.CTkLabel(
            activity_header,
            text="📝 Registro de Actividad",
            font=ctk.CTkFont(size=14, weight="bold"),
            text_color="white"
        )
        header_label.pack(pady=6)

        # Área de texto para actividad
        self.activity_text = ctk.CTkTextbox(activity_frame, wrap=tk.WORD,
                                            corner_radius=6, border_width=1,
                                            border_color="#3a7ebf",
                                            fg_color="#2b2b2b")
        self.activity_text.pack(padx=10, pady=(6, 10), fill=tk.BOTH, expand=True)
        self.activity_text.configure(state=tk.DISABLED)

        # Frame para barra de progreso
        progress_frame = ctk.CTkFrame(parent, fg_color="transparent")
        progress_frame.pack(padx=15, pady=(0, 15), fill=tk.X)

        progress_label = ctk.CTkLabel(progress_frame, text="⏳ Progreso:",
                                      font=ctk.CTkFont(size=12, weight="bold"))
        progress_label.pack(anchor=tk.W, pady=(0, 5))

        # Barra de progreso
        self.progress_bar = ctk.CTkProgressBar(progress_frame, height=15,
                                               corner_radius=8, border_width=1,
                                               border_color="#3a7ebf",
                                               fg_color="#2b2b2b",
                                               progress_color="#4CAF50")
        self.progress_bar.pack(fill=tk.X)
        self.progress_bar.set(0)

    def set_callbacks(self, callbacks):
        """Establece los callbacks para los eventos de la interfaz."""
        self.add_profile_callback = callbacks.get('add_profile')
        self.edit_profile_callback = callbacks.get('edit_profile')
        self.delete_profile_callback = callbacks.get('delete_profile')
        self.execute_profile_callback = callbacks.get('execute_profile')
        self.select_profile_callback = callbacks.get('select_profile')

    def _on_add_profile(self):
        """Maneja el evento de añadir perfil."""
        if self.add_profile_callback:
            self.add_profile_callback()

    def _on_edit_profile(self):
        """Maneja el evento de editar perfil."""
        if self.edit_profile_callback:
            self.edit_profile_callback()

    def _on_delete_profile(self):
        """Maneja el evento de eliminar perfil."""
        if self.delete_profile_callback:
            self.delete_profile_callback()

    def _on_execute_profile(self):
        """Maneja el evento de ejecutar perfil manualmente."""
        if self.execute_profile_callback:
            self.execute_profile_callback()

    def refresh_profiles_table(self, profiles):
        """Actualiza la tabla de perfiles optimizada para el diseño horizontal."""
        # Limpiar tabla actual
        for widget in self.profiles_container.winfo_children():
            widget.destroy()

        # Resetear selección
        self.selected_profile_index = None
        self.edit_button.configure(state="disabled")
        self.delete_button.configure(state="disabled")
        self.execute_button.configure(state="disabled")

        # Si no hay perfiles, mostrar mensaje
        if not profiles:
            empty_frame = ctk.CTkFrame(self.profiles_container, fg_color="#333333",
                                       corner_radius=8, border_width=1,
                                       border_color="#4a8ec8")
            empty_frame.pack(fill=tk.X, pady=15, padx=10)

            empty_icon = ctk.CTkLabel(empty_frame, text="📋", font=ctk.CTkFont(size=24))
            empty_icon.pack(pady=(15, 8))

            empty_label = ctk.CTkLabel(empty_frame,
                                       text="No hay perfiles configurados",
                                       font=ctk.CTkFont(size=14, weight="bold"))
            empty_label.pack()

            empty_desc = ctk.CTkLabel(empty_frame,
                                      text="Haga clic en 'Añadir' para comenzar",
                                      font=ctk.CTkFont(size=11),
                                      text_color="#BBBBBB")
            empty_desc.pack(pady=(5, 15))
            return

        # Añadir cada perfil con diseño compacto horizontal
        for i, profile in enumerate(profiles):
            # Frame para cada fila más compacto
            row_frame = ctk.CTkFrame(self.profiles_container,
                                     corner_radius=6,
                                     fg_color="#383838" if i % 2 == 0 else "#323232",
                                     border_width=1,
                                     border_color="#2b2b2b")
            row_frame.pack(fill=tk.X, pady=1, padx=3)
            row_frame.rowid = i

            # Configurar grid para 4 columnas
            for col in range(4):
                row_frame.grid_columnconfigure(col, weight=[3, 2, 1, 1][col])

            # Columna 1: Nombre del perfil
            name_text = profile.get("name", "Sin nombre")
            if len(name_text) > 20:
                name_text = name_text[:17] + "..."
            name_label = ctk.CTkLabel(row_frame, text=name_text,
                                      font=ctk.CTkFont(size=11, weight="bold"),
                                      anchor="w")
            name_label.grid(row=0, column=0, padx=6, pady=6, sticky="ew")

            # Columna 2: Bandeja (más compacta)
            folder_path = profile.get("folder_path", "INBOX")
            folder_name = self._format_mailbox_name(folder_path)
            if len(folder_name) > 15:
                folder_name = folder_name[:12] + "..."

            folder_label = ctk.CTkLabel(row_frame, text=folder_name,
                                        font=ctk.CTkFont(size=10),
                                        anchor="w", text_color="#CCCCCC")
            folder_label.grid(row=0, column=1, padx=4, pady=6, sticky="ew")

            # Columna 3: Hora
            hour = profile.get("hour", 0)
            minute = profile.get("minute", 0)
            time_text = f"{hour:02d}:{minute:02d}"

            time_label = ctk.CTkLabel(row_frame, text=time_text,
                                      font=ctk.CTkFont(size=11, weight="bold"),
                                      text_color="#81C784")
            time_label.grid(row=0, column=2, padx=4, pady=6, sticky="ew")

            # Columna 4: Estado
            enabled = profile.get("enabled", False)
            if enabled:
                status_text = "🟢"
                status_color = "#4CAF50"
            else:
                status_text = "🔴"
                status_color = "#F44336"

            status_label = ctk.CTkLabel(row_frame, text=status_text,
                                        font=ctk.CTkFont(size=14),
                                        text_color=status_color)
            status_label.grid(row=0, column=3, padx=4, pady=6, sticky="ew")

            # Configurar eventos de clic
            self._configure_row_selection(row_frame, i)

    @staticmethod
    def _format_mailbox_name(folder_path):
        """Devuelve un nombre legible para la bandeja configurada."""
        normalized = (folder_path or "").strip()
        if not normalized:
            return "Bandeja de entrada"

        normalized = normalized.replace("\\", "/")
        if normalized.upper() == "INBOX":
            return "Bandeja de entrada"

        return normalized.split("/")[-1] or "Bandeja de entrada"

    def _configure_row_selection(self, row_frame, index):
        """Configura los eventos de selección para una fila."""
        def on_click(event):
            self.select_profile(index)

        # Aplicar evento a la fila y todos sus hijos
        row_frame.bind("<Button-1>", on_click)
        for widget in row_frame.winfo_children():
            widget.bind("<Button-1>", on_click)

    def select_profile(self, index):
        """Selecciona un perfil de la tabla."""
        # Deseleccionar el perfil anterior
        if self.selected_profile_index is not None:
            for widget in self.profiles_container.winfo_children():
                if (hasattr(widget, 'rowid') and
                        widget.rowid == self.selected_profile_index):
                    # Restaurar color original
                    original_color = "#383838" if widget.rowid % 2 == 0 else "#323232"
                    widget.configure(fg_color=original_color, border_color="#2b2b2b")
                    break

        # Seleccionar nuevo perfil
        self.selected_profile_index = index

        # Resaltar la fila seleccionada
        for widget in self.profiles_container.winfo_children():
            if hasattr(widget, 'rowid') and widget.rowid == index:
                widget.configure(fg_color="#1E4D78", border_color="#4a8ec8")
                break

        # Habilitar botones de acción
        self.edit_button.configure(state="normal")
        self.delete_button.configure(state="normal")
        self.execute_button.configure(state="normal")

        # Notificar al callback
        if self.select_profile_callback:
            self.select_profile_callback(index)

    def update_status(self, message):
        """Actualiza el mensaje de estado."""
        logger.info(f"Estado: {message}")

    def add_log(self, message, level="INFO"):
        """Añade un mensaje al área de actividad."""
        # Colores seguros según el nivel
        colors = {
            "INFO": "#E3F2FD",
            "SUCCESS": "#4CAF50",
            "WARNING": "#FF9800",
            "ERROR": "#F44336"
        }

        color = colors.get(level, "#E3F2FD")

        # Habilitar edición
        self.activity_text.configure(state=tk.NORMAL)

        # Agregar timestamp
        timestamp = datetime.datetime.now().strftime("%H:%M:%S")

        # Insertar el mensaje con iconos
        level_icons = {
            "INFO": "ℹ️",
            "SUCCESS": "✅",
            "WARNING": "⚠️",
            "ERROR": "❌"
        }

        icon = level_icons.get(level, "ℹ️")
        self.activity_text.insert(tk.END, f"{timestamp} {icon} ", "timestamp")
        self.activity_text.insert(tk.END, f"{message}\n", (level,))

        # Configurar colores
        self.activity_text.tag_config("timestamp", foreground="#9E9E9E")
        self.activity_text.tag_config(level, foreground=color)

        # Desplazar al final
        self.activity_text.see(tk.END)

        # Deshabilitar edición
        self.activity_text.configure(state=tk.DISABLED)

        # Registrar en logger
        if level == "INFO":
            logger.info(message)
        elif level == "SUCCESS":
            logger.info(f"SUCCESS: {message}")
        elif level == "WARNING":
            logger.warning(message)
        elif level == "ERROR":
            logger.error(message)

    def add_result(self, text):
        """Añade un resultado al área de actividad."""
        # Habilitar edición
        self.activity_text.configure(state=tk.NORMAL)

        # Insertar separador
        self.activity_text.insert(tk.END, "📊 " + "─" * 40 + "\n", "result_header")
        self.activity_text.tag_config("result_header", foreground="#4a8ec8")

        # Insertar el texto del resultado
        self.activity_text.insert(tk.END, f"{text}\n", "result")
        self.activity_text.tag_config("result", foreground="#E8F5E8")

        # Insertar separador final
        self.activity_text.insert(tk.END, "─" * 48 + "\n\n", "result_footer")
        self.activity_text.tag_config("result_footer", foreground="#4a8ec8")

        # Desplazar al final
        self.activity_text.see(tk.END)

        # Deshabilitar edición
        self.activity_text.configure(state=tk.DISABLED)

    def clear_activity(self):
        """Limpia el área de actividad."""
        self.activity_text.configure(state=tk.NORMAL)
        self.activity_text.delete(1.0, tk.END)
        self.activity_text.configure(state=tk.DISABLED)

    def update_progress(self, progress):
        """Actualiza la barra de progreso."""
        self.progress_bar.set(progress)

    def get_selected_profile_index(self):
        """Obtiene el índice del perfil seleccionado."""
        return self.selected_profile_index

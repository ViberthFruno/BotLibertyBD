# carga_manual_dialog.py
"""
Diálogo para carga manual de archivos Excel con control de notificaciones.

Permite al usuario cargar manualmente un archivo Excel y procesar los IMEIs
sin depender del sistema de correo automático. Incluye opción para controlar
si se envían correos de notificación a los usuarios configurados.
"""

import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import os
import threading
from logger import logger


class CargaManualDialog(tk.Toplevel):
    """Diálogo para carga manual de archivos Excel."""

    def __init__(self, parent, email_connector=None, postgres_connector=None,
                 notify_users=None, schema="automatizacion", table="datos_excel_doforms"):
        """
        Inicializa el diálogo de carga manual.

        Args:
            parent: Widget padre.
            email_connector: Conector de correo para procesamiento.
            postgres_connector: Conector PostgreSQL para sincronización.
            notify_users: Lista de emails a notificar.
            schema: Esquema de la base de datos.
            table: Tabla de la base de datos.
        """
        super().__init__(parent)
        self.parent = parent
        self.email_connector = email_connector
        self.postgres_connector = postgres_connector
        self.notify_users = notify_users or []
        self.schema = schema
        self.table = table

        self.selected_file = None
        self.processing_thread = None

        self._setup_window()
        self._create_widgets()

    def _setup_window(self):
        """Configura las propiedades básicas de la ventana."""
        self.title("Carga Manual de Excel")
        self.geometry("600x700")
        self.grab_set()  # Modal
        self.resizable(True, True)

        # Centrar en pantalla
        self.update_idletasks()
        width = self.winfo_width()
        height = self.winfo_height()
        x = (self.winfo_screenwidth() // 2) - (width // 2)
        y = (self.winfo_screenheight() // 2) - (height // 2)
        self.geometry(f'+{x}+{y}')

    def _create_widgets(self):
        """Crea todos los widgets del diálogo."""
        main_frame = ttk.Frame(self, padding=20)
        main_frame.pack(fill=tk.BOTH, expand=True)

        # Título
        title_label = ttk.Label(
            main_frame,
            text="Carga Manual de Excel",
            font=("Arial", 16, "bold")
        )
        title_label.pack(pady=(0, 20))

        # Sección de selección de archivo
        self._create_file_section(main_frame)

        # Sección de opciones
        self._create_options_section(main_frame)

        # Sección de estado
        self._create_status_section(main_frame)

        # Botones de acción
        self._create_action_buttons(main_frame)

    def _create_file_section(self, parent):
        """Crea la sección de selección de archivo."""
        file_frame = ttk.LabelFrame(parent, text="Archivo Excel", padding=15)
        file_frame.pack(fill=tk.X, pady=(0, 15))

        # Instrucciones
        instructions = ttk.Label(
            file_frame,
            text="Seleccione el archivo Excel que desea procesar manualmente:",
            wraplength=520
        )
        instructions.pack(pady=(0, 10))

        # Frame para archivo seleccionado
        selected_frame = ttk.Frame(file_frame)
        selected_frame.pack(fill=tk.X, pady=(0, 10))

        self.file_label = ttk.Label(
            selected_frame,
            text="Ningún archivo seleccionado",
            foreground="gray",
            wraplength=400
        )
        self.file_label.pack(side=tk.LEFT, fill=tk.X, expand=True)

        # Botón para seleccionar archivo
        select_button = ttk.Button(
            file_frame,
            text="📁 Seleccionar Archivo Excel",
            command=self._select_file
        )
        select_button.pack()

    def _create_options_section(self, parent):
        """Crea la sección de opciones de procesamiento."""
        options_frame = ttk.LabelFrame(parent, text="Opciones de Procesamiento", padding=15)
        options_frame.pack(fill=tk.X, pady=(0, 15))

        # Checkbox para envío de correos
        self.send_emails_var = tk.BooleanVar(value=True)
        send_emails_check = ttk.Checkbutton(
            options_frame,
            text="Enviar correos de confirmación a usuarios configurados",
            variable=self.send_emails_var
        )
        send_emails_check.pack(anchor=tk.W, pady=5)

        # Información adicional
        info_label = ttk.Label(
            options_frame,
            text="💡 Desactive esta opción si solo desea hacer pruebas sin enviar notificaciones.",
            foreground="blue",
            wraplength=520,
            font=("Arial", 9)
        )
        info_label.pack(anchor=tk.W, pady=(5, 0))

        # Mostrar usuarios configurados
        if self.notify_users:
            users_info = ttk.Label(
                options_frame,
                text=f"📧 Usuarios configurados: {len(self.notify_users)}",
                foreground="green",
                font=("Arial", 9)
            )
            users_info.pack(anchor=tk.W, pady=(5, 0))
        else:
            users_warning = ttk.Label(
                options_frame,
                text="⚠ No hay usuarios configurados para notificar",
                foreground="orange",
                font=("Arial", 9)
            )
            users_warning.pack(anchor=tk.W, pady=(5, 0))

    def _create_status_section(self, parent):
        """Crea la sección de estado del procesamiento."""
        status_frame = ttk.LabelFrame(parent, text="Estado del Procesamiento", padding=15)
        status_frame.pack(fill=tk.BOTH, expand=True, pady=(0, 15))

        # Área de texto para mostrar el progreso
        text_container = ttk.Frame(status_frame)
        text_container.pack(fill=tk.BOTH, expand=True)

        scrollbar = ttk.Scrollbar(text_container)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        self.status_text = tk.Text(
            text_container,
            wrap=tk.WORD,
            height=8,
            state=tk.DISABLED,
            yscrollcommand=scrollbar.set
        )
        self.status_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.config(command=self.status_text.yview)

        # Etiqueta de estado
        self.status_label = ttk.Label(
            status_frame,
            text="Estado: Esperando archivo...",
            foreground="gray",
            font=("Arial", 10, "bold")
        )
        self.status_label.pack(pady=(10, 0))

    def _create_action_buttons(self, parent):
        """Crea los botones de acción."""
        button_frame = ttk.Frame(parent)
        button_frame.pack(fill=tk.X)

        # Botón Cancelar
        self.cancel_button = ttk.Button(
            button_frame,
            text="Cancelar",
            command=self._cancel
        )
        self.cancel_button.pack(side=tk.RIGHT, padx=5)

        # Botón Procesar
        self.process_button = ttk.Button(
            button_frame,
            text="🚀 Procesar Excel",
            command=self._process_file,
            state=tk.DISABLED
        )
        self.process_button.pack(side=tk.RIGHT, padx=5)

    def _select_file(self):
        """Abre el diálogo de selección de archivo."""
        file_path = filedialog.askopenfilename(
            title="Seleccionar archivo Excel",
            filetypes=[
                ("Archivos Excel", "*.xlsx *.xls"),
                ("Todos los archivos", "*.*")
            ]
        )

        if file_path:
            self.selected_file = file_path
            filename = os.path.basename(file_path)
            self.file_label.configure(text=filename, foreground="black")
            self.process_button.configure(state=tk.NORMAL)
            self.status_label.configure(
                text=f"Estado: Archivo seleccionado - Listo para procesar",
                foreground="green"
            )
            self._add_status_message(f"✓ Archivo seleccionado: {filename}", "INFO")
            logger.info(f"Archivo seleccionado para carga manual: {file_path}")

    def _add_status_message(self, message, level="INFO"):
        """Añade un mensaje al área de estado."""
        colors = {
            "INFO": "black",
            "SUCCESS": "dark green",
            "WARNING": "orange",
            "ERROR": "red"
        }

        color = colors.get(level, "black")

        self.status_text.configure(state=tk.NORMAL)
        self.status_text.insert(tk.END, f"{message}\n", level)
        self.status_text.tag_config(level, foreground=color)
        self.status_text.see(tk.END)
        self.status_text.configure(state=tk.DISABLED)

    def _process_file(self):
        """Procesa el archivo Excel seleccionado."""
        if not self.selected_file:
            messagebox.showwarning("Sin Archivo", "Por favor, seleccione un archivo Excel primero.")
            return

        if not os.path.exists(self.selected_file):
            messagebox.showerror("Archivo No Encontrado", "El archivo seleccionado no existe.")
            return

        if not self.email_connector:
            messagebox.showerror(
                "Sin Conector",
                "No hay conector de correo disponible. Verifique la configuración."
            )
            return

        # Confirmar si no se van a enviar correos
        send_notifications = self.send_emails_var.get()
        if not send_notifications:
            confirm = messagebox.askyesno(
                "Confirmar Procesamiento",
                "Va a procesar el archivo SIN enviar correos de notificación.\n\n"
                "¿Desea continuar?"
            )
            if not confirm:
                return

        # Deshabilitar botones durante el procesamiento
        self.process_button.configure(state=tk.DISABLED)
        self.cancel_button.configure(state=tk.DISABLED)

        self.status_label.configure(text="Estado: Procesando...", foreground="orange")
        self._add_status_message("="*50, "INFO")
        self._add_status_message("Iniciando procesamiento manual...", "INFO")

        # Procesar en un hilo separado para no bloquear la UI
        self.processing_thread = threading.Thread(
            target=self._execute_processing,
            args=(send_notifications,),
            daemon=True
        )
        self.processing_thread.start()

    def _execute_processing(self, send_notifications):
        """
        Ejecuta el procesamiento del archivo Excel en un hilo separado.

        Args:
            send_notifications: Si se deben enviar correos de notificación.
        """
        try:
            # Callback para actualizar el estado en el hilo principal
            def status_callback(msg, level="INFO"):
                self.after(0, lambda: self._add_status_message(msg, level))

            # Procesar el Excel usando el método del conector
            result = self.email_connector.process_manual_excel(
                excel_path=self.selected_file,
                notify_emails=self.notify_users if send_notifications else [],
                status_callback=status_callback,
                postgres_connector=self.postgres_connector,
                schema=self.schema,
                table=self.table,
                send_notifications=send_notifications
            )

            # Actualizar UI en el hilo principal
            self.after(0, lambda: self._show_processing_result(result))

        except Exception as e:
            error_msg = f"Error durante el procesamiento: {str(e)}"
            self.after(0, lambda: self._show_error(error_msg))
            logger.error(error_msg)

    def _show_processing_result(self, result):
        """Muestra el resultado del procesamiento."""
        if result.get("success"):
            self.status_label.configure(
                text="Estado: ✓ Procesamiento completado",
                foreground="green"
            )

            self._add_status_message("="*50, "SUCCESS")
            self._add_status_message("✓ PROCESAMIENTO COMPLETADO", "SUCCESS")
            self._add_status_message(f"IMEIs procesados: {result.get('total_imeis', 0)}", "SUCCESS")

            if result.get('sync_result'):
                sync = result['sync_result']
                self._add_status_message(f"Nuevos: {sync.get('nuevos', 0)}", "SUCCESS")
                self._add_status_message(f"Actualizados: {sync.get('actualizados', 0)}", "SUCCESS")
                self._add_status_message(f"Desactivados: {sync.get('desactivados', 0)}", "SUCCESS")

            if result.get('notified_users', 0) > 0:
                self._add_status_message(
                    f"Notificaciones enviadas: {result['notified_users']}",
                    "SUCCESS"
                )
            else:
                self._add_status_message("No se enviaron notificaciones", "INFO")

            messagebox.showinfo(
                "Procesamiento Completado",
                f"✓ El archivo se procesó exitosamente.\n\n"
                f"IMEIs procesados: {result.get('total_imeis', 0)}\n"
                f"Notificaciones enviadas: {result.get('notified_users', 0)}"
            )
        else:
            self.status_label.configure(
                text="Estado: ✗ Error en procesamiento",
                foreground="red"
            )
            self._add_status_message(f"✗ Error: {result.get('message', 'Error desconocido')}", "ERROR")

            messagebox.showerror(
                "Error de Procesamiento",
                f"Ocurrió un error:\n\n{result.get('message', 'Error desconocido')}"
            )

        # Rehabilitar botones
        self.process_button.configure(state=tk.NORMAL)
        self.cancel_button.configure(state=tk.NORMAL)

    def _show_error(self, error_msg):
        """Muestra un error durante el procesamiento."""
        self.status_label.configure(text="Estado: ✗ Error", foreground="red")
        self._add_status_message(f"✗ {error_msg}", "ERROR")

        messagebox.showerror("Error", error_msg)

        # Rehabilitar botones
        self.process_button.configure(state=tk.NORMAL)
        self.cancel_button.configure(state=tk.NORMAL)

    def _cancel(self):
        """Cancela y cierra el diálogo."""
        # Verificar si hay un procesamiento en curso
        if self.processing_thread and self.processing_thread.is_alive():
            confirm = messagebox.askyesno(
                "Procesamiento en Curso",
                "Hay un procesamiento en curso.\n\n"
                "¿Está seguro de que desea cerrar?\n"
                "(El procesamiento continuará en segundo plano)"
            )
            if not confirm:
                return

        self.destroy()

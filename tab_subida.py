# tab_subida.py
"""
Componente de la pestaña de subida manual de archivos Excel para EnlaceDB.

Este archivo implementa la interfaz de usuario para la carga y procesamiento de archivos
Excel, extrayendo múltiples campos y cargándolos a la base de datos PostgreSQL en el
esquema y tabla configurados por el usuario. Versión simplificada usando Tkinter estándar.
"""

import os
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import threading
from logger import logger

# Importar los módulos modularizados para procesamiento de Excel y carga a la BD
from excel_processor import ExcelProcessor
from db_uploader import DBUploader

# Intentar importar pandas para verificar disponibilidad
try:
    import pandas as pd

    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False
    logger.warning("Módulo pandas no disponible. No se podrán procesar archivos Excel.")


class SubidaManualTab:
    """Clase que implementa la pestaña de subida manual de archivos Excel."""

    def __init__(self, parent_frame, get_connector_callback):
        """
        Inicializa el componente de la pestaña de subida manual.

        Args:
            parent_frame: Frame contenedor donde se añadirán los componentes.
            get_connector_callback: Función para obtener el conector de base de datos.
        """
        self.parent = parent_frame
        self.get_connector_callback = get_connector_callback
        self.excel_file_path = None
        self.is_uploading = False
        self.upload_thread = None

        # Inicializar procesador de Excel con la configuración predeterminada
        self.excel_processor = ExcelProcessor()

        # Crear la estructura de la pestaña
        self._create_subida_tab()

    def _create_subida_tab(self):
        """Crea los widgets de la pestaña de subida manual."""
        # Frame principal
        main_frame = ttk.Frame(self.parent)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        # Frame izquierdo para selección y configuración
        left_frame = ttk.LabelFrame(main_frame, text="Configuración")
        left_frame.pack(side=tk.LEFT, fill=tk.BOTH, padx=(0, 5), pady=5, expand=True)

        # Frame derecho para previsualización
        right_frame = ttk.LabelFrame(main_frame, text="Previsualización y Detalles")
        right_frame.pack(side=tk.RIGHT, fill=tk.BOTH, padx=(5, 0), pady=5, expand=True)

        # Crear secciones
        self._create_configuration_section(left_frame)
        self._create_preview_section(right_frame)

    def _create_configuration_section(self, parent):
        """Crea la sección de configuración y selección de archivo."""
        # Sección de selección de archivo
        file_frame = ttk.LabelFrame(parent, text="Seleccionar Archivo")
        file_frame.pack(fill=tk.X, padx=10, pady=10)

        # Mostrar archivo seleccionado
        self.file_path_var = tk.StringVar(value="Ningún archivo seleccionado")
        file_path_label = ttk.Label(file_frame, textvariable=self.file_path_var, wraplength=200)
        file_path_label.pack(pady=5, padx=5)

        # Botón de selección
        select_file_button = ttk.Button(
            file_frame,
            text="Seleccionar Excel",
            command=self.select_excel_file
        )
        select_file_button.pack(pady=5, padx=5, fill=tk.X)

        # Sección de destino
        dest_frame = ttk.LabelFrame(parent, text="Destino")
        dest_frame.pack(fill=tk.X, padx=10, pady=10)

        self.dest_info_label = ttk.Label(
            dest_frame,
            text="Configure en pestaña Conexión",
            foreground="orange"
        )
        self.dest_info_label.pack(pady=5, padx=5)

        # Sección de estadísticas
        stats_frame = ttk.LabelFrame(parent, text="Estadísticas")
        stats_frame.pack(fill=tk.X, padx=10, pady=10)

        # Estadísticas básicas
        self.total_fields_label = ttk.Label(stats_frame, text="Total de campos: 0")
        self.total_fields_label.pack(anchor=tk.W, padx=5, pady=2)

        self.found_count_label = ttk.Label(stats_frame, text="Encontrados: 0", foreground="green")
        self.found_count_label.pack(anchor=tk.W, padx=5, pady=2)

        self.missing_count_label = ttk.Label(stats_frame, text="No encontrados: 0", foreground="orange")
        self.missing_count_label.pack(anchor=tk.W, padx=5, pady=2)

        self.records_label = ttk.Label(stats_frame, text="Registros: 0", foreground="green")
        self.records_label.pack(anchor=tk.W, padx=5, pady=2)

        # Botón de carga
        self.upload_button = ttk.Button(
            parent,
            text="Cargar a Base de Datos",
            command=self.upload_to_database,
            state="disabled"
        )
        self.upload_button.pack(pady=10, padx=10, fill=tk.X)

        # Estado y progreso
        status_frame = ttk.Frame(parent)
        status_frame.pack(fill=tk.X, padx=10, pady=5)

        self.status_label = ttk.Label(
            status_frame,
            text="Esperando archivo..."
        )
        self.status_label.pack(side=tk.LEFT)

        # Barra de progreso
        self.progress_bar = ttk.Progressbar(parent, orient="horizontal", mode="determinate")
        self.progress_bar.pack(pady=5, padx=10, fill=tk.X)

        # Verificar pandas
        if not PANDAS_AVAILABLE:
            self.status_label.configure(
                text="Error: pandas no instalado",
                foreground="red"
            )

        # Actualizar información de destino
        self._update_destination_info()

    def _create_preview_section(self, parent):
        """Crea la sección de previsualización y detalles."""
        # Crear notebook para pestañas de previsualización
        self.preview_notebook = ttk.Notebook(parent)
        self.preview_notebook.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        # Pestaña de vista previa
        self.tab_preview = ttk.Frame(self.preview_notebook)
        self.preview_notebook.add(self.tab_preview, text="Vista Previa")

        # Pestaña de campos encontrados
        self.tab_found = ttk.Frame(self.preview_notebook)
        self.preview_notebook.add(self.tab_found, text="Encontrados")

        # Pestaña de campos no encontrados
        self.tab_missing = ttk.Frame(self.preview_notebook)
        self.preview_notebook.add(self.tab_missing, text="No Encontrados")

        # Contenido de vista previa
        preview_frame = ttk.LabelFrame(self.tab_preview, text="Contenido del Archivo")
        preview_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        self.preview_text = tk.Text(preview_frame, wrap=tk.WORD, height=20)
        preview_scroll = ttk.Scrollbar(preview_frame, command=self.preview_text.yview)
        self.preview_text.configure(yscrollcommand=preview_scroll.set)

        self.preview_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=5, pady=5)
        preview_scroll.pack(side=tk.RIGHT, fill=tk.Y)

        self.preview_text.configure(state=tk.DISABLED)

        # Contenido de campos encontrados
        found_frame = ttk.LabelFrame(self.tab_found, text="Campos Detectados")
        found_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        # Canvas y scrollbar para los campos encontrados
        found_canvas = tk.Canvas(found_frame)
        found_scrollbar = ttk.Scrollbar(found_frame, orient="vertical", command=found_canvas.yview)
        found_canvas.configure(yscrollcommand=found_scrollbar.set)

        found_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        found_canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        # Frame dentro del canvas para campos encontrados
        self.found_fields_frame = ttk.Frame(found_canvas)
        found_canvas.create_window((0, 0), window=self.found_fields_frame, anchor="nw")

        self.found_fields_frame.bind("<Configure>",
                                     lambda e: found_canvas.configure(scrollregion=found_canvas.bbox("all")))

        # Contenido de campos no encontrados
        missing_frame = ttk.LabelFrame(self.tab_missing, text="Campos No Detectados")
        missing_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        # Canvas y scrollbar para los campos no encontrados
        missing_canvas = tk.Canvas(missing_frame)
        missing_scrollbar = ttk.Scrollbar(missing_frame, orient="vertical", command=missing_canvas.yview)
        missing_canvas.configure(yscrollcommand=missing_scrollbar.set)

        missing_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        missing_canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        # Frame dentro del canvas para campos no encontrados
        self.missing_fields_frame = ttk.Frame(missing_canvas)
        missing_canvas.create_window((0, 0), window=self.missing_fields_frame, anchor="nw")

        self.missing_fields_frame.bind("<Configure>",
                                       lambda e: missing_canvas.configure(scrollregion=missing_canvas.bbox("all")))

    def _update_destination_info(self):
        """Actualiza la información de destino de datos desde la configuración."""
        try:
            app = self.parent
            while hasattr(app, 'master') and app.master:
                app = app.master

            if hasattr(app, 'get_schema_table_config'):
                schema, table = app.get_schema_table_config()
                self.dest_info_label.configure(
                    text=f"{schema}.{table}",
                    foreground="green"
                )
            else:
                self.dest_info_label.configure(
                    text="Configure en pestaña Conexión",
                    foreground="orange"
                )
        except Exception as e:
            logger.warning(f"No se pudo actualizar información de destino: {str(e)}")

    def select_excel_file(self):
        """Abre un diálogo para seleccionar un archivo Excel y carga su previsualización."""
        if not PANDAS_AVAILABLE:
            messagebox.showerror(
                "Módulo Requerido",
                "El módulo pandas no está instalado. Por favor, instálelo para usar esta funcionalidad."
            )
            return

        # Actualizar información de destino antes de cargar
        self._update_destination_info()

        file_path = filedialog.askopenfilename(
            title="Seleccionar archivo Excel",
            filetypes=(
                ("Archivos Excel", "*.xlsx *.xls"),
                ("Todos los archivos", "*.*")
            )
        )

        if not file_path:
            return

        try:
            # Actualizar la ruta del archivo
            self.excel_file_path = file_path
            filename = os.path.basename(file_path)
            self.file_path_var.set(filename)

            # Limpiar previsualización anterior
            self.clear_preview()

            # Actualizar estado
            self.status_label.configure(text="Cargando archivo...", foreground="black")
            self.parent.update()

            # Usar el procesador para cargar el archivo
            success, message, data = self.excel_processor.load_file(file_path)

            if not success:
                raise ValueError(message)

            # Obtener información del resultado
            found_fields = data["found_fields"]
            missing_fields = data["missing_fields"]
            record_count = data["record_count"]

            # Actualizar la visualización de campos
            self._update_fields_display(found_fields, missing_fields)

            # Actualizar estadísticas
            total_fields = len(self.excel_processor.get_field_mappings())
            self.total_fields_label.configure(text=f"Total de campos: {total_fields}")
            self.found_count_label.configure(text=f"Encontrados: {len(found_fields)}")
            self.missing_count_label.configure(text=f"No encontrados: {len(missing_fields)}")
            self.records_label.configure(text=f"Registros: {record_count}")

            # Mostrar previsualización
            self._show_preview(filename, found_fields, missing_fields, record_count)

            # Habilitar el botón de carga si hay datos
            processed_data = self.excel_processor.get_processed_data()
            if processed_data:
                self.upload_button.configure(state="normal")
                self.status_label.configure(text="Archivo listo para cargar", foreground="green")
            else:
                self.upload_button.configure(state="disabled")
                self.status_label.configure(text="Sin datos válidos", foreground="orange")

            logger.info(f"Archivo Excel cargado: {file_path} - {record_count} registros encontrados")

        except ValueError as ve:
            self._show_error(f"Formato incorrecto: {str(ve)}")
            logger.error(f"Error de formato en archivo Excel: {str(ve)}")

        except Exception as e:
            self._show_error(f"Error al cargar archivo: {str(e)}")
            logger.error(f"Error al cargar archivo Excel: {str(e)}")

    def _show_preview(self, filename, found_fields, missing_fields, record_count):
        """Muestra la previsualización del archivo cargado."""
        self.add_preview_text(f"Archivo: {filename}\n\n")
        self.add_preview_text(f"Estadísticas:\n")
        self.add_preview_text(f"  • Campos encontrados: {len(found_fields)}\n")
        self.add_preview_text(f"  • Registros detectados: {record_count}\n\n")

        # Mostrar muestra de los primeros registros
        processed_data = self.excel_processor.get_processed_data()
        if processed_data:
            self.add_preview_text("Muestra de datos (primeros 3 registros):\n\n")
            preview_data = self.excel_processor.get_preview_data(3)
            field_mappings = self.excel_processor.get_field_mappings()

            for i, record in enumerate(preview_data, 1):
                self.add_preview_text(f"Registro {i}:\n", "SUCCESS")
                for db_field, value in list(record.items())[:5]:  # Mostrar solo los primeros 5 campos
                    original_field = next((k for k, v in field_mappings.items() if v == db_field), db_field)
                    formatted_value = value
                    if isinstance(value, pd.Timestamp):
                        formatted_value = value.strftime('%d/%m/%Y')
                    self.add_preview_text(f"  {original_field}: {formatted_value}\n")
                self.add_preview_text("\n")

            if len(processed_data) > 3:
                self.add_preview_text(f"... y {len(processed_data) - 3} registros más\n")

    def _show_error(self, error_message):
        """Muestra un mensaje de error en la interfaz."""
        self.add_preview_text(f"ERROR: {error_message}\n\n", "ERROR")
        self.status_label.configure(text="Error en archivo", foreground="red")
        self.upload_button.configure(state="disabled")

        # Resetear estadísticas
        self.total_fields_label.configure(text="Total de campos: 0")
        self.found_count_label.configure(text="Encontrados: 0")
        self.missing_count_label.configure(text="No encontrados: 0")
        self.records_label.configure(text="Registros: 0")

    def _update_fields_display(self, found_fields, missing_fields):
        """Actualiza la visualización de campos encontrados y no encontrados."""
        # Limpiar contenedores
        for widget in self.found_fields_frame.winfo_children():
            widget.destroy()

        for widget in self.missing_fields_frame.winfo_children():
            widget.destroy()

        # Mostrar campos encontrados
        if found_fields:
            for field in found_fields:
                field_label = ttk.Label(
                    self.found_fields_frame,
                    text=f"✓ {field}",
                    foreground="green"
                )
                field_label.pack(anchor=tk.W, pady=2, padx=5)
        else:
            ttk.Label(
                self.found_fields_frame,
                text="No se encontraron campos coincidentes",
                foreground="orange"
            ).pack(pady=20)

        # Mostrar campos no encontrados
        if missing_fields:
            for field in missing_fields:
                field_label = ttk.Label(
                    self.missing_fields_frame,
                    text=f"✗ {field}",
                    foreground="red"
                )
                field_label.pack(anchor=tk.W, pady=2, padx=5)
        else:
            ttk.Label(
                self.missing_fields_frame,
                text="¡Todos los campos fueron encontrados!",
                foreground="green"
            ).pack(pady=20)

    def add_preview_text(self, message, level="INFO"):
        """Añade texto al área de previsualización."""
        colors = {
            "INFO": "black",
            "SUCCESS": "green",
            "WARNING": "orange",
            "ERROR": "red"
        }

        color = colors.get(level, "black")

        self.preview_text.configure(state=tk.NORMAL)
        self.preview_text.insert(tk.END, message, (level,))
        self.preview_text.tag_config(level, foreground=color)
        self.preview_text.see(tk.END)
        self.preview_text.configure(state=tk.DISABLED)

    def clear_preview(self):
        """Limpia el área de previsualización."""
        self.preview_text.configure(state=tk.NORMAL)
        self.preview_text.delete(1.0, tk.END)
        self.preview_text.configure(state=tk.DISABLED)

        # Limpiar campos
        for widget in self.found_fields_frame.winfo_children():
            widget.destroy()
        for widget in self.missing_fields_frame.winfo_children():
            widget.destroy()

    def _update_progress(self, progress):
        """Actualiza la barra de progreso."""
        self.progress_bar["value"] = progress * 100
        self.parent.update_idletasks()

    def _update_status(self, status_text, color="black"):
        """Actualiza el estado."""
        self.parent.after(0, lambda: self.status_label.configure(text=status_text, foreground=color))

    def _handle_db_message(self, message, level="INFO"):
        """Maneja mensajes del DBUploader."""
        self.parent.after(0, lambda: self.add_preview_text(f"{message}\n", level))

    def upload_to_database(self):
        """Inicia el proceso de carga de datos a la base de datos."""
        processed_data = self.excel_processor.get_processed_data()

        if not processed_data:
            messagebox.showwarning("Sin datos", "No hay datos para cargar a la base de datos.")
            return

        if self.is_uploading:
            messagebox.showinfo("En progreso", "Ya hay una carga en progreso. Por favor espere.")
            return

        # Obtener el conector de base de datos
        connector = self.get_connector_callback()
        if not connector:
            messagebox.showerror("Error de conexión",
                                 "No hay una conexión activa a la base de datos.\n"
                                 "Por favor, configure y pruebe la conexión en la pestaña 'Conexión'.")
            return

        # Obtener configuración de esquema y tabla
        app = self.parent
        while hasattr(app, 'master') and app.master:
            app = app.master

        if hasattr(app, 'get_schema_table_config'):
            schema, table = app.get_schema_table_config()
        else:
            schema, table = "automatizacion", "datos_excel_doforms"

        # Confirmar la operación
        if not messagebox.askyesno("Confirmar carga",
                                   f"¿Está seguro de cargar {len(processed_data)} registros a:\n\n"
                                   f"{schema}.{table}"):
            return

        # Iniciar hilo de carga
        self.is_uploading = True
        self.upload_thread = threading.Thread(
            target=self._upload_thread_function,
            args=(connector, processed_data, schema, table),
            daemon=True
        )
        self.upload_thread.start()

        # Actualizar interfaz
        self.upload_button.configure(state="disabled")
        self._update_status("Cargando datos...", "orange")
        self.progress_bar["value"] = 0

    def _upload_thread_function(self, connector, data, schema, table):
        """Función que se ejecuta en un hilo separado para cargar los datos."""
        try:
            # Limpiar previsualización
            self.parent.after(0, lambda: self.clear_preview())

            # Crear el cargador de BD
            db_uploader = DBUploader(connector, schema=schema, table=table)

            # Configurar callbacks
            db_uploader.set_callbacks(
                progress_callback=self._update_progress,
                message_callback=self._handle_db_message
            )

            # Verificar estructura de tabla
            self._update_status("Verificando base de datos...", "orange")

            column_types = self.excel_processor.get_column_types()
            if not db_uploader.verify_table_structure(column_types):
                self._update_status("Error en verificación", "red")
                return

            # Cargar datos
            self._update_status(f"Cargando a {schema}.{table}...", "orange")

            success, results = db_uploader.upload_data(data)

            # Mostrar resultados
            total_records = results["total_records"]
            success_count = results["success_count"]
            error_count = results["error_count"]
            errors = results["errors"]

            # Actualizar estadísticas finales
            self.parent.after(0, lambda: self.records_label.configure(
                text=f"Procesados: {success_count}/{total_records}"
            ))

            # Mostrar errores si los hay
            if errors:
                self.parent.after(0, lambda: self.add_preview_text("✗ Errores encontrados:\n", "ERROR"))
                for error in errors[:5]:  # Mostrar solo los primeros 5
                    self.parent.after(0, lambda e=error: self.add_preview_text(f"• {e}\n", "ERROR"))

                if len(errors) > 5:
                    self.parent.after(0, lambda: self.add_preview_text(
                        f"... y {len(errors) - 5} errores más\n", "ERROR"
                    ))

            # Estado final
            if error_count == 0:
                self._update_status(f"✓ Completado: {success_count} registros cargados", "green")
            else:
                self._update_status(f"⚠ Completado con {error_count} errores", "orange")

        except Exception as e:
            self.parent.after(0, lambda: self.add_preview_text(
                f"✗ ERROR CRÍTICO: {str(e)}\n", "ERROR"
            ))
            self._update_status("✗ Error en carga", "red")
            logger.error(f"Error crítico en carga de datos: {str(e)}")

        finally:
            self.parent.after(0, lambda: self.upload_button.configure(state="normal"))
            self.is_uploading = False
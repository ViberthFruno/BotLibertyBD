# conexion_dialog.py
"""
Diálogo de configuración PostgreSQL para EnlaceDB.

Proporciona una interfaz modal simplificada para configurar la conexión a PostgreSQL,
incluyendo parámetros de conexión, pruebas de conectividad y verificación de estructura.
Los valores de configuración se obtienen únicamente del archivo JSON, sin datos hardcodeados.

OPTIMIZADO: Usa threading para pruebas de conexión sin bloquear la UI.
"""

import tkinter as tk
from tkinter import ttk, messagebox
import threading
from postgres_connector import PostgresConnector
from logger import logger


class ConexionDialog(tk.Toplevel):
    """Ventana de diálogo para configurar la conexión PostgreSQL."""

    def __init__(self, parent, existing_config=None):
        """
        Inicializa el diálogo de configuración PostgreSQL.

        Args:
            parent: Widget padre.
            existing_config (dict): Configuración existente para edición.
        """
        super().__init__(parent)
        self.parent = parent
        self.existing_config = existing_config or {}
        self.result = None
        self.postgres_connector = None

        self._setup_window()
        self._create_widgets()
        self._apply_existing_config()

    def _setup_window(self):
        """Configura las propiedades básicas de la ventana."""
        self.title("Configuración PostgreSQL")
        self.geometry("580x700")
        self.grab_set()  # Modal
        self.resizable(False, False)

        # Centrar en pantalla
        self.update_idletasks()
        width = self.winfo_width()
        height = self.winfo_height()
        x = (self.winfo_screenwidth() // 2) - (width // 2)
        y = (self.winfo_screenheight() // 2) - (height // 2)
        self.geometry(f'+{x}+{y}')

    def _create_widgets(self):
        """Crea todos los widgets del diálogo."""
        main_frame = ttk.Frame(self)
        main_frame.pack(padx=20, pady=20, fill=tk.BOTH, expand=True)

        # Título
        title_label = ttk.Label(main_frame, text="Configuración PostgreSQL",
                                font=("Arial", 16, "bold"))
        title_label.pack(pady=(0, 20))

        # Secciones
        self._create_connection_section(main_frame)
        self._create_destination_section(main_frame)
        self._create_actions_section(main_frame)

    def _create_connection_section(self, parent):
        """Crea la sección de parámetros de conexión SIN valores hardcodeados."""
        frame = ttk.LabelFrame(parent, text="Parámetros de Conexión", padding=15)
        frame.pack(fill=tk.X, pady=(0, 15))

        # Configuración de campos SIN valores por defecto hardcodeados
        fields = [
            ("Servidor:", "host"),
            ("Puerto:", "port"),
            ("Base de datos:", "db"),
            ("Usuario:", "user"),
            ("Contraseña:", "pass")
        ]

        for label_text, field_name in fields:
            field_frame = ttk.Frame(frame)
            field_frame.pack(fill=tk.X, pady=6)

            ttk.Label(field_frame, text=label_text, width=12).pack(side=tk.LEFT)

            entry = ttk.Entry(field_frame, width=40)
            if field_name == "pass":
                entry.configure(show="*")
            entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(10, 0))

            # NO insertar valores por defecto aquí - se hará en _apply_existing_config
            setattr(self, f"{field_name}_entry", entry)

    def _create_destination_section(self, parent):
        """Crea la sección de destino de datos SIN valores hardcodeados."""
        frame = ttk.LabelFrame(parent, text="Destino de Datos", padding=15)
        frame.pack(fill=tk.X, pady=(0, 15))

        # Campo esquema
        schema_frame = ttk.Frame(frame)
        schema_frame.pack(fill=tk.X, pady=5)
        ttk.Label(schema_frame, text="Esquema:", width=12).pack(side=tk.LEFT)
        self.schema_entry = ttk.Entry(schema_frame, width=40)
        self.schema_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(10, 0))
        # NO insertar valor por defecto aquí

        # Campo tabla
        table_frame = ttk.Frame(frame)
        table_frame.pack(fill=tk.X, pady=5)
        ttk.Label(table_frame, text="Tabla:", width=12).pack(side=tk.LEFT)
        self.table_entry = ttk.Entry(table_frame, width=40)
        self.table_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(10, 0))
        # NO insertar valor por defecto aquí

    def _create_actions_section(self, parent):
        """Crea la sección de acciones y botones."""
        frame = ttk.LabelFrame(parent, text="Verificación y Configuración", padding=15)
        frame.pack(fill=tk.X, pady=(0, 15))

        # Botones organizados en grid 2x3
        buttons_frame = ttk.Frame(frame)
        buttons_frame.pack(fill=tk.X, pady=(0, 15))
        buttons_frame.grid_columnconfigure(0, weight=1)
        buttons_frame.grid_columnconfigure(1, weight=1)

        # Primera fila: botones de verificación
        ttk.Button(buttons_frame, text="Probar Conexión",
                   command=self.test_connection).grid(row=0, column=0, padx=5, pady=5, sticky="ew")

        ttk.Button(buttons_frame, text="Verificar Esquema/Tabla",
                   command=self.verify_table).grid(row=0, column=1, padx=5, pady=5, sticky="ew")

        # Segunda fila: botón de limpiar datos (centrado)
        ttk.Button(buttons_frame, text="🗑️ Limpiar Todos los Datos",
                   command=self.clear_all_data).grid(row=1, column=0, columnspan=2, padx=5, pady=5, sticky="ew")

        # Tercera fila: botones de acción
        ttk.Button(buttons_frame, text="Cancelar",
                   command=self.cancel).grid(row=2, column=0, padx=5, pady=5, sticky="ew")

        ttk.Button(buttons_frame, text="Guardar Configuración",
                   command=self.save).grid(row=2, column=1, padx=5, pady=5, sticky="ew")

        # Estado
        self.status_label = ttk.Label(frame, text="Estado: Esperando verificación...",
                                      font=("Arial", 10), foreground="orange")
        self.status_label.pack(anchor=tk.W)

    def _apply_existing_config(self):
        """
        Aplica la configuración existente a los campos.
        Si no hay configuración, los campos quedan vacíos.
        """
        if not self.existing_config:
            logger.info("No hay configuración previa - campos vacíos")
            return

        # Mapeo de configuración a campos de entrada
        field_mappings = {
            "host": self.host_entry,
            "port": self.port_entry,
            "database": self.db_entry,
            "username": self.user_entry,
            "password": self.pass_entry,
            "schema": self.schema_entry,
            "table": self.table_entry
        }

        # Aplicar solo los valores que existen en la configuración
        for config_key, entry_widget in field_mappings.items():
            if config_key in self.existing_config:
                value = self.existing_config[config_key]
                if value:  # Solo insertar si hay un valor
                    entry_widget.delete(0, tk.END)
                    entry_widget.insert(0, str(value))
                    logger.debug(f"Aplicando configuración: {config_key} = {value}")

    def _get_connection_params(self):
        """Obtiene y valida los parámetros de conexión."""
        params = {
            "host": self.host_entry.get().strip(),
            "port": self.port_entry.get().strip(),
            "database": self.db_entry.get().strip(),
            "username": self.user_entry.get().strip(),
            "password": self.pass_entry.get()
        }

        # Validar campos obligatorios
        required_fields = ["host", "port", "database", "username"]
        missing_fields = [field for field in required_fields if not params[field]]

        if missing_fields:
            messagebox.showerror("Campos incompletos",
                                 f"Los siguientes campos son obligatorios: {', '.join(missing_fields)}")
            return None

        return params

    def test_connection(self):
        """Prueba la conexión a PostgreSQL en un hilo separado para no bloquear la UI."""
        params = self._get_connection_params()
        if not params:
            self.status_label.configure(text="Estado: ✗ Campos incompletos", foreground="red")
            return

        self.status_label.configure(text="Estado: Probando conexión...", foreground="black")

        # Crear un diálogo de progreso
        progress_dialog = tk.Toplevel(self)
        progress_dialog.title("Probando Conexión")
        progress_dialog.geometry("350x120")
        progress_dialog.transient(self)
        progress_dialog.grab_set()

        ttk.Label(progress_dialog, text="Probando conexión a PostgreSQL...", font=("Arial", 10)).pack(pady=15)
        ttk.Label(progress_dialog, text="Esto puede tardar hasta 10 segundos", font=("Arial", 8), foreground="gray").pack()
        progress_bar = ttk.Progressbar(progress_dialog, mode='indeterminate')
        progress_bar.pack(pady=10, padx=20, fill=tk.X)
        progress_bar.start()

        def run_test():
            """Ejecuta el test de conexión en un hilo separado."""
            try:
                # Crear y probar conector
                connector = PostgresConnector(**params)
                success, message = connector.test_connection()

                # Actualizar UI en el hilo principal
                self.after(0, lambda: self._show_connection_result(success, message, connector, progress_dialog))
            except Exception as e:
                self.after(0, lambda: self._show_connection_result(False, str(e), None, progress_dialog))

        # Iniciar el test en un hilo separado
        test_thread = threading.Thread(target=run_test, daemon=True)
        test_thread.start()

    def _show_connection_result(self, success, message, connector, progress_dialog):
        """Muestra el resultado del test de conexión en el hilo principal."""
        try:
            progress_dialog.destroy()
        except:
            pass

        if success:
            self.postgres_connector = connector
            self.status_label.configure(text="Estado: ✓ Conexión exitosa", foreground="green")
            messagebox.showinfo("Conexión exitosa", f"Conexión establecida correctamente.\n\n{message}")
            logger.info(f"Conexión exitosa: {message}")
        else:
            self.status_label.configure(text="Estado: ✗ Error de conexión", foreground="red")
            messagebox.showerror("Error de conexión", f"No se pudo conectar:\n\n{message}")
            logger.error(f"Error de conexión: {message}")

    def verify_table(self):
        """Verifica la existencia del esquema y tabla."""
        schema = self.schema_entry.get().strip()
        table = self.table_entry.get().strip()

        if not schema or not table:
            messagebox.showerror("Campos incompletos",
                                 "Debe especificar tanto el esquema como la tabla.")
            return

        # Verificar que se haya probado la conexión primero
        if not self.postgres_connector:
            messagebox.showwarning("Conexión requerida",
                                   "Debe probar la conexión primero usando el botón 'Probar Conexión'.")
            return

        if not self.postgres_connector.connect():
            messagebox.showerror("Error de conexión",
                                 "No se pudo conectar. Verifique la configuración.")
            return

        try:
            # Verificar esquema
            if not self._check_schema_exists(schema):
                return

            # Verificar tabla
            if not self._check_table_exists(schema, table):
                self._handle_missing_table(schema, table)
            else:
                self._show_table_info(schema, table)

        except Exception as e:
            messagebox.showerror("Error de verificación", f"Error: {str(e)}")
            logger.error(f"Error en verificación: {str(e)}")
        finally:
            self.postgres_connector.disconnect()

    def clear_all_data(self):
        """Limpia todos los datos de la tabla especificada."""
        schema = self.schema_entry.get().strip()
        table = self.table_entry.get().strip()

        if not schema or not table:
            messagebox.showerror("Campos incompletos",
                                 "Debe especificar tanto el esquema como la tabla.")
            return

        # Verificar que se haya probado la conexión primero
        if not self.postgres_connector:
            messagebox.showwarning("Conexión requerida",
                                   "Debe probar la conexión primero usando el botón 'Probar Conexión'.")
            return

        if not self.postgres_connector.connect():
            messagebox.showerror("Error de conexión",
                                 "No se pudo conectar. Verifique la configuración.")
            return

        try:
            # Verificar que la tabla exista
            if not self._check_table_exists(schema, table):
                messagebox.showerror("Tabla no encontrada",
                                     f"La tabla '{schema}.{table}' no existe.")
                return

            # Obtener conteo de registros antes de eliminar
            count_query = f"SELECT COUNT(*) FROM {schema}.{table};"
            result = self.postgres_connector.execute_query(count_query)

            if result is None:
                messagebox.showerror("Error", "No se pudo obtener información de la tabla.")
                return

            record_count = result[0][0] if result else 0

            # Confirmación con información del conteo
            if record_count == 0:
                messagebox.showinfo("Tabla vacía",
                                    f"La tabla '{schema}.{table}' no contiene datos.")
                return

            confirmation_message = (
                f"⚠️ ADVERTENCIA ⚠️\n\n"
                f"Esta acción eliminará TODOS los datos de la tabla:\n"
                f"{schema}.{table}\n\n"
                f"Registros actuales: {record_count:,}\n\n"
                f"Esta operación NO se puede deshacer.\n\n"
                f"¿Está seguro de que desea continuar?"
            )

            if not messagebox.askyesno("Confirmar eliminación de datos",
                                       confirmation_message,
                                       icon="warning"):
                return

            # Doble confirmación para operaciones críticas
            if record_count > 100:  # Para tablas con muchos registros
                second_confirmation = (
                    f"CONFIRMACIÓN FINAL\n\n"
                    f"Se eliminarán {record_count:,} registros.\n"
                    f"Escriba 'ELIMINAR' para confirmar:"
                )

                # Diálogo personalizado para confirmación de texto
                confirm_dialog = tk.Toplevel(self)
                confirm_dialog.title("Confirmación Final")
                confirm_dialog.geometry("400x200")
                confirm_dialog.resizable(False, False)
                confirm_dialog.grab_set()

                # Centrar el diálogo
                confirm_dialog.update_idletasks()
                x = (confirm_dialog.winfo_screenwidth() // 2) - 200
                y = (confirm_dialog.winfo_screenheight() // 2) - 100
                confirm_dialog.geometry(f"+{x}+{y}")

                confirmed = [False]  # Lista para poder modificar desde función anidada

                ttk.Label(confirm_dialog, text=second_confirmation,
                          justify="center").pack(pady=20)

                entry_var = tk.StringVar()
                confirm_entry = ttk.Entry(confirm_dialog, textvariable=entry_var, width=30)
                confirm_entry.pack(pady=10)

                def check_confirmation():
                    if entry_var.get().upper() == "ELIMINAR":
                        confirmed[0] = True
                        confirm_dialog.destroy()
                    else:
                        messagebox.showerror("Texto incorrecto",
                                             "Debe escribir exactamente 'ELIMINAR' para confirmar.")

                def cancel_confirmation():
                    confirm_dialog.destroy()

                button_frame = ttk.Frame(confirm_dialog)
                button_frame.pack(pady=10)

                ttk.Button(button_frame, text="Confirmar",
                           command=check_confirmation).pack(side=tk.LEFT, padx=5)
                ttk.Button(button_frame, text="Cancelar",
                           command=cancel_confirmation).pack(side=tk.LEFT, padx=5)

                confirm_entry.focus()
                self.wait_window(confirm_dialog)

                if not confirmed[0]:
                    return

            # Ejecutar la eliminación
            self.status_label.configure(text="Estado: Eliminando datos...", foreground="orange")
            self.update()

            delete_query = f"DELETE FROM {schema}.{table};"
            result = self.postgres_connector.execute_query(delete_query)

            if result is not None:
                self.status_label.configure(text="Estado: ✓ Datos eliminados", foreground="green")
                messagebox.showinfo("Datos eliminados",
                                    f"✓ Todos los datos han sido eliminados exitosamente.\n\n"
                                    f"Registros eliminados: {record_count:,}\n"
                                    f"Tabla: {schema}.{table}")
                logger.info(f"Datos eliminados exitosamente de {schema}.{table} - {record_count} registros")
            else:
                self.status_label.configure(text="Estado: ✗ Error al eliminar", foreground="red")
                messagebox.showerror("Error",
                                     "No se pudieron eliminar los datos. "
                                     "Verifique los permisos y la conexión.")

        except Exception as e:
            self.status_label.configure(text="Estado: ✗ Error", foreground="red")
            messagebox.showerror("Error de eliminación", f"Error: {str(e)}")
            logger.error(f"Error al eliminar datos: {str(e)}")
        finally:
            self.postgres_connector.disconnect()

    def _check_schema_exists(self, schema):
        """Verifica si el esquema existe."""
        query = "SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = %s);"
        result = self.postgres_connector.execute_query(query, (schema,))

        if not result or not result[0][0]:
            messagebox.showerror("Esquema no encontrado",
                                 f"El esquema '{schema}' no existe.\n\n"
                                 f"Verifique el nombre o créelo antes de continuar.")
            return False

        logger.info(f"Esquema '{schema}' encontrado")
        return True

    def _check_table_exists(self, schema, table):
        """Verifica si la tabla existe."""
        query = """SELECT EXISTS (SELECT 1 FROM information_schema.tables 
                   WHERE table_schema = %s AND table_name = %s);"""
        result = self.postgres_connector.execute_query(query, (schema, table))
        return result and result[0][0]

    def _handle_missing_table(self, schema, table):
        """Maneja el caso cuando la tabla no existe."""
        response = messagebox.askyesno("Tabla no encontrada",
                                       f"La tabla '{schema}.{table}' no existe.\n\n"
                                       f"¿Desea que se cree automáticamente al cargar datos?")

        message = ("La tabla será creada automáticamente" if response
                   else "Deberá crear la tabla manualmente")
        messagebox.showinfo("Configuración", message)
        logger.info(f"Tabla faltante - Usuario eligió: {'auto-crear' if response else 'crear manual'}")

    def _show_table_info(self, schema, table):
        """Muestra información de la tabla existente."""
        query = """SELECT column_name, data_type FROM information_schema.columns 
                   WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position;"""
        columns = self.postgres_connector.execute_query(query, (schema, table))

        if columns:
            messagebox.showinfo("Verificación exitosa",
                                f"✓ Esquema '{schema}' encontrado\n"
                                f"✓ Tabla '{table}' encontrada\n"
                                f"✓ {len(columns)} columnas disponibles\n\n"
                                f"Configuración válida.")
            logger.info(f"Verificación exitosa: {schema}.{table} con {len(columns)} columnas")

    def save(self):
        """Guarda la configuración."""
        params = self._get_connection_params()
        if not params:
            return

        schema = self.schema_entry.get().strip()
        table = self.table_entry.get().strip()

        if not schema or not table:
            messagebox.showwarning("Datos incompletos", "Complete esquema y tabla.")
            return

        # Construir resultado
        self.result = {
            "host": params["host"],
            "port": params["port"],
            "database": params["database"],
            "username": params["username"],
            "schema": schema,
            "table": table
        }

        # Incluir contraseña si existe
        if params["password"]:
            self.result["password"] = params["password"]

        logger.info(f"Configuración guardada: {list(self.result.keys())}")
        self.destroy()

    def cancel(self):
        """Cancela y cierra el diálogo."""
        self.result = None
        self.destroy()
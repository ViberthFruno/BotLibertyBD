# app_gui.py
"""
Interfaz gráfica principal para la aplicación EnlaceDB.

Este archivo implementa la ventana principal de la aplicación utilizando Tkinter estándar,
gestionando la inicialización de la interfaz, la carga/guardado de configuraciones JSON
y la coordinación entre los diferentes componentes de la GUI.
"""

import os
import sys
import json
import tkinter as tk
from tkinter import ttk, messagebox

from logger import logger
from postgres_connector import is_psycopg2_installed
from tab_principal import PrincipalTab

# Definición de tamaño para la ventana
WINDOW_WIDTH = 1100
WINDOW_HEIGHT = 750


def get_app_directory():
    """
    Obtiene el directorio donde está ubicada la aplicación.
    Funciona tanto para ejecutables empaquetados como para desarrollo.

    Returns:
        str: Ruta absoluta del directorio de la aplicación.
    """
    try:
        if getattr(sys, 'frozen', False):
            app_dir = os.path.dirname(sys.executable)
        else:
            app_dir = os.path.dirname(os.path.abspath(__file__))

        logger.info(f"Directorio de aplicación detectado: {app_dir}")
        return app_dir
    except Exception as e:
        logger.error(f"Error al obtener directorio de aplicación: {e}")
        return os.getcwd()


def get_config_file_path():
    """
    Obtiene la ruta absoluta para el archivo de configuración principal.

    Returns:
        str: Ruta absoluta del archivo config.json.
    """
    app_dir = get_app_directory()
    config_path = os.path.join(app_dir, "config.json")
    return config_path


class EnlaceDBApp(tk.Tk):
    """Clase principal de la interfaz gráfica de la aplicación EnlaceDB."""

    def __init__(self):
        """Inicializa la ventana principal y los componentes de la interfaz."""
        super().__init__()

        # Configuración inicial de la ventana
        self.title("EnlaceDB")
        self.geometry(f"{WINDOW_WIDTH}x{WINDOW_HEIGHT}")
        self.resizable(True, True)

        # Intentar cargar icono si existe
        self._try_load_icon()

        # Configuración de archivos
        self.config_file = get_config_file_path()
        logger.info(f"Archivo de configuración: {self.config_file}")

        # Crear el contenido de la interfaz
        self.create_widgets()

        # Cargar configuración guardada
        self.config = self.load_config()

        # Aplicar configuración a las pestañas
        if self.config:
            self.tab_principal.apply_config(self.config)
            logger.info("Configuración aplicada a las pestañas")
        else:
            logger.info("No hay configuración previa para aplicar")

        # Verificar la disponibilidad de psycopg2
        self.check_psycopg2()

        # Centrar ventana
        self.center_window()

    def _try_load_icon(self):
        """
        Intenta cargar el icono de la aplicación.

        Funciona tanto en desarrollo como cuando se empaqueta con PyInstaller.
        Busca el icono primero en el directorio temporal de PyInstaller (_MEIPASS)
        y luego en el directorio de la aplicación.
        """
        try:
            # Intentar primero con _MEIPASS (para PyInstaller)
            if hasattr(sys, '_MEIPASS'):
                icon_path = os.path.join(sys._MEIPASS, "icon.ico")
                if os.path.exists(icon_path):
                    self.iconbitmap(icon_path)
                    logger.info(f"Icono cargado desde paquete: {icon_path}")
                    return

            # Si no está empaquetado, buscar en el directorio de la aplicación
            icon_path = os.path.join(get_app_directory(), "icon.ico")
            if os.path.exists(icon_path):
                self.iconbitmap(icon_path)
                logger.info(f"Icono cargado: {icon_path}")
            else:
                logger.debug(f"Icono no encontrado en: {icon_path}")

        except Exception as e:
            logger.warning(f"No se pudo cargar el icono: {e}")

    def center_window(self):
        """Centra la ventana en la pantalla."""
        self.update_idletasks()
        width = self.winfo_width()
        height = self.winfo_height()
        x = (self.winfo_screenwidth() // 2) - (width // 2)
        y = (self.winfo_screenheight() // 2) - (height // 2)
        self.geometry(f'+{x}+{y}')

    def create_widgets(self):
        """Crea y configura los widgets principales de la interfaz."""
        # Crear un frame principal
        main_frame = ttk.Frame(self)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        # Crear frame para la pestaña principal
        principal_frame = ttk.Frame(main_frame)
        principal_frame.pack(fill=tk.BOTH, expand=True)

        # Inicializar la pestaña principal
        self.tab_principal = PrincipalTab(principal_frame, self.save_config)

    def get_connector(self):
        """
        Proporciona acceso al conector PostgreSQL para otras pestañas.

        Returns:
            PostgresConnector: Conector de PostgreSQL o None si no está disponible.
        """
        return self.tab_principal.get_connector()

    def get_schema_table_config(self):
        """
        Obtiene la configuración actual de esquema y tabla desde la pestaña principal.

        Returns:
            tuple: (esquema, tabla) configurados actualmente.
        """
        return self.tab_principal.get_schema_table_config()

    def load_config(self):
        """
        Carga la configuración desde el archivo JSON si existe.

        Returns:
            dict: Configuración cargada o diccionario vacío si no existe/hay error.
        """
        if not os.path.exists(self.config_file):
            logger.info(f"No se encontró archivo de configuración: {self.config_file}")
            return {}

        try:
            with open(self.config_file, 'r', encoding='utf-8') as f:
                config = json.load(f)

            logger.info(f"Configuración cargada correctamente desde: {self.config_file}")
            return config

        except json.JSONDecodeError as e:
            logger.error(f"Error de formato JSON en configuración: {e}")
            self._handle_corrupted_config()
            return {}

        except UnicodeDecodeError as e:
            logger.warning(f"Problema de codificación en configuración: {e}")
            return self._try_load_with_fallback_encoding()

        except Exception as e:
            logger.error(f"Error inesperado al cargar configuración: {e}")
            return {}

    def _try_load_with_fallback_encoding(self):
        """
        Intenta cargar la configuración con codificación alternativa.

        Returns:
            dict: Configuración cargada o diccionario vacío si falla.
        """
        try:
            with open(self.config_file, 'r', encoding='latin-1') as f:
                config = json.loads(f.read())

            logger.info("Configuración cargada con codificación latin-1, re-guardando en UTF-8")

            # Re-guardar en UTF-8 para futuras lecturas
            if self.save_config(config):
                logger.info("Configuración normalizada a UTF-8")

            return config

        except Exception as e:
            logger.error(f"Error al cargar con codificación alternativa: {e}")
            self._handle_corrupted_config()
            return {}

    def _handle_corrupted_config(self):
        """Maneja el caso de configuración corrupta."""
        backup_name = f"{self.config_file}.backup"

        try:
            # Crear backup del archivo corrupto
            if os.path.exists(self.config_file):
                os.rename(self.config_file, backup_name)
                logger.warning(f"Archivo de configuración corrupto respaldado como: {backup_name}")

            messagebox.showwarning(
                "Configuración Corrupta",
                f"El archivo de configuración estaba corrupto y fue respaldado.\n\n"
                f"Se creará una nueva configuración.\n"
                f"Respaldo guardado en: {os.path.basename(backup_name)}"
            )

        except Exception as e:
            logger.error(f"Error al respaldar configuración corrupta: {e}")

    def save_config(self, config_data):
        """
        Guarda la configuración en el archivo JSON.

        Args:
            config_data (dict): Datos de configuración a guardar.

        Returns:
            bool: True si se guardó correctamente, False en caso contrario.
        """
        if not config_data:
            logger.warning("Intento de guardar configuración vacía")
            return False

        try:
            # Asegurar que el directorio existe
            config_dir = os.path.dirname(self.config_file)
            if config_dir and not os.path.exists(config_dir):
                os.makedirs(config_dir, exist_ok=True)

            # Guardar configuración
            with open(self.config_file, 'w', encoding='utf-8') as f:
                json.dump(config_data, f, indent=4, ensure_ascii=False)

            logger.info(f"Configuración guardada correctamente en: {self.config_file}")
            return True

        except PermissionError:
            logger.error(f"Sin permisos para escribir en: {self.config_file}")
            messagebox.showerror(
                "Error de Permisos",
                f"No se pudo guardar la configuración.\n\n"
                f"Verifique los permisos del directorio:\n{os.path.dirname(self.config_file)}"
            )
            return False

        except Exception as e:
            logger.error(f"Error al guardar configuración: {e}")
            messagebox.showerror(
                "Error al Guardar",
                f"No se pudo guardar la configuración:\n\n{str(e)}"
            )
            return False

    def check_psycopg2(self):
        """Verifica si psycopg2 está instalado y muestra una advertencia si no lo está."""
        if not is_psycopg2_installed():
            warning_message = (
                "El módulo psycopg2 no está instalado.\n"
                "No podrá conectarse a bases de datos PostgreSQL.\n\n"
                "Para instalar psycopg2, ejecute:\n"
                "pip install psycopg2-binary"
            )

            # Agregar mensaje al log de la pestaña principal
            self.tab_principal.add_log(warning_message, "WARNING")

            # Mostrar ventana de advertencia
            messagebox.showwarning("Módulo Requerido", warning_message)
            logger.warning("psycopg2 no está instalado")

    def destroy(self):
        """Override del método destroy para limpiar recursos al cerrar."""
        try:
            logger.info("Aplicación cerrada correctamente")

        except Exception as e:
            logger.error(f"Error al cerrar aplicación: {e}")

        finally:
            super().destroy()
# main.py
# Script principal que inicia la aplicación EnlaceDB, verifica dependencias e inicia la GUI.

import os
import sys
import importlib.util
from logger import logger


def check_dependencies():
    """Verifica dependencias necesarias."""
    deps = {
        "psycopg2": "Para la conexión a bases de datos PostgreSQL",
        "pandas": "Para el procesamiento de archivos Excel",
        "customtkinter": "Para la interfaz gráfica de usuario",
    }
    missing = [f"{p}: {d}" for p, d in deps.items() if importlib.util.find_spec(p) is None]
    for pkg in missing:
        logger.warning(f"Dependencia faltante: {pkg}")
    if missing:
        logger.warning(f"Faltan dependencias: {', '.join(p.split(':')[0] for p in missing)}")
        if any(pkg.startswith(prefix) for pkg in missing for prefix in ("psycopg2", "customtkinter")):
            return False
    return True


def resource_path(relative_path):
    """Retorna la ruta absoluta de un recurso."""
    base_path = getattr(sys, "_MEIPASS", os.path.abspath("."))
    return os.path.join(base_path, relative_path)


def show_error(title, message):
    try:
        import tkinter as tk
        from tkinter import messagebox
        root = tk.Tk(); root.withdraw()
        messagebox.showerror(title, message)
        root.destroy()
    except Exception:
        pass


def main():
    """Función principal que inicia la aplicación."""
    logger.info("Iniciando aplicación EnlaceDB")
    logger.info(f"Directorio de trabajo: {os.getcwd()}")
    frozen = getattr(sys, 'frozen', False)
    path = sys.executable if frozen else os.path.abspath(__file__)
    application_path = os.path.dirname(path)
    logger.info(f"Ejecutando desde {'archivo empaquetado' if frozen else 'script'}: {path}")
    logger.info(f"Ruta de la aplicación: {application_path}")

    if not check_dependencies():
        if frozen:
            show_error(
                "Error de Dependencias",
                "Faltan dependencias críticas para el funcionamiento de la aplicación.\n\n"
                "Por favor, asegúrese de instalar todas las dependencias requeridas:\n"
                "- psycopg2-binary\n"
                "- customtkinter\n\n"
                "La aplicación se cerrará ahora."
            )
            return 1
        logger.warning(
            "Algunas dependencias críticas no están disponibles, pero se continuará en modo de desarrollo."
        )

    try:
        from app_gui import EnlaceDBApp
        app = EnlaceDBApp()
        app.mainloop()
    except Exception as e:
        logger.error(f"Error al iniciar la aplicación: {e}")
        logger.exception("Detalles del error:")
        import traceback; traceback.print_exc()
        show_error(
            "Error Fatal",
            f"Se ha producido un error al iniciar la aplicación:\n\n{e}\n\nConsulte los logs para más detalles."
        )
        return 1

    logger.info("Aplicación cerrada correctamente")
    return 0


if __name__ == "__main__":
    sys.exit(main())

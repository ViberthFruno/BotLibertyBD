# logger.py
"""
Sistema de registro para la aplicación EnlaceDB.

Este archivo proporciona funcionalidades para registrar eventos, errores y mensajes
de información de la aplicación, permitiendo un seguimiento detallado de la ejecución
y facilitando la depuración de posibles problemas durante la conexión a bases de datos
PostgreSQL y la operación general del sistema. Incluye manejo robusto de permisos
para funcionar correctamente tanto en ejecución manual como desde Task Scheduler.
"""

import os
import sys
import logging
import datetime
import tempfile
from pathlib import Path
from logging.handlers import RotatingFileHandler


def get_safe_log_directory():
    """Obtiene un directorio seguro para guardar logs."""
    dirs = []
    try:
        base = Path(sys.executable if getattr(sys, 'frozen', False) else __file__).resolve().parent
        dirs.append(base / "logs")
    except Exception:
        pass
    if os.name == 'nt':
        appdata = os.getenv('APPDATA')
        if appdata:
            dirs.append(Path(appdata) / "EnlaceDB" / "logs")
    dirs += [Path.home() / "EnlaceDB_logs", Path(tempfile.gettempdir()) / "EnlaceDB_logs"]
    for d in dirs:
        try:
            d.mkdir(parents=True, exist_ok=True)
            with tempfile.TemporaryFile(dir=d):
                pass
            return str(d)
        except Exception:
            continue
    return None


def setup_logger():
    """Configura el sistema de logging."""
    logger = logging.getLogger("EnlaceDB")
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console = logging.StreamHandler()
    console.setFormatter(formatter)
    logger.addHandler(console)
    log_dir = get_safe_log_directory()
    if log_dir:
        try:
            handler = RotatingFileHandler(
                os.path.join(log_dir, f"enlacedb_{datetime.datetime.now():%Y%m%d}.log"),
                maxBytes=10485760, backupCount=5)
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.info(f"Sistema de logging iniciado. Directorio de logs: {log_dir}")
        except Exception as e:
            logger.warning(f"No se pudo configurar logging a archivo: {e}. Continuando solo con consola.")
    else:
        logger.warning("No se pudo encontrar un directorio con permisos para logs. Usando solo consola.")
    return logger


# Inicializar el logger
logger = setup_logger()

# Exportar el logger para que otros módulos lo usen
__all__ = ['logger']
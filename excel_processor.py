# excel_processor.py
"""
Procesador de archivos Excel para EnlaceDB.

Este módulo proporciona funcionalidades para cargar, validar y procesar
archivos Excel, extrayendo campos específicos y transformándolos al formato
requerido por la aplicación. Está diseñado para ser reutilizable para distintos
tipos de carga de datos en el sistema.
"""

import os
import pandas as pd
from datetime import datetime, time
from logger import logger


class ExcelProcessor:
    """Clase para procesar archivos Excel y extraer datos estructurados."""

    def __init__(self, field_mappings=None, column_types=None):
        """
        Inicializa el procesador de Excel con mapeos de campos y tipos.

        Args:
            field_mappings (dict): Mapeo de nombres de campo Excel a nombres de campo BD.
            column_types (dict): Tipos de datos para cada columna de la BD.
        """
        # Definir mapeo predeterminado si no se proporciona
        self.field_mappings = field_mappings or self._generate_field_mappings()

        # Definir tipos de columna predeterminados si no se proporcionan
        self.column_types = column_types or self._generate_column_types()

        self.df = None
        self.processed_data = []

    def _generate_field_mappings(self):
        """
        Genera el mapeo de campos Excel a campos de base de datos, separando
        los campos regulares de los campos repetitivos (terminales).

        Returns:
            dict: Mapeo completo de campos
        """
        # Campos no repetitivos (base)
        base_mappings = {
            # Campos originales
            "Nombre del Archivo": "nombre_archivo",
            "Fecha de Reporte": "fecha_reporte",
            "Correlativo": "correlativo",
            "Número Afiliado Gestión Afiliado principal": "numero_afiliado",
            "Nombre del Afiliado": "nombre_afiliado",
            "Indique número de oportunidad": "numero_oportunidad",
            "Indique número de SS": "numero_ss",
            "Atención por": "atencion_por",
            "Cantidad GSM": "cantidad_gsm",
            "Cierre de gestión": "cierre_gestion",
            "Fecha compromiso": "fecha_compromiso",
            "Detalle de trabajo realizado para cierre de gestión": "detalle_trabajo",
            "Entrega de Papelería y Cantidad": "entrega_papeleria",
            "Evaluaciones a realizar": "evaluaciones_realizar",
            "Fecha resolución": "fecha_resolucion",
            "Hora de llegada": "hora_llegada",
            "Hora de salida": "hora_salida",
            "Nombre del oficial técnico que brinda servicio": "nombre_tecnico",
            "Nombre persona que atiende": "nombre_atiende",
            "Revisión General en cualquier visita": "revision_general",
            "Tipo de gestiones": "tipo_gestiones",
            "Tipo de terminal instalada, reprogramada o retirada": "tipo_terminal",
            "Técnico que atiende": "tecnico_atiende",
            "Validación fecha": "fecha_validacion",
            "¿El datáfono instalado lleva código QR?": "tiene_qr",
            "¿Es posible capturar el correo electrónico del comercio?": "correo_comercio_capturado",
            "¿Instalar SIM adicional?": "sim_adicional",
            "¿POS GSM Prestada?": "pos_gsm_prestada",
            # Nuevo campo
            "Datos de terminal": "datos_terminal",
        }

        # Generar campos repetitivos para terminales (20 veces)
        terminal_mappings = self._generate_terminal_mappings()

        # Generar campos repetitivos para SIM adicional (20 veces)
        sim_adicional_mappings = self._generate_sim_adicional_mappings()

        # Combinar y devolver el mapeo completo
        return {**base_mappings, **terminal_mappings, **sim_adicional_mappings}

    def _generate_terminal_mappings(self):
        """
        Genera los mapeos para los campos de terminales que se repiten 20 veces.

        Returns:
            dict: Mapeo de campos de terminales
        """
        terminal_mappings = {}

        # Patrón para generar los campos repetitivos
        terminal_patterns = [
            # Patrón 1: Terminal N - Actualización en Sistema Adquirente
            {
                "prefix": "Terminal{}",
                "field_template": "{} - Actualización en Sistema Adquirente",
                "db_field_template": "terminal_{}_actualizacion",
                "first_format": "",  # Para el primer terminal, no lleva número
                "rest_format": " {}"  # Para los demás terminales, lleva espacio y número
            },
            # Patrón 2: Terminal N - Esta serie fue
            {
                "prefix": "Terminal{}",
                "field_template": "{} - Esta serie fue",
                "db_field_template": "terminal_{}_estado",
                "first_format": "",
                "rest_format": " {}"
            },
            # Patrón 3: Terminal N - Esta serie lleva SIM
            {
                "prefix": "Terminal{}",
                "field_template": "{} - Esta serie lleva SIM",
                "db_field_template": "terminal_{}_lleva_sim",
                "first_format": "",
                "rest_format": " {}"
            },
            # Patrón 4: Terminal N - Modelo de Terminal (NUEVO)
            {
                "prefix": "Terminal{}",
                "field_template": "{} - Modelo de Terminal",
                "db_field_template": "terminal_{}_modelo",
                "first_format": "",
                "rest_format": " {}"
            },
            # Patrón 5: Terminal N - Número de SIM (NUEVO)
            {
                "prefix": "Terminal{}",
                "field_template": "{} - Número de SIM",
                "db_field_template": "terminal_{}_numero_sim",
                "first_format": "",
                "rest_format": " {}"
            },
            # Patrón 6: Terminal N - Número de Serie (NUEVO)
            {
                "prefix": "Terminal{}",
                "field_template": "{} - Número de Serie",
                "db_field_template": "terminal_{}_numero_serie",
                "first_format": "",
                "rest_format": " {}"
            },
            # Patrón 7: Terminal N - Número de Terminal (NUEVO)
            {
                "prefix": "Terminal{}",
                "field_template": "{} - Número de Terminal",
                "db_field_template": "terminal_{}_numero_terminal",
                "first_format": "",
                "rest_format": " {}"
            },
            # Patrón 8: Terminal N - Comentario (NUEVO)
            {
                "prefix": "Terminal{}",
                "field_template": "{} - Comentario",
                "db_field_template": "terminal_{}_comentario",
                "first_format": "",
                "rest_format": " {}"
            }
        ]

        # Generar campos para los 20 terminales
        for pattern in terminal_patterns:
            # Primer terminal (sin número)
            first_prefix = pattern["prefix"].format(pattern["first_format"])
            excel_field = pattern["field_template"].format(first_prefix)
            db_field = pattern["db_field_template"].format(1)
            terminal_mappings[excel_field] = db_field

            # Terminales 2-20 (con número)
            for i in range(2, 21):
                prefix = pattern["prefix"].format(pattern["rest_format"].format(i))
                excel_field = pattern["field_template"].format(prefix)
                db_field = pattern["db_field_template"].format(i)
                terminal_mappings[excel_field] = db_field

        return terminal_mappings

    def _generate_sim_adicional_mappings(self):
        """
        Genera los mapeos para los campos de SIM adicional que se repiten 20 veces.

        Returns:
            dict: Mapeo de campos de SIM adicional
        """
        sim_adicional_mappings = {}

        # Campo original (ya incluido en base_mappings)
        # "¿Instalar SIM adicional?": "sim_adicional"

        # Campos repetitivos con el patrón (N)
        for i in range(2, 21):
            excel_field = f"¿Instalar SIM adicional? ({i})"
            db_field = f"sim_adicional_{i}"
            sim_adicional_mappings[excel_field] = db_field

        return sim_adicional_mappings

    def _generate_column_types(self):
        """
        Genera los tipos de columna para la base de datos, separando
        los campos regulares de los campos repetitivos (terminales).

        Returns:
            dict: Tipos de columna para la base de datos
        """
        # Tipos de columna básicos (no repetitivos)
        base_types = {
            "nombre_archivo": "TEXT",
            "fecha_reporte": "DATE",
            "correlativo": "TEXT",
            "numero_afiliado": "TEXT",
            "nombre_afiliado": "TEXT",
            "numero_oportunidad": "TEXT",
            "numero_ss": "TEXT",
            "atencion_por": "TEXT",
            "cantidad_gsm": "INTEGER",
            "cierre_gestion": "TEXT",
            "fecha_compromiso": "DATE",
            "detalle_trabajo": "TEXT",
            "entrega_papeleria": "TEXT",
            "evaluaciones_realizar": "TEXT",
            "fecha_resolucion": "DATE",
            "hora_llegada": "TIME WITHOUT TIME ZONE",
            "hora_salida": "TIME WITHOUT TIME ZONE",
            "nombre_tecnico": "TEXT",
            "nombre_atiende": "TEXT",
            "revision_general": "TEXT",
            "tipo_gestiones": "TEXT",
            "tipo_terminal": "TEXT",
            "tecnico_atiende": "TEXT",
            "fecha_validacion": "DATE",
            "tiene_qr": "TEXT",
            "correo_comercio_capturado": "TEXT",
            "sim_adicional": "TEXT",
            "pos_gsm_prestada": "TEXT",
            # Nuevo campo
            "datos_terminal": "TEXT",
        }

        # Generar tipos para campos repetitivos de terminales
        terminal_types = self._generate_terminal_types()

        # Generar tipos para campos repetitivos de SIM adicional
        sim_adicional_types = self._generate_sim_adicional_types()

        # Combinar y devolver los tipos completos
        return {**base_types, **terminal_types, **sim_adicional_types}

    def _generate_terminal_types(self):
        """
        Genera los tipos de columna para los campos de terminales que se repiten 20 veces.

        Returns:
            dict: Tipos de columna para campos de terminales
        """
        terminal_types = {}

        # Patrones de campos de terminales
        terminal_patterns = [
            "terminal_{}_actualizacion",
            "terminal_{}_estado",
            "terminal_{}_lleva_sim",
            "terminal_{}_modelo",
            "terminal_{}_numero_sim",
            "terminal_{}_numero_serie",
            "terminal_{}_numero_terminal",
            "terminal_{}_comentario"
        ]

        # Generar tipos para 20 terminales para cada patrón
        for pattern in terminal_patterns:
            for i in range(1, 21):
                terminal_types[pattern.format(i)] = "TEXT"

        return terminal_types

    def _generate_sim_adicional_types(self):
        """
        Genera los tipos de columna para los campos de SIM adicional que se repiten 20 veces.

        Returns:
            dict: Tipos de columna para campos de SIM adicional
        """
        sim_adicional_types = {}

        # Generar tipos para los 19 campos adicionales (2-20)
        for i in range(2, 21):
            sim_adicional_types[f"sim_adicional_{i}"] = "TEXT"

        return sim_adicional_types

    def load_file(self, file_path):
        """
        Carga un archivo Excel y devuelve información sobre su contenido.

        Args:
            file_path (str): Ruta al archivo Excel.

        Returns:
            tuple: (éxito, mensaje, datos)
                - éxito (bool): True si la carga fue exitosa, False en caso contrario
                - mensaje (str): Mensaje descriptivo del resultado
                - datos (dict): Información sobre campos encontrados, faltantes y registros
        """
        try:
            logger.info(f"Cargando archivo Excel: {file_path}")
            self.df = pd.read_excel(file_path)

            # Verificar los campos disponibles en el archivo
            found_fields = []
            missing_fields = []

            for field in self.field_mappings.keys():
                if field in self.df.columns:
                    found_fields.append(field)
                else:
                    missing_fields.append(field)

            # Verificar si hay al menos un campo disponible
            if not found_fields:
                return False, "El archivo no contiene ninguno de los campos requeridos", {}

            # Procesar datos
            self._process_data()

            result_data = {
                "found_fields": found_fields,
                "missing_fields": missing_fields,
                "record_count": len(self.processed_data),
                "file_name": os.path.basename(file_path)
            }

            return True, "Archivo cargado correctamente", result_data

        except Exception as e:
            logger.error(f"Error al cargar archivo Excel: {str(e)}")
            return False, f"Error al cargar archivo: {str(e)}", {}

    def _process_data(self):
        """
        Procesa los datos del DataFrame y los transforma al formato requerido.
        Gestiona conversiones de tipos y manejo de valores nulos.
        """
        if self.df is None:
            return

        self.processed_data = []

        # Extraer los datos para cada registro
        for index, row in self.df.iterrows():
            record = {}
            for field, db_field in self.field_mappings.items():
                if field in self.df.columns:
                    # Para campos de fecha, asegurarnos de que sea un objeto datetime
                    if field in ["Fecha de Reporte", "Fecha compromiso", "Fecha resolución",
                                 "Validación fecha"] and not pd.isna(row[field]):
                        if isinstance(row[field], (datetime, pd.Timestamp)):
                            record[db_field] = row[field]
                        else:
                            # Intentar convertir si es string, especificando dayfirst=True
                            try:
                                record[db_field] = pd.to_datetime(row[field], dayfirst=True)
                            except:
                                record[db_field] = None
                    # Para campos de hora, extraer la hora de un datetime o convertir desde string
                    elif field in ["Hora de llegada", "Hora de salida"] and not pd.isna(row[field]):
                        try:
                            if isinstance(row[field], (datetime, pd.Timestamp)):
                                record[db_field] = row[field].time()
                            else:
                                # Intentar convertir desde varios formatos de hora
                                time_formats = ["%I:%M %p", "%I:%M %p GMT%z", "%H:%M"]
                                converted = False
                                for fmt in time_formats:
                                    try:
                                        time_obj = pd.to_datetime(row[field], format=fmt).time()
                                        record[db_field] = time_obj
                                        converted = True
                                        break
                                    except:
                                        pass

                                if not converted:
                                    record[db_field] = None
                        except:
                            record[db_field] = None
                    # Para el campo cantidad_gsm, asegurarnos de que es un entero
                    elif field == "Cantidad GSM" and not pd.isna(row[field]):
                        try:
                            record[db_field] = int(row[field])
                        except:
                            record[db_field] = None
                    # Para campos de Terminal - Actualización en Sistema Adquirente, Terminal - Esta serie fue,
                    # Terminal - Esta serie lleva SIM, Terminal - Modelo de Terminal, Terminal - Número de SIM,
                    # Terminal - Número de Serie, Terminal - Número de Terminal, Terminal - Comentario
                    elif ("Terminal" in field and
                          ("Actualización en Sistema Adquirente" in field or
                           "Esta serie fue" in field or
                           "Esta serie lleva SIM" in field or
                           "Modelo de Terminal" in field or
                           "Número de SIM" in field or
                           "Número de Serie" in field or
                           "Número de Terminal" in field or
                           "Comentario" in field)):
                        # Manejar estos campos como texto
                        record[db_field] = str(row[field]) if not pd.isna(row[field]) else None
                    # Para campos de SIM adicional repetitivos
                    elif field.startswith("¿Instalar SIM adicional?"):
                        # Manejar estos campos como texto
                        record[db_field] = str(row[field]) if not pd.isna(row[field]) else None
                    # Para el nuevo campo "Datos de terminal"
                    elif field == "Datos de terminal":
                        # Manejar como texto
                        record[db_field] = str(row[field]) if not pd.isna(row[field]) else None
                    else:
                        record[db_field] = row[field] if not pd.isna(row[field]) else None

            # Solo agregar registros que tengan al menos un campo no nulo
            if any(value is not None for value in record.values()):
                self.processed_data.append(record)

    def get_processed_data(self):
        """
        Devuelve los datos procesados.

        Returns:
            list: Lista de diccionarios con los datos procesados.
        """
        return self.processed_data

    def get_field_mappings(self):
        """
        Devuelve el mapeo de campos Excel a campos de BD.

        Returns:
            dict: Mapeo de campos.
        """
        return self.field_mappings

    def get_column_types(self):
        """
        Devuelve los tipos de columna para la BD.

        Returns:
            dict: Tipos de columna.
        """
        return self.column_types

    def get_preview_data(self, max_records=5):
        """
        Devuelve una vista previa de los datos procesados.

        Args:
            max_records (int): Número máximo de registros a devolver.

        Returns:
            list: Primeros N registros procesados.
        """
        return self.processed_data[:max_records] if self.processed_data else []
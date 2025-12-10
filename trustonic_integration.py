# trustonic_integration.py
"""
Módulo de integración con sistema Trustonic Trade-In.

Este módulo integra las funcionalidades del código del compañero con el proyecto BotLibertyBD:
- Manejo de zonas horarias por país (ISO 3166-1 alpha-2)
- Parseo robusto de fechas (12+ formatos)
- Limpieza de duplicados con reporte detallado
- Integración con tipos compuestos PostgreSQL
- Uso de psycopg v3

Autor: Migración desde código original del compañero
Fecha: 2025-12-10
"""

import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo
from logger import logger

# ============================================================================
# CONSTANTES: Mapeo de países a zonas horarias
# ============================================================================

# Mapeo de ISO 3166-1 alpha-2 a zona horaria principal
ZONAS_HORARIAS = {
    'CR': 'America/Costa_Rica',  # UTC-6 (Costa Rica)
    'PA': 'America/Panama',  # UTC-5 (Panamá)
    'CO': 'America/Bogota',  # UTC-5 (Colombia)
    'MX': 'America/Mexico_City',  # UTC-6 (México)
    'GT': 'America/Guatemala',  # UTC-6 (Guatemala)
    'HN': 'America/Tegucigalpa',  # UTC-6 (Honduras)
    'SV': 'America/El_Salvador',  # UTC-6 (El Salvador)
    'NI': 'America/Managua',  # UTC-6 (Nicaragua)
    'EC': 'America/Guayaquil',  # UTC-5 (Ecuador)
    'PE': 'America/Lima',  # UTC-5 (Perú)
    'CL': 'America/Santiago',  # UTC-4/-3 (Chile, con horario de verano)
    'AR': 'America/Argentina/Buenos_Aires',  # UTC-3 (Argentina)
    'BR': 'America/Sao_Paulo',  # UTC-3 (Brasil)
    'US': 'America/New_York',  # UTC-5/-4 (Estados Unidos - Este)
    'ES': 'Europe/Madrid',  # UTC+1/+2 (España)
}

# Zona horaria por defecto (usada cuando no se especifica país o el código no existe)
ZONA_HORARIA_DEFAULT = 'America/Costa_Rica'


# ============================================================================
# FUNCIONES DE ZONA HORARIA
# ============================================================================

def obtener_zona_horaria(iso3166_2: str | None = None) -> ZoneInfo:
    """
    Obtiene la zona horaria basada en el código ISO 3166-1 alpha-2.

    Args:
        iso3166_2: Código de país ISO 3166-1 alpha-2 (ej: 'CR', 'PA', 'CO')
                  Si es None o vacío, se usa la zona horaria por defecto.

    Returns:
        ZoneInfo: Objeto de zona horaria correspondiente al país.

    Examples:
        >>> obtener_zona_horaria('CR')
        ZoneInfo('America/Costa_Rica')

        >>> obtener_zona_horaria('PA')
        ZoneInfo('America/Panama')

        >>> obtener_zona_horaria(None)
        ZoneInfo('America/Costa_Rica')
    """
    if not iso3166_2:
        logger.info(f"Usando zona horaria por defecto: {ZONA_HORARIA_DEFAULT}")
        return ZoneInfo(ZONA_HORARIA_DEFAULT)

    # Normalizar: mayúsculas y quitar espacios
    iso3166_2 = iso3166_2.upper().strip()

    if iso3166_2 in ZONAS_HORARIAS:
        zona = ZONAS_HORARIAS[iso3166_2]
        logger.info(f"Zona horaria para {iso3166_2}: {zona}")
        return ZoneInfo(zona)

    # Si no se encuentra el código, usar default y advertir
    logger.warning(
        f"Código de país '{iso3166_2}' no encontrado en el mapeo. "
        f"Usando zona horaria por defecto: {ZONA_HORARIA_DEFAULT}"
    )
    return ZoneInfo(ZONA_HORARIA_DEFAULT)


def obtener_pais_por_zona(zona_horaria_str: str) -> str | None:
    """
    Busca el código de país correspondiente a una zona horaria.

    Args:
        zona_horaria_str: Nombre de la zona horaria (ej: 'America/Costa_Rica')

    Returns:
        Código ISO 3166-1 alpha-2 o None si no se encuentra

    Examples:
        >>> obtener_pais_por_zona('America/Costa_Rica')
        'CR'
    """
    for codigo, zona in ZONAS_HORARIAS.items():
        if zona == zona_horaria_str:
            return codigo
    return None


def listar_paises_disponibles() -> list[dict]:
    """
    Retorna lista de todos los países disponibles con sus zonas horarias.

    Returns:
        Lista de diccionarios con 'codigo', 'pais', 'zona_horaria'

    Examples:
        >>> paises = listar_paises_disponibles()
        >>> print(paises[0])
        {'codigo': 'CR', 'pais': 'Costa Rica', 'zona_horaria': 'America/Costa_Rica'}
    """
    # Mapeo de códigos a nombres (para la GUI)
    nombres_paises = {
        'CR': 'Costa Rica',
        'PA': 'Panamá',
        'CO': 'Colombia',
        'MX': 'México',
        'GT': 'Guatemala',
        'HN': 'Honduras',
        'SV': 'El Salvador',
        'NI': 'Nicaragua',
        'EC': 'Ecuador',
        'PE': 'Perú',
        'CL': 'Chile',
        'AR': 'Argentina',
        'BR': 'Brasil',
        'US': 'Estados Unidos',
        'ES': 'España',
    }

    return [
        {
            'codigo': codigo,
            'pais': nombres_paises.get(codigo, codigo),
            'zona_horaria': zona
        }
        for codigo, zona in sorted(ZONAS_HORARIAS.items())
    ]


# ============================================================================
# FUNCIONES DE PARSEO DE FECHAS
# ============================================================================

def parsear_fecha(valor, zona_horaria: ZoneInfo | None = None) -> datetime | None:
    """
    Parsea una fecha desde diferentes formatos de Excel y strings.

    Formatos soportados:
    - Objetos datetime/Timestamp (pandas)
    - Strings en múltiples formatos:
      * 6/17/2025 1:31:07 PM
      * 6/17/2025 13:31:07
      * 6/17/2025 13:31
      * 17/06/2025 13:31:07
      * 2025-06-17 13:31:07
      * 2025-06-17
      * Y más...

    Args:
        valor: Valor a parsear (puede ser datetime, string, pandas.Timestamp, etc.)
        zona_horaria: Zona horaria a aplicar si la fecha es naive (sin timezone)
                     Si es None, la fecha se retorna como naive.

    Returns:
        datetime con o sin timezone, o None si no se pudo parsear

    Examples:
        >>> from zoneinfo import ZoneInfo
        >>> parsear_fecha('6/17/2025 1:31:07 PM')
        datetime(2025, 6, 17, 13, 31, 7)

        >>> parsear_fecha('6/17/2025 1:31:07 PM', ZoneInfo('America/Costa_Rica'))
        datetime(2025, 6, 17, 13, 31, 7, tzinfo=ZoneInfo('America/Costa_Rica'))
    """
    # Caso 1: Valor nulo/vacío
    if pd.isna(valor):
        return None

    # Caso 2: Ya es datetime
    if isinstance(valor, datetime):
        fecha = valor
    # Caso 3: Es pandas.Timestamp
    elif isinstance(valor, pd.Timestamp):
        fecha = valor.to_pydatetime()
    # Caso 4: Es string, intentar parsear
    elif isinstance(valor, str):
        fecha = _parsear_fecha_string(valor.strip())
    else:
        # Tipo no soportado
        logger.warning(f"Tipo de dato no soportado para parseo de fecha: {type(valor)}")
        return None

    # Si no se pudo parsear, retornar None
    if fecha is None:
        return None

    # Aplicar zona horaria si es naive y se proporcionó zona_horaria
    if zona_horaria and fecha.tzinfo is None:
        fecha = fecha.replace(tzinfo=zona_horaria)

    return fecha


def _parsear_fecha_string(valor_str: str) -> datetime | None:
    """
    Parsea un string de fecha probando múltiples formatos.

    Args:
        valor_str: String a parsear (ya sin espacios extra)

    Returns:
        datetime naive o None si no se pudo parsear
    """
    # Lista de formatos a probar (en orden de más común a menos común)
    formatos = [
        # Formatos con hora y AM/PM
        '%m/%d/%Y %I:%M:%S %p',  # 6/17/2025 1:31:07 PM
        '%m/%d/%Y %I:%M %p',  # 6/17/2025 1:31 PM
        '%d/%m/%Y %I:%M:%S %p',  # 17/06/2025 1:31:07 PM
        '%d/%m/%Y %I:%M %p',  # 17/06/2025 1:31 PM

        # Formatos con hora 24h
        '%m/%d/%Y %H:%M:%S',  # 6/17/2025 13:31:07
        '%m/%d/%Y %H:%M',  # 6/17/2025 13:31
        '%d/%m/%Y %H:%M:%S',  # 17/06/2025 13:31:07
        '%d/%m/%Y %H:%M',  # 17/06/2025 13:31

        # Formato ISO
        '%Y-%m-%d %H:%M:%S',  # 2025-06-17 13:31:07
        '%Y-%m-%d %H:%M',  # 2025-06-17 13:31
        '%Y-%m-%dT%H:%M:%S',  # 2025-06-17T13:31:07 (ISO 8601)
        '%Y-%m-%dT%H:%M:%SZ',  # 2025-06-17T13:31:07Z (ISO 8601 UTC)

        # Solo fecha (sin hora)
        '%Y-%m-%d',  # 2025-06-17
        '%m/%d/%Y',  # 6/17/2025
        '%d/%m/%Y',  # 17/06/2025
    ]

    for formato in formatos:
        try:
            return datetime.strptime(valor_str, formato)
        except ValueError:
            continue

    # Si ningún formato funcionó, registrar advertencia
    logger.warning(f"No se pudo parsear la fecha: '{valor_str}' (probados {len(formatos)} formatos)")
    return None


# ============================================================================
# FUNCIONES DE LIMPIEZA DE DUPLICADOS
# ============================================================================

def limpiar_duplicados(registros: list[tuple]) -> tuple[list[tuple], list[dict]]:
    """
    Limpia duplicados del Excel basándose en el primer elemento (IMEI).
    Solo mantiene la primera ocurrencia de cada IMEI.

    Args:
        registros: Lista de tuplas (imei_serie, fecha_cliente)

    Returns:
        Tupla con:
        - Lista de registros limpios (sin duplicados)
        - Lista de reportes de duplicados encontrados

    Examples:
        >>> registros = [
        ...     ('123456', datetime(2024, 1, 1)),
        ...     ('123456', datetime(2024, 1, 2)),  # Duplicado
        ...     ('789012', datetime(2024, 1, 3)),
        ... ]
        >>> limpios, dupes = limpiar_duplicados(registros)
        >>> len(limpios)
        2
        >>> len(dupes)
        1
    """
    vistos: dict[str, int] = {}  # IMEI -> contador de ocurrencias
    registros_limpios = []
    reporte_duplicados = []

    for imei_serie, fecha in registros:
        # Saltar registros con IMEI None o vacío
        if imei_serie is None or (isinstance(imei_serie, str) and not imei_serie.strip()):
            logger.debug("Registro con IMEI vacío/None ignorado durante limpieza")
            continue

        # Normalizar IMEI (string y sin espacios)
        imei_str = str(imei_serie).strip()

        # Verificar si ya lo vimos
        contador = vistos.get(imei_str, 0)

        if contador > 0:
            # Es duplicado (ya se procesó antes)
            reporte_duplicados.append({
                'imei_serie': imei_str,
                'fecha': fecha,
                'ocurrencia': contador + 1,
                'motivo': f'Duplicado encontrado (ocurrencia #{contador + 1})'
            })
            logger.debug(f"Duplicado detectado: {imei_str} (ocurrencia #{contador + 1})")
        else:
            # Primera vez que vemos este IMEI
            registros_limpios.append((imei_str, fecha))

        # Incrementar contador
        vistos[imei_str] = contador + 1

    # Log final
    if reporte_duplicados:
        logger.warning(f"Duplicados encontrados durante limpieza: {len(reporte_duplicados)}")
        logger.info(f"Registros únicos después de limpieza: {len(registros_limpios)}")
    else:
        logger.info(f"No se encontraron duplicados. Total de registros únicos: {len(registros_limpios)}")

    return registros_limpios, reporte_duplicados


def limpiar_duplicados_de_diccionarios(registros: list[dict]) -> tuple[list[dict], list[dict]]:
    """
    Versión alternativa de limpiar_duplicados que trabaja con diccionarios.

    Args:
        registros: Lista de diccionarios con keys 'imei' y 'fecha_cliente'

    Returns:
        Tupla con:
        - Lista de registros limpios (sin duplicados)
        - Lista de reportes de duplicados encontrados

    Examples:
        >>> registros = [
        ...     {'imei': '123456', 'fecha_cliente': datetime(2024, 1, 1)},
        ...     {'imei': '123456', 'fecha_cliente': datetime(2024, 1, 2)},
        ...     {'imei': '789012', 'fecha_cliente': datetime(2024, 1, 3)},
        ... ]
        >>> limpios, dupes = limpiar_duplicados_de_diccionarios(registros)
        >>> len(limpios)
        2
        >>> len(dupes)
        1
    """
    vistos: dict[str, int] = {}
    registros_limpios = []
    reporte_duplicados = []

    for registro in registros:
        imei = registro.get('imei')
        fecha = registro.get('fecha_cliente')

        # Saltar registros con IMEI None o vacío
        if imei is None or (isinstance(imei, str) and not imei.strip()):
            continue

        # Normalizar IMEI
        imei_str = str(imei).strip()

        # Verificar si ya lo vimos
        contador = vistos.get(imei_str, 0)

        if contador > 0:
            # Es duplicado
            reporte_duplicados.append({
                'imei': imei_str,
                'fecha_cliente': fecha,
                'ocurrencia': contador + 1,
                'motivo': f'Duplicado encontrado (ocurrencia #{contador + 1})'
            })
        else:
            # Primera vez
            registros_limpios.append({
                'imei': imei_str,
                'fecha_cliente': fecha
            })

        # Incrementar contador
        vistos[imei_str] = contador + 1

    if reporte_duplicados:
        logger.warning(f"Duplicados encontrados: {len(reporte_duplicados)}")

    return registros_limpios, reporte_duplicados


# ============================================================================
# FUNCIONES DE LECTURA DE EXCEL (con nuevas capacidades)
# ============================================================================

def leer_excel_con_timezone(
        archivo: str,
        col_texto: str,
        col_fecha: str,
        iso3166_2: str | None = None,
        hoja: str | int = 0
) -> dict:
    """
    Lee Excel y retorna diccionario con datos procesados (con timezone).

    Args:
        archivo: Ruta al archivo Excel
        col_texto: Nombre de la columna de texto (IMEI/Serie)
        col_fecha: Nombre de la columna de fecha
        iso3166_2: Código ISO 3166-1 alpha-2 del país (default: usa zona default)
        hoja: Nombre o índice de la hoja (default: primera hoja)

    Returns:
        dict: {
            'success': bool,
            'data': lista de tuplas (imei, fecha_con_timezone),
            'total_rows': int,
            'duplicados': lista de diccionarios con duplicados,
            'duplicados_count': int,
            'zona_horaria': str,
            'error': str (si aplica)
        }
    """
    result = {
        'success': False,
        'data': [],
        'total_rows': 0,
        'duplicados': [],
        'duplicados_count': 0,
        'zona_horaria': None,
        'error': None
    }

    try:
        import openpyxl

        # Verificar que el archivo existe
        import os
        if not os.path.exists(archivo):
            result['error'] = f"Archivo no encontrado: {archivo}"
            logger.error(result['error'])
            return result

        logger.info(f"Leyendo archivo Excel: {archivo}")

        # Obtener zona horaria
        zona_horaria = obtener_zona_horaria(iso3166_2)
        result['zona_horaria'] = str(zona_horaria)

        # Leer Excel con pandas
        df = pd.read_excel(archivo, sheet_name=hoja)

        # Validar columnas
        if col_texto not in df.columns:
            result['error'] = f"Columna '{col_texto}' no encontrada en el Excel"
            logger.error(result['error'])
            return result

        if col_fecha not in df.columns:
            result['error'] = f"Columna '{col_fecha}' no encontrada en el Excel"
            logger.error(result['error'])
            return result

        # Procesar filas
        registros = []
        for idx, row in df.iterrows():
            texto = str(row[col_texto]).strip() if pd.notna(row[col_texto]) else None

            # Parsear fecha con timezone
            fecha = parsear_fecha(row[col_fecha], zona_horaria)

            # Solo agregar si tiene texto válido
            if texto:
                registros.append((texto, fecha))

        # Limpiar duplicados
        registros_limpios, duplicados = limpiar_duplicados(registros)

        # Guardar resultados
        result['data'] = registros_limpios
        result['total_rows'] = len(registros_limpios)
        result['duplicados'] = duplicados
        result['duplicados_count'] = len(duplicados)
        result['success'] = True

        logger.info(
            f"Excel procesado: {result['total_rows']} registros únicos, "
            f"{result['duplicados_count']} duplicados, zona horaria: {result['zona_horaria']}"
        )

    except ImportError as e:
        result['error'] = f"Librería faltante: {str(e)}"
        logger.error(result['error'])
    except Exception as e:
        result['error'] = f"Error al leer Excel: {str(e)}"
        logger.error(result['error'])
        import traceback
        logger.debug(traceback.format_exc())

    return result


# ============================================================================
# FUNCIONES DE VALIDACIÓN
# ============================================================================

def validar_imei(imei: str) -> bool:
    """
    Valida formato básico de IMEI (15 dígitos).

    Args:
        imei: String con el IMEI a validar

    Returns:
        bool: True si es válido, False en caso contrario

    Examples:
        >>> validar_imei('123456789012345')
        True

        >>> validar_imei('12345')
        False
    """
    if not imei or not isinstance(imei, str):
        return False

    # Limpiar espacios
    imei_limpio = imei.strip()

    # Verificar que tenga exactamente 15 dígitos
    if len(imei_limpio) != 15:
        return False

    # Verificar que solo contenga dígitos
    if not imei_limpio.isdigit():
        return False

    return True


def validar_serie(serie: str, min_length: int = 5, max_length: int = 50) -> bool:
    """
    Valida formato básico de número de serie.

    Args:
        serie: String con el número de serie a validar
        min_length: Longitud mínima permitida
        max_length: Longitud máxima permitida

    Returns:
        bool: True si es válido, False en caso contrario
    """
    if not serie or not isinstance(serie, str):
        return False

    # Limpiar espacios
    serie_limpia = serie.strip()

    # Verificar longitud
    if not (min_length <= len(serie_limpia) <= max_length):
        return False

    # Verificar que contenga caracteres alfanuméricos
    if not any(c.isalnum() for c in serie_limpia):
        return False

    return True


# ============================================================================
# INFORMACIÓN DEL MÓDULO
# ============================================================================

def obtener_info_modulo() -> dict:
    """
    Retorna información sobre el módulo y sus capacidades.

    Returns:
        dict con información del módulo
    """
    return {
        'nombre': 'trustonic_integration',
        'version': '1.0.0',
        'descripcion': 'Módulo de integración con sistema Trustonic Trade-In',
        'caracteristicas': [
            'Manejo de zonas horarias por país (ISO 3166-1 alpha-2)',
            'Parseo robusto de fechas (15+ formatos)',
            'Limpieza de duplicados con reporte detallado',
            'Validación de IMEIs y números de serie',
            'Integración con psycopg v3 y tipos compuestos',
        ],
        'paises_soportados': list(ZONAS_HORARIAS.keys()),
        'zona_horaria_default': ZONA_HORARIA_DEFAULT,
    }


# ============================================================================
# TESTING (para verificar funcionalidad)
# ============================================================================

if __name__ == "__main__":
    # Tests básicos del módulo
    print("=" * 70)
    print("TESTING: trustonic_integration.py")
    print("=" * 70)

    # Test 1: Zonas horarias
    print("\n1. Test de zonas horarias:")
    print(f"   CR: {obtener_zona_horaria('CR')}")
    print(f"   PA: {obtener_zona_horaria('PA')}")
    print(f"   None: {obtener_zona_horaria(None)}")
    print(f"   ZZ (inválido): {obtener_zona_horaria('ZZ')}")

    # Test 2: Parseo de fechas
    print("\n2. Test de parseo de fechas:")
    zona_cr = obtener_zona_horaria('CR')
    test_fechas = [
        '6/17/2025 1:31:07 PM',
        '6/17/2025 13:31',
        '2025-06-17',
        '17/06/2025 13:31:07',
    ]
    for fecha_str in test_fechas:
        parsed = parsear_fecha(fecha_str, zona_cr)
        print(f"   '{fecha_str}' -> {parsed}")

    # Test 3: Limpieza de duplicados
    print("\n3. Test de limpieza de duplicados:")
    registros_test = [
        ('123456', datetime(2024, 1, 1)),
        ('123456', datetime(2024, 1, 2)),  # Duplicado
        ('789012', datetime(2024, 1, 3)),
        ('789012', datetime(2024, 1, 4)),  # Duplicado
        ('456789', datetime(2024, 1, 5)),
    ]
    limpios, dupes = limpiar_duplicados(registros_test)
    print(f"   Registros originales: {len(registros_test)}")
    print(f"   Registros limpios: {len(limpios)}")
    print(f"   Duplicados encontrados: {len(dupes)}")

    # Test 4: Validaciones
    print("\n4. Test de validaciones:")
    print(f"   IMEI válido: {validar_imei('123456789012345')}")
    print(f"   IMEI inválido: {validar_imei('12345')}")
    print(f"   Serie válida: {validar_serie('ABC-123-XYZ')}")
    print(f"   Serie inválida: {validar_serie('AB')}")

    # Test 5: Info del módulo
    print("\n5. Información del módulo:")
    info = obtener_info_modulo()
    print(f"   Nombre: {info['nombre']}")
    print(f"   Versión: {info['version']}")
    print(f"   Países soportados: {len(info['paises_soportados'])}")

    print("\n" + "=" * 70)
    print("TESTS COMPLETADOS")
    print("=" * 70)

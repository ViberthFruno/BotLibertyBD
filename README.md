# BotLibertyBD

Bot automatizado para sincronización de IMEIs desde archivos Excel hacia base de datos PostgreSQL con notificaciones por correo electrónico.

## Lógica del Bot

### 🔥 IMPORTANTE: EL BOT NO ELIMINA NADA DE LA BASE DE DATOS - NUNCA

El bot procesa archivos Excel con información de IMEIs y sincroniza la base de datos **SIN ELIMINAR NINGÚN REGISTRO**. Los registros que ya no aparecen en el Excel se marcan como inactivos, pero permanecen en la base de datos.

### Flujo de Trabajo

```
1. Bot recibe correo electrónico con archivo Excel adjunto
                    ↓
2. Extrae IMEIs del Excel (Columna A: IMEI, Columna B: Fecha)
                    ↓
3. Sincroniza con la base de datos PostgreSQL
                    ↓
4. Compara Excel vs Base de Datos y ejecuta:
   - 📥 INSERT: IMEIs que están en Excel pero NO en BD (nuevos)
   - 🔄 UPDATE: IMEIs que están en Excel Y en BD (actualizar fecha)
   - 🚫 UPDATE activo=false: IMEIs que están en BD pero NO en Excel (desactivar)
                    ↓
5. Genera reporte PDF con estadísticas y detalles de cambios
                    ↓
6. Envía correo electrónico con PDF adjunto
```

### Casos de Sincronización

#### Caso 1: IMEIs Nuevos (INSERT)
- **Condición**: El IMEI está en el Excel pero NO existe en la base de datos
- **Acción**: Se inserta como nuevo registro con `activo=true`
- **Resultado**: Se agrega a la lista de "Nuevos" en el PDF

#### Caso 2: IMEIs Existentes (UPDATE)
- **Condición**: El IMEI está en el Excel Y en la base de datos
- **Acción**:
  - Si la fecha cambió → se actualiza la fecha y `activo=true`
  - Si estaba inactivo → se reactiva con `activo=true`
  - Si no hay cambios → se mantiene igual (sin cambios)
- **Resultado**: Se agrega a "Actualizados" o "Sin cambios" según corresponda

#### Caso 3: IMEIs Desactivados (UPDATE activo=false)
- **Condición**: El IMEI está en la BD pero NO aparece en el Excel recibido
- **Acción**: Se marca como `activo=false` (NO SE ELIMINA)
- **Resultado**: Se agrega a la lista de "Desactivados" en el PDF
- **Nota importante**: Estos registros NO se eliminan de la base de datos, solo se marcan como inactivos

### Estructura de la Base de Datos

**Tabla**: `automatizacion.datos_excel_doforms`

| Columna        | Tipo      | Descripción                                    |
|---------------|-----------|------------------------------------------------|
| imei_serie    | VARCHAR   | IMEI del dispositivo (clave primaria)         |
| fecha_cliente | TIMESTAMP | Fecha de registro del cliente                  |
| creado        | TIMESTAMP | Fecha de creación del registro                 |
| actualizado   | TIMESTAMP | Fecha de última actualización                  |
| activo        | BOOLEAN   | Estado del registro (true/false)               |
| detalle       | VARCHAR   | Detalle adicional (ej: 'traiding_trustonic')  |

### Reporte PDF

El bot genera un reporte PDF profesional con:

1. **Estadísticas generales**
   - Total de IMEIs procesados del Excel
   - Cantidad de nuevos, actualizados, desactivados y sin cambios
   - Porcentajes de cada categoría

2. **Gráfico visual de barras**
   - Representación visual de los cambios

3. **Detalles de registros (primeros 10 de cada categoría)**
   - 📥 Registros Nuevos: IMEI y fecha
   - 🔄 Registros Actualizados: IMEI, fecha nueva y fecha anterior
   - 🚫 Registros Desactivados: IMEIs en BD pero no en Excel
   - ✓ Sin cambios: IMEIs que no requirieron actualización

### Configuración

#### Requisitos
- Python 3.x
- PostgreSQL
- Cuenta de correo SMTP/IMAP

#### Dependencias
```bash
pip install -r requirements.txt
```

Las principales dependencias son:
- `psycopg2` - Conexión a PostgreSQL
- `openpyxl` - Lectura de archivos Excel
- `reportlab` - Generación de PDFs
- `matplotlib` - Gráficos para el PDF

#### Configuración de Correo
El bot monitorea una carpeta de correo IMAP en busca de correos con:
- Filtro de título específico
- Archivos Excel adjuntos (.xls o .xlsx)
- Solo correos de hoy que no han sido leídos

#### Configuración de PostgreSQL
- Schema: `automatizacion` (configurable)
- Tabla: `datos_excel_doforms` (configurable)
- La tabla se crea automáticamente si no existe

### Archivos Principales

#### `email_connector.py`
Maneja toda la lógica de correo electrónico:
- `monitor_and_notify()`: Monitorea correos y procesa adjuntos
- `extract_all_imeis_from_excel()`: Extrae IMEIs del Excel
- `generar_reporte_pdf()`: Genera el reporte PDF con estadísticas

#### `postgres_connector.py`
Maneja la conexión y sincronización con PostgreSQL:
- `sync_imeis()`: Sincroniza IMEIs entre Excel y BD
  - Retorna listas detalladas de nuevos, actualizados, desactivados
  - NO elimina registros, solo marca como inactivos

### Formato del Archivo Excel

El archivo Excel debe tener la siguiente estructura:

| Columna A (IMEI) | Columna B (Registered at) |
|------------------|---------------------------|
| 123456789012345  | 2024-01-15                |
| 234567890123456  | 2024-01-16                |
| ...              | ...                       |

- **Fila 1**: Encabezados (se omiten al procesar)
- **Fila 2+**: Datos
- **Columna A**: IMEI del dispositivo
- **Columna B**: Fecha de registro (formatos soportados: YYYY-MM-DD, DD/MM/YYYY, MM/DD/YYYY)

### Notificaciones por Correo

Cada vez que se procesa un archivo Excel, el bot envía un correo de notificación con:
- Resumen de procesamiento (nuevos, actualizados, desactivados, sin cambios)
- Archivo PDF adjunto con reporte detallado
- Timestamp de procesamiento

### Logs

El bot registra todas las operaciones en logs para auditoría:
- Conexiones exitosas/fallidas
- IMEIs procesados (nuevos, actualizados, desactivados)
- Errores durante la sincronización
- Generación de PDFs y envío de correos

### Seguridad

- El bot NO expone credenciales en logs
- Archivos temporales se eliminan después de procesar
- Solo procesa archivos Excel (.xls, .xlsx)
- Timeout de conexión configurado para evitar bloqueos

### Mantenimiento

#### Limpieza de registros inactivos
Los registros marcados como `activo=false` permanecen en la base de datos para auditoría. Si necesitas limpiarlos:

```sql
-- Ver cuántos registros inactivos hay
SELECT COUNT(*) FROM automatizacion.datos_excel_doforms WHERE activo = false;

-- Eliminar registros inactivos antiguos (opcional - solo si es necesario)
DELETE FROM automatizacion.datos_excel_doforms
WHERE activo = false
AND actualizado < NOW() - INTERVAL '90 days';
```

**Nota**: Solo elimina registros inactivos si realmente es necesario. Se recomienda mantenerlos para auditoría.

### Soporte

Para reportar problemas o solicitar nuevas funcionalidades, contacta al equipo de desarrollo.

---

**Última actualización**: 2024-12-05
**Versión**: 2.0 - Sincronización con listas detalladas y PDF mejorado

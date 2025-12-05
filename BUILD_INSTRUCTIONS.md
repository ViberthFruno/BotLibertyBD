# Instrucciones para Crear Ejecutable con PyInstaller

Este documento explica cómo empaquetar **EnlaceDB** en un ejecutable independiente usando PyInstaller.

## Requisitos Previos

1. **Python instalado** (versión 3.8 o superior recomendada)
2. **Todas las dependencias instaladas**:
   ```bash
   pip install -r requirements.txt
   ```
3. **PyInstaller instalado**:
   ```bash
   pip install pyinstaller
   ```

## Método 1: Usando el archivo .spec (Recomendado)

Este es el método más simple y está preconfigurado con todas las opciones necesarias:

```bash
pyinstaller EnlaceDB.spec
```

### ¿Qué hace este comando?

- Crea un ejecutable llamado `EnlaceDB.exe` (en Windows) o `EnlaceDB` (en Linux/Mac)
- **Incluye el icono** tanto en el ejecutable como dentro de la aplicación
- Incluye todas las dependencias necesarias
- El ejecutable se guardará en la carpeta `dist/`

## Método 2: Comando directo (Sin archivo .spec)

Si prefieres usar PyInstaller directamente sin el archivo .spec:

```bash
pyinstaller --onefile --windowed --icon=icon.ico --add-data "icon.ico;." --name EnlaceDB main.py
```

### Explicación de los parámetros:

- `--onefile`: Crea un único archivo ejecutable
- `--windowed`: No muestra la consola (para aplicaciones GUI)
- `--icon=icon.ico`: Define el icono del ejecutable
- `--add-data "icon.ico;."`: Incluye el icono dentro del paquete para que la aplicación lo use
  - En Linux/Mac use `:` en lugar de `;`: `--add-data "icon.ico:."`
- `--name EnlaceDB`: Nombre del ejecutable
- `main.py`: Archivo principal de entrada

## Ubicación del Ejecutable

Después de ejecutar PyInstaller:

1. El ejecutable estará en la carpeta `dist/`
2. Puedes distribuir solo el archivo `EnlaceDB.exe` (o `EnlaceDB` en Linux/Mac)
3. El icono ya está integrado, no necesitas distribuirlo por separado

## Verificación

Para verificar que el icono se cargó correctamente:

1. Ejecuta el archivo generado en `dist/EnlaceDB.exe`
2. Deberías ver el icono personalizado en:
   - La barra de tareas
   - La barra de título de la ventana
   - El archivo ejecutable en el explorador de archivos

## Solución de Problemas

### El icono no aparece en la aplicación

Verifica los logs de la aplicación para ver si hay algún error al cargar el icono.

### Errores de importación

Si PyInstaller no detecta alguna dependencia automáticamente, agrégala en el archivo `EnlaceDB.spec` en la sección `hiddenimports`.

### El ejecutable es muy grande

- Esto es normal con PyInstaller, ya que incluye Python y todas las dependencias
- El tamaño típico puede ser de 40-100 MB dependiendo de las librerías

### Antivirus bloquea el ejecutable

- Algunos antivirus pueden marcar ejecutables generados con PyInstaller como falsos positivos
- Considera firmar digitalmente el ejecutable para distribución profesional

## Distribución

Para distribuir tu aplicación:

1. Comparte solo el archivo de la carpeta `dist/`
2. No necesitas incluir Python ni las dependencias
3. El icono ya está integrado en el ejecutable

## Notas Adicionales

- El icono debe estar en formato `.ico` (Windows) para mejor compatibilidad
- Se recomienda usar iconos de al menos 256x256 píxeles
- El código ya está preparado para funcionar tanto en desarrollo como empaquetado

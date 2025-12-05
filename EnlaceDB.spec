# -*- mode: python ; coding: utf-8 -*-
"""
Archivo de configuración para PyInstaller - EnlaceDB
=====================================================

Este archivo configura cómo PyInstaller empaquetará la aplicación EnlaceDB.

Para crear el ejecutable, ejecute:
    pyinstaller EnlaceDB.spec

El ejecutable se generará en la carpeta 'dist/'
"""

block_cipher = None

a = Analysis(
    ['main.py'],
    pathex=[],
    binaries=[],
    datas=[
        # Incluir el icono dentro del paquete para que la aplicación lo use
        ('icon.ico', '.'),
    ],
    hiddenimports=[
        # Dependencias que PyInstaller podría no detectar automáticamente
        'psycopg2',
        'pandas',
        'customtkinter',
        'openpyxl',
        'email',
        'imaplib',
        'smtplib',
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='EnlaceDB',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,  # False para no mostrar consola (aplicación GUI)
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    # Configurar el icono del ejecutable
    icon='icon.ico',
)

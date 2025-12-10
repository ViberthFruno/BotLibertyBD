# postgres_connector.py
"""
Conector de PostgreSQL para la aplicación EnlaceDB.

⚠️ MIGRADO A PSYCOPG V3 - Sistema Trustonic Trade-In

Este archivo proporciona funcionalidades para conectarse a una base de datos PostgreSQL,
ejecutar consultas y gestionar las conexiones de manera segura con manejo de errores
apropiado y registros detallados, incluyendo soporte para:
- Diferentes configuraciones de SSL
- Tipos compuestos de PostgreSQL
- Funciones PL/pgSQL
- Compatibilidad con código legacy (psycopg2)

Versión: 2.0.0 (psycopg v3)
Fecha: 2025-12-10

# email_connector.py

import os
import re
import smtplib
import imaplib
import email
import tempfile
import unicodedata
import socket
from datetime import datetime
from logger import logger
from email.mime.base import MIMEBase
from email import encoders


class EmailConnector:
    """Conector genérico para servicios de correo mediante SMTP e IMAP."""

    def __init__(self, smtp_server, smtp_port, imap_server, imap_port,
                 email_address, password, use_tls=True):
        self.smtp_server = smtp_server
        self.smtp_port = int(smtp_port)
        self.imap_server = imap_server
        self.imap_port = int(imap_port)
        self.email_address = email_address
        self.password = password
        self.use_tls = use_tls
        # Configurar timeout por defecto para sockets
        socket.setdefaulttimeout(30)

    def is_available(self):
        """Verifica si las dependencias básicas están disponibles."""
        return True  # smtplib e imaplib son parte de la biblioteca estándar

    def test_connection(self):
        """Prueba la conexión SMTP con las credenciales proporcionadas."""
        try:
            with smtplib.SMTP(self.smtp_server, self.smtp_port, timeout=10) as server:
                if self.use_tls:
                    server.starttls()
                server.login(self.email_address, self.password)
            return True, "Conexión SMTP exitosa"
        except Exception as e:
            logger.error(f"Error en conexión SMTP: {e}")
            return False, str(e)

    def load_folders(self, callback=None):
        """Obtiene la lista de carpetas disponibles mediante IMAP."""
        folders = []
        imap = None
        try:
            # Agregar timeout de 30 segundos para evitar bloqueos indefinidos
            imap = imaplib.IMAP4_SSL(self.imap_server, self.imap_port, timeout=30)
            imap.login(self.email_address, self.password)
            typ, data = imap.list()
            if typ == 'OK':
                for line in data:
                    decoded = line.decode()
                    parts = decoded.split(' "/" ')
                    if len(parts) == 2:
                        folder = parts[1].strip('"')
                        folders.append(folder)
                        if callback:
                            callback(f"Bandeja encontrada: {folder}")
        except Exception as e:
            if callback:
                callback(f"Error al cargar carpetas: {e}", "ERROR")
            logger.error(f"Error al cargar carpetas IMAP: {e}")
        finally:
            if imap:
                try:
                    imap.logout()
                except:
                    pass
        return folders

    @staticmethod
    def _normalize_text(value):
        normalized = unicodedata.normalize('NFKD', value or '')
        return ''.join(ch for ch in normalized if not unicodedata.combining(ch)).lower()

    @classmethod
    def _prepare_title_filters(cls, title_filter):
        if not title_filter:
            return []

        filters = []
        for raw_group in re.split(r'[;,|]+', title_filter):
            tokens = [cls._normalize_text(token) for token in raw_group.split() if token]
            if tokens:
                filters.append(tokens)
        return filters

    @classmethod
    def _subject_matches(cls, subject, prepared_filters):
        if not prepared_filters:
            return True

        normalized_subject = cls._normalize_text(subject)
        for tokens in prepared_filters:
            if all(token in normalized_subject for token in tokens):
                return True
        return False

    def search_emails_and_download_excel(self, folder_path, title_filter,
                                         today_only=True, status_callback=None,
                                         result_callback=None, max_emails_to_check=50,
                                         max_matches=10):
        """
        Busca correos por título y descarga adjuntos de Excel.
        OPTIMIZADO: Procesa correos uno a uno con límites para evitar consumo excesivo.

        Args:
            folder_path: Carpeta de correo a buscar
            title_filter: Filtro de título para buscar correos
            today_only: Si True, solo busca correos de hoy (default: True)
            status_callback: Callback para reportar estado
            result_callback: Callback para reportar resultados
            max_emails_to_check: Máximo de correos a revisar (default: 50)
            max_matches: Máximo de coincidencias a procesar (default: 10)
        """
        results = {
            "success": False,
            "total_items": 0,
            "matching_items": 0,
            "excel_files": [],
            "errors": [],
            "temp_dir": None,
            "message": ""
        }

        folder_path = (folder_path or "INBOX").strip() or "INBOX"
        prepared_filters = self._prepare_title_filters(title_filter)

        temp_dir = tempfile.mkdtemp(prefix="enlace_db_excel_")
        results["temp_dir"] = temp_dir

        imap = None
        try:
            # Agregar timeout de 30 segundos
            imap = imaplib.IMAP4_SSL(self.imap_server, self.imap_port, timeout=30)
            imap.login(self.email_address, self.password)
            imap.select(folder_path)

            date_str = datetime.now().strftime('%d-%b-%Y')
            search_criteria = ['ON', date_str] if today_only else ['ALL']

            typ, data = imap.search(None, *search_criteria)
            if typ != 'OK':
                results["message"] = "Error en búsqueda IMAP"
                return results

            email_ids = data[0].split()
            total_emails = len(email_ids)

            if not email_ids:
                results["success"] = True
                results["message"] = "No se encontraron correos"
                return results

            # OPTIMIZACIÓN: Limitar cantidad de correos a revisar
            emails_to_process = email_ids[:max_emails_to_check]

            if status_callback:
                if total_emails > max_emails_to_check:
                    status_callback(f"Encontrados {total_emails} correos. Procesando los primeros {max_emails_to_check}...", "INFO")
                else:
                    status_callback(f"Encontrados {total_emails} correos. Procesando...", "INFO")

            # Procesar cada correo UNO A UNO
            emails_checked = 0
            for num in emails_to_process:
                emails_checked += 1

                # OPTIMIZACIÓN: Detener si ya encontramos suficientes coincidencias
                if results["matching_items"] >= max_matches:
                    if status_callback:
                        status_callback(f"Límite de {max_matches} coincidencias alcanzado. Deteniendo búsqueda.", "INFO")
                    break

                # Leer solo el encabezado para obtener el asunto sin marcar como leído
                typ, header_data = imap.fetch(num, '(BODY.PEEK[HEADER.FIELDS (SUBJECT)])')
                if typ != 'OK':
                    continue

                header_msg = email.message_from_bytes(header_data[0][1])
                subject = header_msg.get('Subject', '')

                # Verificar si el asunto coincide con el filtro
                if not self._subject_matches(subject, prepared_filters):
                    # No coincide, continuar con el siguiente
                    if status_callback and emails_checked % 10 == 0:
                        status_callback(f"Revisados {emails_checked} correos...", "INFO")
                    continue

                # COINCIDENCIA ENCONTRADA - Descargar el mensaje completo
                typ, msg_data = imap.fetch(num, '(BODY.PEEK[])')
                if typ != 'OK':
                    continue

                msg = email.message_from_bytes(msg_data[0][1])
                results["matching_items"] += 1

                if status_callback:
                    status_callback(f"✓ Coincidencia #{results['matching_items']}: '{subject}'", "SUCCESS")

                # Buscar adjuntos Excel
                for part in msg.walk():
                    if part.get_content_disposition() == 'attachment':
                        filename = part.get_filename()
                        if filename and filename.lower().endswith(('.xls', '.xlsx')):
                            filepath = os.path.join(temp_dir, filename)
                            with open(filepath, 'wb') as f:
                                f.write(part.get_payload(decode=True))
                            results["excel_files"].append(filepath)
                            if status_callback:
                                status_callback(f"Descargado: {filename}", "SUCCESS")

                # Marcar como leído después de procesar
                imap.store(num, '+FLAGS', '\\Seen')

            results["total_items"] = emails_checked
        except socket.timeout:
            results["errors"].append("Timeout de conexión IMAP")
            if status_callback:
                status_callback("Error: Timeout de conexión IMAP", "ERROR")
            logger.error("Timeout de conexión IMAP")
        except Exception as e:
            results["errors"].append(str(e))
            if status_callback:
                status_callback(f"Error: {e}", "ERROR")
            logger.error(f"Error en búsqueda IMAP: {e}")
        finally:
            if imap:
                try:
                    imap.logout()
                except:
                    pass

        if results["errors"]:
            return results

        results["success"] = True
        results["message"] = f"{results['matching_items']} correos coincidentes"
        return results

    def send_simple_email(self, to_email, subject, body):
        """Envía un correo simple a un destinatario."""
        try:
            from email.mime.text import MIMEText
            from email.mime.multipart import MIMEMultipart

            msg = MIMEMultipart()
            msg['From'] = self.email_address
            msg['To'] = to_email
            msg['Subject'] = subject

            msg.attach(MIMEText(body, 'plain'))

            with smtplib.SMTP(self.smtp_server, self.smtp_port, timeout=10) as server:
                if self.use_tls:
                    server.starttls()
                server.login(self.email_address, self.password)
                server.send_message(msg)

            logger.info(f"Correo enviado a {to_email}")
            return True, "Correo enviado exitosamente"

        except socket.timeout:
            error_msg = f"Timeout al enviar correo a {to_email}"
            logger.error(error_msg)
            return False, error_msg
        except Exception as e:
            logger.error(f"Error al enviar correo a {to_email}: {e}")
            return False, str(e)

    def send_email_with_attachment(self, to_email, subject, body, attachment_path):
        """
        Envía un correo con un archivo adjunto.

        Args:
            to_email: Destinatario del correo
            subject: Asunto del correo
            body: Cuerpo del mensaje
            attachment_path: Ruta completa del archivo a adjuntar

        Returns:
            tuple: (success, message)
        """
        try:
            from email.mime.text import MIMEText
            from email.mime.multipart import MIMEMultipart

            msg = MIMEMultipart()
            msg['From'] = self.email_address
            msg['To'] = to_email
            msg['Subject'] = subject

            # Adjuntar cuerpo del mensaje
            msg.attach(MIMEText(body, 'plain'))

            # Adjuntar archivo
            if attachment_path and os.path.exists(attachment_path):
                filename = os.path.basename(attachment_path)

                with open(attachment_path, 'rb') as attachment:
                    part = MIMEBase('application', 'octet-stream')
                    part.set_payload(attachment.read())

                encoders.encode_base64(part)
                part.add_header(
                    'Content-Disposition',
                    f'attachment; filename= {filename}'
                )
                msg.attach(part)

                logger.info(f"Archivo adjunto agregado: {filename}")

            # Enviar correo
            with smtplib.SMTP(self.smtp_server, self.smtp_port, timeout=10) as server:
                if self.use_tls:
                    server.starttls()
                server.login(self.email_address, self.password)
                server.send_message(msg)

            logger.info(f"Correo con adjunto enviado a {to_email}")
            return True, "Correo con adjunto enviado exitosamente"

        except socket.timeout:
            error_msg = f"Timeout al enviar correo a {to_email}"
            logger.error(error_msg)
            return False, error_msg
        except Exception as e:
            logger.error(f"Error al enviar correo con adjunto a {to_email}: {e}")
            return False, str(e)

    @staticmethod
    def extract_excel_data(excel_path, row=1, col_g='G', col_h='H'):
        """
        Extrae datos de un archivo Excel en posiciones específicas.

        Args:
            excel_path: Ruta del archivo Excel
            row: Número de fila (default: 1)
            col_g: Columna para el primer dato (default: 'G')
            col_h: Columna para el segundo dato (default: 'H')

        Returns:
            dict: {
                'success': bool,
                'data_g': valor de columna G,
                'data_h': valor de columna H,
                'error': mensaje de error (si aplica)
            }
        """
        result = {
            'success': False,
            'data_g': None,
            'data_h': None,
            'error': None
        }

        try:
            import openpyxl

            # Verificar que el archivo existe
            if not os.path.exists(excel_path):
                result['error'] = f"Archivo no encontrado: {excel_path}"
                logger.error(result['error'])
                return result

            # Abrir el archivo Excel
            workbook = openpyxl.load_workbook(excel_path, data_only=True)
            sheet = workbook.active

            # Extraer datos de las columnas G y H en la fila especificada
            cell_g = f"{col_g}{row}"
            cell_h = f"{col_h}{row}"

            value_g = sheet[cell_g].value
            value_h = sheet[cell_h].value

            result['data_g'] = value_g
            result['data_h'] = value_h
            result['success'] = True

            workbook.close()

            logger.info(f"Datos extraídos del Excel: G{row}={value_g}, H{row}={value_h}")

        except ImportError:
            result['error'] = "La librería 'openpyxl' no está instalada. Ejecute: pip install openpyxl"
            logger.error(result['error'])
        except Exception as e:
            result['error'] = f"Error al extraer datos del Excel: {str(e)}"
            logger.error(result['error'])

        return result

    @staticmethod
    def create_text_file_with_data(data_g, data_h, excel_filename, output_dir=None):
        """
        Crea un archivo de texto con los datos extraídos del Excel.

        Args:
            data_g: Dato de la columna G
            data_h: Dato de la columna H
            excel_filename: Nombre del archivo Excel procesado
            output_dir: Directorio donde guardar el archivo (default: temp)

        Returns:
            tuple: (success, file_path, error_message)
        """
        try:
            # Definir directorio de salida
            if output_dir is None:
                output_dir = tempfile.gettempdir()

            # Crear nombre de archivo con timestamp
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"datos_extraidos_{timestamp}.txt"
            filepath = os.path.join(output_dir, filename)

            # Crear contenido del archivo
            content = f"""=== DATOS EXTRAÍDOS DEL EXCEL ===
Fecha de extracción: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

Columna G (Fila 1): {data_g if data_g is not None else 'Sin datos'}
Columna H (Fila 1): {data_h if data_h is not None else 'Sin datos'}

Archivo procesado: {excel_filename}

---
Generado automáticamente por BotLibertyBD
"""

            # Escribir archivo
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(content)

            logger.info(f"Archivo de texto creado: {filepath}")
            return True, filepath, None

        except Exception as e:
            error_msg = f"Error al crear archivo de texto: {str(e)}"
            logger.error(error_msg)
            return False, None, error_msg

    def monitor_and_notify(self, title_filter, notify_emails, folder_path="INBOX",
                          status_callback=None, max_emails_to_check=50, max_matches=5):
        """
        Monitorea correos no leídos con un título específico, los marca como leídos
        y envía notificaciones a los usuarios especificados.
        OPTIMIZADO: Procesa correos uno a uno con límites para evitar consumo excesivo.

        Args:
            title_filter: Filtro de título para buscar correos
            notify_emails: Lista de emails a notificar
            folder_path: Carpeta de correo a monitorear (default: INBOX)
            status_callback: Callback para reportar estado
            max_emails_to_check: Máximo de correos a revisar por ciclo (default: 50)
            max_matches: Máximo de coincidencias a procesar por ciclo (default: 5)
        """
        results = {
            "success": False,
            "total_items": 0,
            "matching_items": 0,
            "notified_users": 0,
            "errors": [],
            "message": ""
        }

        folder_path = (folder_path or "INBOX").strip() or "INBOX"
        prepared_filters = self._prepare_title_filters(title_filter)

        imap = None
        try:
            # Agregar timeout de 30 segundos para evitar bloqueos indefinidos
            imap = imaplib.IMAP4_SSL(self.imap_server, self.imap_port, timeout=30)
            imap.login(self.email_address, self.password)
            imap.select(folder_path)

            # Buscar solo correos NO LEÍDOS de hoy
            date_str = datetime.now().strftime('%d-%b-%Y')
            typ, data = imap.search(None, 'UNSEEN', 'ON', date_str)

            if typ != 'OK':
                results["message"] = "Error en búsqueda IMAP"
                return results

            email_ids = data[0].split()
            total_unread = len(email_ids)

            if not email_ids:
                results["success"] = True
                results["message"] = "No hay correos nuevos"
                return results

            # OPTIMIZACIÓN: Limitar cantidad de correos a revisar
            emails_to_process = email_ids[:max_emails_to_check]

            if status_callback:
                if total_unread > max_emails_to_check:
                    status_callback(f"Encontrados {total_unread} correos no leídos. Procesando los primeros {max_emails_to_check}...", "INFO")
                else:
                    status_callback(f"Encontrados {total_unread} correos no leídos. Procesando...", "INFO")

            # Procesar cada correo UNO A UNO
            emails_checked = 0
            for num in emails_to_process:
                emails_checked += 1

                # OPTIMIZACIÓN: Detener si ya encontramos suficientes coincidencias
                if results["matching_items"] >= max_matches:
                    if status_callback:
                        status_callback(f"Límite de {max_matches} coincidencias alcanzado. Deteniendo búsqueda.", "INFO")
                    break

                # Leer encabezado para obtener asunto (uno a uno)
                typ, header_data = imap.fetch(num, '(BODY.PEEK[HEADER.FIELDS (SUBJECT FROM)])')
                if typ != 'OK':
                    continue

                header_msg = email.message_from_bytes(header_data[0][1])
                subject = header_msg.get('Subject', '')
                from_email = header_msg.get('From', '')

                # Verificar si el asunto coincide con el filtro
                if not self._subject_matches(subject, prepared_filters):
                    # No coincide, continuar con el siguiente
                    if status_callback and emails_checked % 10 == 0:
                        status_callback(f"Revisados {emails_checked} correos...", "INFO")
                    continue

                # COINCIDENCIA ENCONTRADA
                results["matching_items"] += 1

                if status_callback:
                    status_callback(f"✓ Coincidencia #{results['matching_items']}: '{subject}' de {from_email}", "SUCCESS")

                # Descargar el mensaje completo para obtener adjuntos
                typ, full_msg_data = imap.fetch(num, '(RFC822)')
                if typ != 'OK':
                    if status_callback:
                        status_callback("Error al descargar mensaje completo", "WARNING")
                    continue

                full_msg = email.message_from_bytes(full_msg_data[0][1])

                # Variables para almacenar archivos temporales
                excel_files = []
                text_file_path = None
                temp_dir = tempfile.mkdtemp(prefix="bot_liberty_")

                try:
                    # Buscar y descargar adjuntos Excel
                    for part in full_msg.walk():
                        if part.get_content_disposition() == 'attachment':
                            filename = part.get_filename()
                            if filename and filename.lower().endswith(('.xls', '.xlsx')):
                                filepath = os.path.join(temp_dir, filename)
                                with open(filepath, 'wb') as f:
                                    f.write(part.get_payload(decode=True))
                                excel_files.append(filepath)
                                if status_callback:
                                    status_callback(f"📎 Excel descargado: {filename}", "INFO")

                    # Procesar el primer archivo Excel encontrado
                    if excel_files:
                        excel_path = excel_files[0]
                        excel_filename = os.path.basename(excel_path)

                        if status_callback:
                            status_callback(f"📊 Extrayendo datos del Excel...", "INFO")

                        # Extraer datos de columnas G y H
                        extraction_result = self.extract_excel_data(excel_path)

                        if extraction_result['success']:
                            data_g = extraction_result['data_g']
                            data_h = extraction_result['data_h']

                            if status_callback:
                                status_callback(f"✓ Datos extraídos - G1: {data_g}, H1: {data_h}", "SUCCESS")

                            # Crear archivo de texto con los datos
                            success, text_file_path, error_msg = self.create_text_file_with_data(
                                data_g, data_h, excel_filename, temp_dir
                            )

                            if success and status_callback:
                                status_callback(f"✓ Archivo de texto creado", "SUCCESS")
                        else:
                            if status_callback:
                                status_callback(f"⚠ Error al extraer datos: {extraction_result['error']}", "WARNING")

                    # Marcar como leído después de procesar
                    imap.store(num, '+FLAGS', '\\Seen')

                    # Enviar notificación a cada usuario
                    for notify_email in notify_emails:
                        notification_subject = "Correo Detectado - BotLibertyBD"

                        # Preparar cuerpo del mensaje
                        notification_body = f"""Se ha detectado un nuevo correo que coincide con sus criterios de búsqueda.

Asunto del correo: {subject}
Remitente: {from_email}
Fecha de detección: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

El correo ha sido marcado como leído automáticamente."""

                        if excel_files:
                            notification_body += f"\n\n📎 Se encontró un archivo Excel adjunto: {os.path.basename(excel_files[0])}"
                            if text_file_path:
                                notification_body += f"\n📄 Los datos extraídos (columnas G y H) se adjuntan en este correo."

                        notification_body += "\n\n---\nEste es un mensaje automático de BotLibertyBD."

                        # Enviar correo con o sin adjunto
                        if text_file_path and os.path.exists(text_file_path):
                            success, message = self.send_email_with_attachment(
                                notify_email,
                                notification_subject,
                                notification_body,
                                text_file_path
                            )
                        else:
                            success, message = self.send_simple_email(
                                notify_email,
                                notification_subject,
                                notification_body
                            )

                        if success:
                            results["notified_users"] += 1
                            if status_callback:
                                status_callback(f"✉ Notificación enviada a {notify_email}", "SUCCESS")
                        else:
                            error_msg = f"Error al notificar a {notify_email}: {message}"
                            results["errors"].append(error_msg)
                            if status_callback:
                                status_callback(error_msg, "ERROR")

                finally:
                    # Limpiar archivos temporales
                    try:
                        import shutil
                        if os.path.exists(temp_dir):
                            shutil.rmtree(temp_dir)
                            if status_callback:
                                status_callback(f"🗑 Archivos temporales eliminados", "INFO")
                    except Exception as e:
                        logger.warning(f"No se pudieron eliminar archivos temporales: {e}")

            results["success"] = True
            results["total_items"] = emails_checked
            results["message"] = f"{results['matching_items']} correo(s) detectado(s), {results['notified_users']} notificación(es) enviada(s)"

        except socket.timeout:
            error_msg = "Timeout de conexión IMAP en monitoreo"
            results["errors"].append(error_msg)
            results["message"] = error_msg
            if status_callback:
                status_callback(error_msg, "ERROR")
            logger.error(error_msg)
        except Exception as e:
            error_msg = f"Error en monitoreo: {str(e)}"
            results["errors"].append(error_msg)
            results["message"] = error_msg
            if status_callback:
                status_callback(error_msg, "ERROR")
            logger.error(error_msg)
        finally:
            if imap:
                try:
                    imap.logout()
                except:
                    pass

        return results

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
                                         result_callback=None):
        """Busca correos por título y descarga adjuntos de Excel."""
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

            for num in data[0].split():
                # Leer solo el encabezado para obtener el asunto sin marcar como leído
                typ, header_data = imap.fetch(num, '(BODY.PEEK[HEADER.FIELDS (SUBJECT)])')
                if typ != 'OK':
                    continue
                results["total_items"] += 1
                header_msg = email.message_from_bytes(header_data[0][1])
                subject = header_msg.get('Subject', '')
                if not self._subject_matches(subject, prepared_filters):
                    continue

                # Descargar el mensaje completo sin marcar como leído
                typ, msg_data = imap.fetch(num, '(BODY.PEEK[])')
                if typ != 'OK':
                    continue
                msg = email.message_from_bytes(msg_data[0][1])
                results["matching_items"] += 1

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

    def monitor_and_notify(self, title_filter, notify_emails, folder_path="INBOX",
                          status_callback=None):
        """
        Monitorea correos no leídos con un título específico, los marca como leídos
        y envía notificaciones a los usuarios especificados.
        OPTIMIZADO: Usa timeouts y mejor manejo de errores.
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
            results["total_items"] = len(email_ids)

            if not email_ids:
                results["success"] = True
                results["message"] = "No hay correos nuevos"
                return results

            # Procesar cada correo
            for num in email_ids:
                # Leer encabezado para obtener asunto
                typ, header_data = imap.fetch(num, '(BODY.PEEK[HEADER.FIELDS (SUBJECT FROM)])')
                if typ != 'OK':
                    continue

                header_msg = email.message_from_bytes(header_data[0][1])
                subject = header_msg.get('Subject', '')
                from_email = header_msg.get('From', '')

                # Verificar si el asunto coincide con el filtro
                if not self._subject_matches(subject, prepared_filters):
                    continue

                results["matching_items"] += 1

                # Marcar como leído
                imap.store(num, '+FLAGS', '\\Seen')

                if status_callback:
                    status_callback(f"Correo detectado: '{subject}' de {from_email}", "INFO")

                # Enviar notificación a cada usuario
                for notify_email in notify_emails:
                    notification_subject = "Correo Detectado - BotLibertyBD"
                    notification_body = f"""Se ha detectado un nuevo correo que coincide con sus criterios de búsqueda.

Asunto del correo: {subject}
Remitente: {from_email}
Fecha de detección: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

El correo ha sido marcado como leído automáticamente.

---
Este es un mensaje automático de BotLibertyBD."""

                    success, message = self.send_simple_email(
                        notify_email,
                        notification_subject,
                        notification_body
                    )

                    if success:
                        results["notified_users"] += 1
                        if status_callback:
                            status_callback(f"Notificación enviada a {notify_email}", "SUCCESS")
                    else:
                        error_msg = f"Error al notificar a {notify_email}: {message}"
                        results["errors"].append(error_msg)
                        if status_callback:
                            status_callback(error_msg, "ERROR")

            results["success"] = True
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

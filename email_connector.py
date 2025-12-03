import os
import re
import smtplib
import imaplib
import email
import tempfile
import unicodedata
from datetime import datetime
from logger import logger


class EmailConnector:
    """Conector genérico para servicios de correo mediante SMTP e IMAP."""

    def __init__(self, smtp_server, smtp_port, imap_server, imap_port,
                 email_address, password, use_tls=True):
        # Limpiar espacios no separables y espacios en blanco
        self.smtp_server = str(smtp_server).replace('\xa0', ' ').strip()
        self.smtp_port = int(smtp_port)
        self.imap_server = str(imap_server).replace('\xa0', ' ').strip()
        self.imap_port = int(imap_port)
        self.email_address = str(email_address).replace('\xa0', ' ').strip()
        self.password = str(password).replace('\xa0', ' ').strip()
        self.use_tls = use_tls

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
        try:
            with imaplib.IMAP4_SSL(self.imap_server, self.imap_port) as imap:
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

        try:
            with imaplib.IMAP4_SSL(self.imap_server, self.imap_port) as imap:
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
        except Exception as e:
            results["errors"].append(str(e))
            if status_callback:
                status_callback(f"Error: {e}", "ERROR")
            logger.error(f"Error en búsqueda IMAP: {e}")
            return results

        results["success"] = True
        results["message"] = f"{results['matching_items']} correos coincidentes"
        return results

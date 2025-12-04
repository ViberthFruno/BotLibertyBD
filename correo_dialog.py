# correo_dialog.py

import tkinter as tk
from tkinter import ttk, messagebox
import threading
from email_connector import EmailConnector

PROVIDERS = {
    "Gmail": {
        "smtp": ("smtp.gmail.com", 587),
        "imap": ("imap.gmail.com", 993),
    },
    "Outlook": {
        "smtp": ("smtp-mail.outlook.com", 587),
        "imap": ("outlook.office365.com", 993),
    },
    "Yahoo": {
        "smtp": ("smtp.mail.yahoo.com", 587),
        "imap": ("imap.mail.yahoo.com", 993),
    },
    "iCloud": {
        "smtp": ("smtp.mail.me.com", 587),
        "imap": ("imap.mail.me.com", 993),
    },
    "Hotmail": {
        "smtp": ("smtp-mail.outlook.com", 587),
        "imap": ("outlook.office365.com", 993),
    }
}


class CorreoDialog(tk.Toplevel):
    """Diálogo para configurar conexión de correo (SMTP/IMAP)."""

    def __init__(self, parent, existing_config=None):
        super().__init__(parent)
        self.parent = parent
        self.existing_config = existing_config or {}
        self.result = None

        self._setup_window()
        self._create_widgets()
        self._apply_existing_config()

    def _setup_window(self):
        self.title("Configuración de Correo")
        self.geometry("420x250")
        self.resizable(False, False)
        self.grab_set()

    def _create_widgets(self):
        self.main_frame = ttk.Frame(self, padding=15)
        self.main_frame.pack(fill=tk.BOTH, expand=True)

        # Provider selection
        provider_frame = ttk.Frame(self.main_frame)
        provider_frame.pack(fill=tk.X, pady=5)
        ttk.Label(provider_frame, text="Proveedor:", width=15).pack(side=tk.LEFT)
        self.provider_var = tk.StringVar()
        self.provider_combo = ttk.Combobox(provider_frame, textvariable=self.provider_var,
                                           values=list(PROVIDERS.keys()) + ["Otro"], state="readonly")
        self.provider_combo.pack(side=tk.LEFT, fill=tk.X, expand=True)
        self.provider_combo.bind('<<ComboboxSelected>>', self._on_provider_selected)

        # Email and password
        email_frame = ttk.Frame(self.main_frame)
        email_frame.pack(fill=tk.X, pady=5)
        ttk.Label(email_frame, text="Correo:", width=15).pack(side=tk.LEFT)
        self.email_entry = ttk.Entry(email_frame, width=30)
        self.email_entry.pack(side=tk.LEFT, fill=tk.X, expand=True)

        pass_frame = ttk.Frame(self.main_frame)
        pass_frame.pack(fill=tk.X, pady=5)
        ttk.Label(pass_frame, text="Contraseña:", width=15).pack(side=tk.LEFT)
        self.pass_entry = ttk.Entry(pass_frame, show="*", width=30)
        self.pass_entry.pack(side=tk.LEFT, fill=tk.X, expand=True)

        # Advanced settings frame (initially hidden)
        self.advanced_frame = ttk.LabelFrame(self.main_frame, text="Configuración Avanzada", padding=10)

        # SMTP server
        smtp_frame = ttk.Frame(self.advanced_frame)
        smtp_frame.pack(fill=tk.X, pady=5)
        ttk.Label(smtp_frame, text="SMTP Servidor:", width=15).pack(side=tk.LEFT)
        self.smtp_entry = ttk.Entry(smtp_frame, width=30)
        self.smtp_entry.pack(side=tk.LEFT, fill=tk.X, expand=True)

        smtp_port_frame = ttk.Frame(self.advanced_frame)
        smtp_port_frame.pack(fill=tk.X, pady=5)
        ttk.Label(smtp_port_frame, text="SMTP Puerto:", width=15).pack(side=tk.LEFT)
        self.smtp_port_entry = ttk.Entry(smtp_port_frame, width=30)
        self.smtp_port_entry.pack(side=tk.LEFT, fill=tk.X, expand=True)

        # IMAP server
        imap_frame = ttk.Frame(self.advanced_frame)
        imap_frame.pack(fill=tk.X, pady=5)
        ttk.Label(imap_frame, text="IMAP Servidor:", width=15).pack(side=tk.LEFT)
        self.imap_entry = ttk.Entry(imap_frame, width=30)
        self.imap_entry.pack(side=tk.LEFT, fill=tk.X, expand=True)

        imap_port_frame = ttk.Frame(self.advanced_frame)
        imap_port_frame.pack(fill=tk.X, pady=5)
        ttk.Label(imap_port_frame, text="IMAP Puerto:", width=15).pack(side=tk.LEFT)
        self.imap_port_entry = ttk.Entry(imap_port_frame, width=30)
        self.imap_port_entry.pack(side=tk.LEFT, fill=tk.X, expand=True)

        # Buttons
        button_frame = ttk.Frame(self.main_frame)
        button_frame.pack(fill=tk.X, pady=10)

        ttk.Button(button_frame, text="Probar Conexión",
                   command=self.test_connection).pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="Guardar",
                   command=self.save).pack(side=tk.RIGHT, padx=5)
        ttk.Button(button_frame, text="Cancelar",
                   command=self.cancel).pack(side=tk.RIGHT, padx=5)

    def _apply_existing_config(self):
        if not self.existing_config:
            return

        # Detectar proveedor basado en configuración existente
        smtp_server = self.existing_config.get("smtp_server", "")
        imap_server = self.existing_config.get("imap_server", "")
        detected_provider = None

        for provider_name, config in PROVIDERS.items():
            if config["smtp"][0] == smtp_server and config["imap"][0] == imap_server:
                detected_provider = provider_name
                break

        if detected_provider:
            self.provider_var.set(detected_provider)
        else:
            self.provider_var.set("Otro")

        self.email_entry.insert(0, self.existing_config.get("email", ""))
        self.pass_entry.insert(0, self.existing_config.get("password", ""))
        self.smtp_entry.insert(0, smtp_server)
        self.smtp_port_entry.insert(0, str(self.existing_config.get("smtp_port", "")))
        self.imap_entry.insert(0, imap_server)
        self.imap_port_entry.insert(0, str(self.existing_config.get("imap_port", "")))

        # Mostrar campos avanzados si es necesario
        self._toggle_advanced_settings()

    def _toggle_advanced_settings(self):
        """Muestra u oculta los campos avanzados según el proveedor seleccionado."""
        provider = self.provider_var.get()

        if provider == "Otro":
            # Mostrar campos avanzados
            self.advanced_frame.pack(fill=tk.X, pady=10, before=self.main_frame.winfo_children()[-1])
            self.geometry("420x460")
        else:
            # Ocultar campos avanzados
            self.advanced_frame.pack_forget()
            self.geometry("420x250")

    def _on_provider_selected(self, event):
        provider = self.provider_var.get()
        config = PROVIDERS.get(provider)

        if config:
            # Autocompletar configuración del proveedor
            smtp_server, smtp_port = config["smtp"]
            imap_server, imap_port = config["imap"]
            self.smtp_entry.delete(0, tk.END)
            self.smtp_entry.insert(0, smtp_server)
            self.smtp_port_entry.delete(0, tk.END)
            self.smtp_port_entry.insert(0, smtp_port)
            self.imap_entry.delete(0, tk.END)
            self.imap_entry.insert(0, imap_server)
            self.imap_port_entry.delete(0, tk.END)
            self.imap_port_entry.insert(0, imap_port)

        # Mostrar u ocultar campos avanzados
        self._toggle_advanced_settings()

    def _gather_config(self):
        return {
            "email": self.email_entry.get().strip(),
            "password": self.pass_entry.get(),
            "smtp_server": self.smtp_entry.get().strip(),
            "smtp_port": int(self.smtp_port_entry.get().strip() or 0),
            "imap_server": self.imap_entry.get().strip(),
            "imap_port": int(self.imap_port_entry.get().strip() or 0),
        }

    def test_connection(self):
        """Prueba la conexión de correo en un hilo separado para no bloquear la UI."""
        config = self._gather_config()

        # Crear un diálogo de progreso
        progress_dialog = tk.Toplevel(self)
        progress_dialog.title("Probando Conexión")
        progress_dialog.geometry("300x100")
        progress_dialog.transient(self)
        progress_dialog.grab_set()

        ttk.Label(progress_dialog, text="Probando conexión SMTP...", font=("Arial", 10)).pack(pady=20)
        progress_bar = ttk.Progressbar(progress_dialog, mode='indeterminate')
        progress_bar.pack(pady=10, padx=20, fill=tk.X)
        progress_bar.start()

        def run_test():
            """Ejecuta el test de conexión en un hilo separado."""
            try:
                connector = EmailConnector(
                    smtp_server=config["smtp_server"],
                    smtp_port=config["smtp_port"],
                    imap_server=config["imap_server"],
                    imap_port=config["imap_port"],
                    email_address=config["email"],
                    password=config["password"],
                )
                success, message = connector.test_connection()

                # Actualizar UI en el hilo principal
                self.after(0, lambda: self._show_test_result(success, message, progress_dialog))
            except Exception as e:
                self.after(0, lambda: self._show_test_result(False, str(e), progress_dialog))

        # Iniciar el test en un hilo separado
        test_thread = threading.Thread(target=run_test, daemon=True)
        test_thread.start()

    def _show_test_result(self, success, message, progress_dialog):
        """Muestra el resultado del test de conexión en el hilo principal."""
        try:
            progress_dialog.destroy()
        except:
            pass

        if success:
            messagebox.showinfo("Éxito", message)
        else:
            messagebox.showerror("Error", message)

    def save(self):
        self.result = self._gather_config()
        self.destroy()

    def cancel(self):
        self.destroy()

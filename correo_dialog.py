import tkinter as tk
from tkinter import ttk, messagebox
from email_connector import EmailConnector

PROVIDERS = {
    "Gmail": {
        "smtp": ("smtp.gmail.com", 587),
        "imap": ("imap.gmail.com", 993),
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
        self.geometry("420x420")
        self.resizable(False, False)
        self.grab_set()

    def _create_widgets(self):
        frame = ttk.Frame(self, padding=15)
        frame.pack(fill=tk.BOTH, expand=True)

        # Provider selection
        provider_frame = ttk.Frame(frame)
        provider_frame.pack(fill=tk.X, pady=5)
        ttk.Label(provider_frame, text="Proveedor:", width=15).pack(side=tk.LEFT)
        self.provider_var = tk.StringVar()
        self.provider_combo = ttk.Combobox(provider_frame, textvariable=self.provider_var,
                                           values=list(PROVIDERS.keys()) + ["Otro"], state="readonly")
        self.provider_combo.pack(side=tk.LEFT, fill=tk.X, expand=True)
        self.provider_combo.bind('<<ComboboxSelected>>', self._on_provider_selected)

        # Email and password
        email_frame = ttk.Frame(frame)
        email_frame.pack(fill=tk.X, pady=5)
        ttk.Label(email_frame, text="Correo:", width=15).pack(side=tk.LEFT)
        self.email_entry = ttk.Entry(email_frame, width=30)
        self.email_entry.pack(side=tk.LEFT, fill=tk.X, expand=True)

        pass_frame = ttk.Frame(frame)
        pass_frame.pack(fill=tk.X, pady=5)
        ttk.Label(pass_frame, text="Contraseña:", width=15).pack(side=tk.LEFT)
        self.pass_entry = ttk.Entry(pass_frame, show="*", width=30)
        self.pass_entry.pack(side=tk.LEFT, fill=tk.X, expand=True)

        # SMTP server
        smtp_frame = ttk.Frame(frame)
        smtp_frame.pack(fill=tk.X, pady=5)
        ttk.Label(smtp_frame, text="SMTP Servidor:", width=15).pack(side=tk.LEFT)
        self.smtp_entry = ttk.Entry(smtp_frame, width=30)
        self.smtp_entry.pack(side=tk.LEFT, fill=tk.X, expand=True)

        smtp_port_frame = ttk.Frame(frame)
        smtp_port_frame.pack(fill=tk.X, pady=5)
        ttk.Label(smtp_port_frame, text="SMTP Puerto:", width=15).pack(side=tk.LEFT)
        self.smtp_port_entry = ttk.Entry(smtp_port_frame, width=30)
        self.smtp_port_entry.pack(side=tk.LEFT, fill=tk.X, expand=True)

        # IMAP server
        imap_frame = ttk.Frame(frame)
        imap_frame.pack(fill=tk.X, pady=5)
        ttk.Label(imap_frame, text="IMAP Servidor:", width=15).pack(side=tk.LEFT)
        self.imap_entry = ttk.Entry(imap_frame, width=30)
        self.imap_entry.pack(side=tk.LEFT, fill=tk.X, expand=True)

        imap_port_frame = ttk.Frame(frame)
        imap_port_frame.pack(fill=tk.X, pady=5)
        ttk.Label(imap_port_frame, text="IMAP Puerto:", width=15).pack(side=tk.LEFT)
        self.imap_port_entry = ttk.Entry(imap_port_frame, width=30)
        self.imap_port_entry.pack(side=tk.LEFT, fill=tk.X, expand=True)

        # Buttons
        button_frame = ttk.Frame(frame)
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
        self.email_entry.insert(0, self.existing_config.get("email", ""))
        self.pass_entry.insert(0, self.existing_config.get("password", ""))
        self.smtp_entry.insert(0, self.existing_config.get("smtp_server", ""))
        self.smtp_port_entry.insert(0, str(self.existing_config.get("smtp_port", "")))
        self.imap_entry.insert(0, self.existing_config.get("imap_server", ""))
        self.imap_port_entry.insert(0, str(self.existing_config.get("imap_port", "")))

    def _on_provider_selected(self, event):
        provider = self.provider_var.get()
        config = PROVIDERS.get(provider)
        if config:
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
        config = self._gather_config()
        connector = EmailConnector(
            smtp_server=config["smtp_server"],
            smtp_port=config["smtp_port"],
            imap_server=config["imap_server"],
            imap_port=config["imap_port"],
            email_address=config["email"],
            password=config["password"],
        )
        success, message = connector.test_connection()
        if success:
            messagebox.showinfo("Éxito", message)
        else:
            messagebox.showerror("Error", message)

    def save(self):
        self.result = self._gather_config()
        self.destroy()

    def cancel(self):
        self.destroy()

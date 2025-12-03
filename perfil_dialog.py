"""Dialogo para crear o editar perfiles de automatización usando Tkinter."""

import tkinter as tk
from tkinter import ttk, messagebox


class PerfilDialog(tk.Toplevel):
    """Ventana de diálogo para configurar perfiles de automatización."""

    def __init__(self, parent, default_folder="INBOX", existing_data=None):
        super().__init__(parent)
        self.parent = parent
        self.default_folder = default_folder
        self.existing_data = existing_data or {}
        self.result = None

        self.title("Configurar Perfil de Automatización")
        self.geometry("480x400")
        self.resizable(False, False)
        self.grab_set()

        self._create_widgets()
        if self.existing_data:
            self._apply_existing_data()

        self.update_idletasks()
        w = self.winfo_width()
        h = self.winfo_height()
        x = (self.winfo_screenwidth() // 2) - (w // 2)
        y = (self.winfo_screenheight() // 2) - (h // 2)
        self.geometry(f"{w}x{h}+{x}+{y}")

    def _create_widgets(self):
        frame = ttk.Frame(self, padding=20)
        frame.pack(fill=tk.BOTH, expand=True)

        ttk.Label(frame, text="Configuración de Perfil", font=("TkDefaultFont", 14, "bold")).pack(pady=(0, 10))

        # Nombre del perfil
        name_frame = ttk.Frame(frame)
        name_frame.pack(fill=tk.X, pady=5)
        ttk.Label(name_frame, text="Nombre del perfil:", width=20).pack(side=tk.LEFT)
        self.name_entry = ttk.Entry(name_frame)
        self.name_entry.pack(side=tk.LEFT, fill=tk.X, expand=True)

        # Bandeja de correo (siempre bandeja de entrada)
        folder_frame = ttk.Frame(frame)
        folder_frame.pack(fill=tk.X, pady=5)
        ttk.Label(folder_frame, text="Bandeja de correo:", width=20).pack(side=tk.LEFT)
        inbox_text = f"Bandeja de entrada ({self.default_folder})"
        ttk.Label(folder_frame, text=inbox_text, width=30, anchor="w").pack(side=tk.LEFT, fill=tk.X, expand=True)

        # Filtro de título
        filter_frame = ttk.Frame(frame)
        filter_frame.pack(fill=tk.X, pady=5)
        ttk.Label(filter_frame, text="Buscar título:", width=20).pack(side=tk.LEFT)
        self.filter_entry = ttk.Entry(filter_frame)
        self.filter_entry.pack(side=tk.LEFT, fill=tk.X, expand=True)

        # Hora programada
        time_frame = ttk.Frame(frame)
        time_frame.pack(fill=tk.X, pady=5)
        ttk.Label(time_frame, text="Hora programada:", width=20).pack(side=tk.LEFT)

        self.hour_var = tk.StringVar(value="12")
        self.hour_spin = ttk.Spinbox(time_frame, from_=0, to=23, width=5,
                                     textvariable=self.hour_var, wrap=True, format="%02.0f")
        self.hour_spin.pack(side=tk.LEFT)

        ttk.Label(time_frame, text=":").pack(side=tk.LEFT)

        self.minute_var = tk.StringVar(value="00")
        self.minute_spin = ttk.Spinbox(time_frame, from_=0, to=59, increment=5, width=5,
                                       textvariable=self.minute_var, wrap=True, format="%02.0f")
        self.minute_spin.pack(side=tk.LEFT)

        # Habilitar perfil
        enable_frame = ttk.Frame(frame)
        enable_frame.pack(fill=tk.X, pady=5)
        self.enable_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(enable_frame, text="Habilitar este perfil", variable=self.enable_var).pack(anchor=tk.W)

        # Descripción
        desc = (
            "Este perfil ejecutará la búsqueda automáticamente en la bandeja de entrada "
            "a la hora programada y procesará los archivos Excel adjuntos según el título especificado."
        )
        ttk.Label(frame, text=desc, wraplength=440, justify="left").pack(pady=(10, 0))

        # Botones
        button_frame = ttk.Frame(frame)
        button_frame.pack(fill=tk.X, pady=(20, 0))
        ttk.Button(button_frame, text="Cancelar", command=self.cancel).pack(side=tk.RIGHT, padx=5)
        ttk.Button(button_frame, text="Guardar", command=self.save).pack(side=tk.RIGHT, padx=5)

    def _apply_existing_data(self):
        self.name_entry.insert(0, self.existing_data.get("name", ""))
        self.filter_entry.insert(0, self.existing_data.get("title_filter", ""))
        self.hour_var.set(f"{self.existing_data.get('hour', 12):02d}")
        self.minute_var.set(f"{self.existing_data.get('minute', 0):02d}")
        self.enable_var.set(self.existing_data.get("enabled", True))

    def save(self):
        name = self.name_entry.get().strip()
        if not name:
            messagebox.showwarning("Datos Incompletos", "Debe proporcionar un nombre para el perfil.")
            return

        folder = self.default_folder or "INBOX"

        self.result = {
            "name": name,
            "folder_path": folder,
            "title_filter": self.filter_entry.get().strip(),
            "hour": int(self.hour_var.get()),
            "minute": int(self.minute_var.get()),
            "enabled": self.enable_var.get(),
            "today_only": True,
            "last_run": None,
        }
        self.destroy()

    def cancel(self):
        self.result = None
        self.destroy()


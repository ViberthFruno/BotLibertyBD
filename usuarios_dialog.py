# usuarios_dialog.py

import tkinter as tk
from tkinter import ttk, messagebox
import re


class UsuariosDialog(tk.Toplevel):
    """Diálogo para configurar usuarios a notificar."""

    def __init__(self, parent, existing_users=None):
        super().__init__(parent)
        self.parent = parent
        self.existing_users = existing_users or []
        self.result = None

        # Lista interna de correos
        self.users = self.existing_users.copy()

        self._setup_window()
        self._create_widgets()
        self._populate_list()

    def _setup_window(self):
        self.title("Usuarios a Notificar")
        self.geometry("500x400")
        self.resizable(False, False)
        self.grab_set()

    def _create_widgets(self):
        main_frame = ttk.Frame(self, padding=15)
        main_frame.pack(fill=tk.BOTH, expand=True)

        # Título
        title_label = ttk.Label(
            main_frame,
            text="Correos Electrónicos a Notificar",
            font=("Arial", 12, "bold")
        )
        title_label.pack(pady=(0, 10))

        # Instrucciones
        instructions = ttk.Label(
            main_frame,
            text="Cuando se detecte un correo, se enviará una notificación simple ('correo detectado') a estos usuarios.",
            wraplength=450,
            justify=tk.LEFT
        )
        instructions.pack(pady=(0, 10))

        # Frame para entrada
        input_frame = ttk.Frame(main_frame)
        input_frame.pack(fill=tk.X, pady=(0, 10))

        ttk.Label(input_frame, text="Correo:").pack(side=tk.LEFT, padx=(0, 5))
        self.email_entry = ttk.Entry(input_frame, width=35)
        self.email_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 5))
        self.email_entry.bind('<Return>', lambda e: self._add_user())

        add_button = ttk.Button(input_frame, text="Agregar", command=self._add_user)
        add_button.pack(side=tk.LEFT)

        # Frame para lista
        list_frame = ttk.LabelFrame(main_frame, text="Usuarios configurados", padding=10)
        list_frame.pack(fill=tk.BOTH, expand=True, pady=(0, 10))

        # Listbox con scrollbar
        list_container = ttk.Frame(list_frame)
        list_container.pack(fill=tk.BOTH, expand=True)

        scrollbar = ttk.Scrollbar(list_container)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        self.users_listbox = tk.Listbox(
            list_container,
            yscrollcommand=scrollbar.set,
            selectmode=tk.SINGLE,
            height=10
        )
        self.users_listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.config(command=self.users_listbox.yview)

        # Botones de lista
        list_buttons = ttk.Frame(list_frame)
        list_buttons.pack(fill=tk.X, pady=(5, 0))

        ttk.Button(list_buttons, text="Eliminar Seleccionado", command=self._remove_user).pack(side=tk.LEFT, padx=5)
        ttk.Button(list_buttons, text="Limpiar Todo", command=self._clear_all).pack(side=tk.LEFT, padx=5)

        # Botones de acción
        button_frame = ttk.Frame(main_frame)
        button_frame.pack(fill=tk.X)

        ttk.Button(button_frame, text="Guardar", command=self._save).pack(side=tk.RIGHT, padx=5)
        ttk.Button(button_frame, text="Cancelar", command=self._cancel).pack(side=tk.RIGHT, padx=5)

    def _populate_list(self):
        """Puebla el listbox con los usuarios existentes."""
        self.users_listbox.delete(0, tk.END)
        for email in self.users:
            self.users_listbox.insert(tk.END, email)

    def _validate_email(self, email):
        """Valida que el correo electrónico tenga un formato básico válido."""
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return re.match(pattern, email) is not None

    def _add_user(self):
        """Agrega un nuevo usuario a la lista."""
        email = self.email_entry.get().strip()

        if not email:
            messagebox.showwarning("Campo Vacío", "Por favor, ingrese un correo electrónico.")
            return

        if not self._validate_email(email):
            messagebox.showwarning("Correo Inválido", "Por favor, ingrese un correo electrónico válido.")
            return

        if email in self.users:
            messagebox.showwarning("Correo Duplicado", "Este correo ya existe en la lista.")
            return

        self.users.append(email)
        self.users_listbox.insert(tk.END, email)
        self.email_entry.delete(0, tk.END)
        self.email_entry.focus()

    def _remove_user(self):
        """Elimina el usuario seleccionado."""
        selection = self.users_listbox.curselection()

        if not selection:
            messagebox.showwarning("Sin Selección", "Por favor, seleccione un correo para eliminar.")
            return

        index = selection[0]
        email = self.users[index]

        if messagebox.askyesno("Confirmar", f"¿Desea eliminar el correo:\n\n'{email}'?"):
            self.users.pop(index)
            self.users_listbox.delete(index)

    def _clear_all(self):
        """Limpia todos los usuarios."""
        if not self.users:
            messagebox.showinfo("Lista Vacía", "No hay usuarios para eliminar.")
            return

        if messagebox.askyesno("Confirmar", "¿Desea eliminar todos los usuarios de la lista?"):
            self.users.clear()
            self.users_listbox.delete(0, tk.END)

    def _save(self):
        """Guarda la configuración."""
        self.result = self.users.copy()
        self.destroy()

    def _cancel(self):
        """Cancela la configuración."""
        self.result = None
        self.destroy()

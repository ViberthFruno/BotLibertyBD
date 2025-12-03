import tkinter as tk
from tkinter import ttk, messagebox


class ParametrosDialog(tk.Toplevel):
    """Diálogo para configurar parámetros de búsqueda de correos."""

    def __init__(self, parent, existing_params=None):
        super().__init__(parent)
        self.parent = parent
        self.existing_params = existing_params or {"titles": []}
        self.result = None

        # Lista interna de títulos
        self.titles = self.existing_params.get("titles", []).copy()

        self._setup_window()
        self._create_widgets()
        self._populate_list()

    def _setup_window(self):
        self.title("Parámetros de Búsqueda")
        self.geometry("500x400")
        self.resizable(False, False)
        self.grab_set()

    def _create_widgets(self):
        main_frame = ttk.Frame(self, padding=15)
        main_frame.pack(fill=tk.BOTH, expand=True)

        # Título
        title_label = ttk.Label(
            main_frame,
            text="Títulos de Correo a Buscar",
            font=("Arial", 12, "bold")
        )
        title_label.pack(pady=(0, 10))

        # Instrucciones
        instructions = ttk.Label(
            main_frame,
            text="El bot buscará correos que contengan estos títulos en su asunto y los marcará como leídos.",
            wraplength=450,
            justify=tk.LEFT
        )
        instructions.pack(pady=(0, 10))

        # Frame para entrada
        input_frame = ttk.Frame(main_frame)
        input_frame.pack(fill=tk.X, pady=(0, 10))

        ttk.Label(input_frame, text="Nuevo título:").pack(side=tk.LEFT, padx=(0, 5))
        self.title_entry = ttk.Entry(input_frame, width=35)
        self.title_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 5))
        self.title_entry.bind('<Return>', lambda e: self._add_title())

        add_button = ttk.Button(input_frame, text="Agregar", command=self._add_title)
        add_button.pack(side=tk.LEFT)

        # Frame para lista
        list_frame = ttk.LabelFrame(main_frame, text="Títulos configurados", padding=10)
        list_frame.pack(fill=tk.BOTH, expand=True, pady=(0, 10))

        # Listbox con scrollbar
        list_container = ttk.Frame(list_frame)
        list_container.pack(fill=tk.BOTH, expand=True)

        scrollbar = ttk.Scrollbar(list_container)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        self.titles_listbox = tk.Listbox(
            list_container,
            yscrollcommand=scrollbar.set,
            selectmode=tk.SINGLE,
            height=10
        )
        self.titles_listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.config(command=self.titles_listbox.yview)

        # Botones de lista
        list_buttons = ttk.Frame(list_frame)
        list_buttons.pack(fill=tk.X, pady=(5, 0))

        ttk.Button(list_buttons, text="Eliminar Seleccionado", command=self._remove_title).pack(side=tk.LEFT, padx=5)
        ttk.Button(list_buttons, text="Limpiar Todo", command=self._clear_all).pack(side=tk.LEFT, padx=5)

        # Botones de acción
        button_frame = ttk.Frame(main_frame)
        button_frame.pack(fill=tk.X)

        ttk.Button(button_frame, text="Guardar", command=self._save).pack(side=tk.RIGHT, padx=5)
        ttk.Button(button_frame, text="Cancelar", command=self._cancel).pack(side=tk.RIGHT, padx=5)

    def _populate_list(self):
        """Puebla el listbox con los títulos existentes."""
        self.titles_listbox.delete(0, tk.END)
        for title in self.titles:
            self.titles_listbox.insert(tk.END, title)

    def _add_title(self):
        """Agrega un nuevo título a la lista."""
        title = self.title_entry.get().strip()

        if not title:
            messagebox.showwarning("Campo Vacío", "Por favor, ingrese un título.")
            return

        if title in self.titles:
            messagebox.showwarning("Título Duplicado", "Este título ya existe en la lista.")
            return

        self.titles.append(title)
        self.titles_listbox.insert(tk.END, title)
        self.title_entry.delete(0, tk.END)
        self.title_entry.focus()

    def _remove_title(self):
        """Elimina el título seleccionado."""
        selection = self.titles_listbox.curselection()

        if not selection:
            messagebox.showwarning("Sin Selección", "Por favor, seleccione un título para eliminar.")
            return

        index = selection[0]
        title = self.titles[index]

        if messagebox.askyesno("Confirmar", f"¿Desea eliminar el título:\n\n'{title}'?"):
            self.titles.pop(index)
            self.titles_listbox.delete(index)

    def _clear_all(self):
        """Limpia todos los títulos."""
        if not self.titles:
            messagebox.showinfo("Lista Vacía", "No hay títulos para eliminar.")
            return

        if messagebox.askyesno("Confirmar", "¿Desea eliminar todos los títulos de la lista?"):
            self.titles.clear()
            self.titles_listbox.delete(0, tk.END)

    def _save(self):
        """Guarda la configuración."""
        self.result = {"titles": self.titles}
        self.destroy()

    def _cancel(self):
        """Cancela la configuración."""
        self.result = None
        self.destroy()

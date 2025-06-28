import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from datetime import datetime

MKK_LIST = ["", "МКК1", "МКК2", "МКК3", "МКК4", "МКК5"]

class FilePairInput(tk.Frame):
    def __init__(self, master, index, MKK_LIST):
        super().__init__(master)
        self.index = index

        tk.Label(self, text=f"Пара #{index + 1}").grid(row=0, column=0, columnspan=3, pady=5)

        tk.Label(self, text="Введите дату заявки (ДД.ММ.ГГГГ):").grid(row=1, column=0, sticky="w")
        self.date_entry = tk.Entry(self)
        self.date_entry.grid(row=1, column=1, padx=5)

        tk.Label(self, text="Файл КО:").grid(row=2, column=0, sticky="w")
        self.ko_path_var = tk.StringVar()
        self.ko_entry = tk.Entry(self, textvariable=self.ko_path_var, width=40, state="readonly")
        self.ko_entry.grid(row=2, column=1)
        self.ko_button = tk.Button(self, text="Выбрать", command=self.select_ko_file)
        self.ko_button.grid(row=2, column=2, padx=5)

        tk.Label(self, text="Файл ССП:").grid(row=3, column=0, sticky="w")
        self.ssp_path_var = tk.StringVar()
        self.ssp_entry = tk.Entry(self, textvariable=self.ssp_path_var, width=40, state="readonly")
        self.ssp_entry.grid(row=3, column=1)
        self.ssp_button = tk.Button(self, text="Выбрать", command=self.select_ssp_file)
        self.ssp_button.grid(row=3, column=2, padx=5)

        tk.Label(self, text="Выберите МКК:").grid(row=4, column=0, sticky="w")
        self.mkk_var = tk.StringVar()
        self.mkk_combo = ttk.Combobox(self, textvariable=self.mkk_var, values=MKK_LIST, state="readonly")
        self.mkk_combo.grid(row=4, column=1, padx=5, sticky="w")

        # self.mkk_combo.set(MKK_LIST[0] if MKK_LIST else "")

        self.grid_columnconfigure(1, weight=1)

    def select_ko_file(self):
        path = filedialog.askopenfilename(title="Выберите файл КО", filetypes=[("XML файлы", "*.xml"), ("Все файлы", "*.*")])
        if path:
            self.ko_path_var.set(path)

    def select_ssp_file(self):
        path = filedialog.askopenfilename(title="Выберите файл ССП", filetypes=[("XML файлы", "*.xml"), ("Все файлы", "*.*")])
        if path:
            self.ssp_path_var.set(path)

    def get_data(self):
        return {
            "date_request": self.date_entry.get().strip(),
            "ko_path": self.ko_path_var.get(),
            "ssp_path": self.ssp_path_var.get(),
            "mkk": self.mkk_var.get().strip()
        }

class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Парсер КО + ССП")
        self.geometry("700x700")

        self.blocks = []
        for i in range(5):
            block = FilePairInput(self, i, MKK_LIST)
            block.pack(fill="x", padx=10, pady=5)
            self.blocks.append(block)

        self.run_button = tk.Button(self, text="Запустить парсинг", command=self.run_parsing)
        self.run_button.pack(pady=15)

        self.result = None  # сюда попадут данные заполненных блоков после успешного запуска

    def run_parsing(self):
        all_data = []
        for i, block in enumerate(self.blocks):
            data = block.get_data()

            date = data["date_request"]
            ko = data["ko_path"]
            ssp = data["ssp_path"]
            mkk = data["mkk"]

            # Все поля пусты — пропускаем блок
            if not date and not ko and not ssp and not mkk:
                continue

            # Если частично заполнен — ошибка
            if not (date and ko and ssp and mkk):
                messagebox.showerror("Ошибка", f"Блок #{i+1} заполнен не полностью. Пожалуйста, заполните все поля или оставьте блок пустым.")
                return

            # Проверка формата даты
            try:
                datetime.strptime(date, "%d.%m.%Y")
            except ValueError:
                messagebox.showerror("Ошибка", f"Неверный формат даты заявки в блоке #{i+1}, используйте ДД.ММ.ГГГГ")
                return
            
            all_data.append(data)

        if not all_data:
            messagebox.showwarning("Внимание", "Нет заполненных блоков для обработки.")
            return

        self.result = all_data
        self.destroy()  # Закрываем окно

        # Здесь запускаем обработку всех пар (пример)
        for entry in all_data:
            print(f"Дата заявки: {entry['date_request']}")
            print(f"Файл КО: {entry['ko_path']}")
            print(f"Файл ССП: {entry['ssp_path']}")
            print(f"МКК: {entry['mkk']}")
            print("----")

        messagebox.showinfo("Готово", "Все пары успешно считаны. Далее можно запускать парсинг.")

if __name__ == "__main__":
    app = App()
    app.mainloop()
import tkinter as tk
from tkinter import simpledialog, filedialog, messagebox
import os
import re
import pandas as pd

def ask_date_request() -> 'pd.Timestamp':
    """Запрашивает дату заявки через диалоговое окно, формат ДД.ММ.ГГГГ."""
    root = tk.Tk()
    root.withdraw()
    while True:
        date_str = simpledialog.askstring("Дата заявки", "Введите дату заявки (ДД.ММ.ГГГГ):")
        if date_str is None:
            messagebox.showerror("Ошибка", "Дата заявки не указана.")
            continue
        try:
            import pandas as pd
            date = pd.to_datetime(date_str, format="%d.%m.%Y", errors="raise")
            root.destroy()
            return date
        except Exception:
            messagebox.showerror("Ошибка", "Неверный формат даты. Используйте ДД.ММ.ГГГГ.")

def select_file(title):
    path = filedialog.askopenfilename(title=title, filetypes=[("XML files", "*.xml"), ("All files", "*.*")])
    if not path:
        raise Exception(f"{title} не был выбран.")
    return path

def get_desktop_processed_path(sample_file_path: str) -> str:
    """Формирует путь для сохранения файла Excel на Рабочем столе в папку 'Обработанные' с уникальным именем."""
    desktop = os.path.join(os.path.expanduser("~"), "Desktop")
    processed_folder = os.path.join(desktop, "Обработанные")
    os.makedirs(processed_folder, exist_ok=True)

    base_name = os.path.splitext(os.path.basename(sample_file_path))[0]
    match = re.search(r"\d+", base_name)
    core_name = match.group() if match else "результат"

    result_filename = f"{core_name}.xlsx"
    full_path = os.path.join(processed_folder, result_filename)

    counter = 1
    unique_path = full_path
    while os.path.exists(unique_path):
        unique_filename = f"{core_name} ({counter}).xlsx"
        unique_path = os.path.join(processed_folder, unique_filename)
        counter += 1

    return unique_path
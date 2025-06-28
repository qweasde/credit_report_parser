import tkinter as tk
from tkinter import ttk
from tkinter import messagebox, simpledialog, filedialog
import pandas as pd
import os
import sqlite3
import datetime
import traceback
import re
import concurrent.futures

from db import open_connection, close_connection, create_table_from_df, insert_df_to_table
from parser_credit_report import parse_credit_report
from parser_monthly_payment import parse_monthly_payment, evaluate_row_conditions
from duplicate_markers import mark_duplicates_preply, mark_duplicates_preply2
from converters import convert_types_credit_report
from summary import make_monthly_summary_split
from utils import get_desktop_processed_path, ask_date_request, select_file
from gui import App


def process_pair(entry, index):
    date_request = entry.get("date_request")
    ko_path = entry.get("ko_path")
    ssp_path = entry.get("ssp_path")
    mkk_name = entry.get("mkk") or "default"
    db_name = f"{mkk_name}.db"

    try:
        # Парсинг КО
        credit_df = parse_credit_report(ko_path)
        credit_df_full = credit_df.copy()

        preply_df = credit_df[credit_df["Тип"] == "Договор"].copy()
        preply2_df = credit_df[credit_df["Тип"] == "Договор RUTDF"].copy()

        preply_df = mark_duplicates_preply(preply_df)
        preply2_df = mark_duplicates_preply2(preply2_df)

        credit_df = pd.concat([preply_df, preply2_df], ignore_index=True)
        credit_df = convert_types_credit_report(credit_df)

        # Парсинг ССП
        df_full = parse_monthly_payment(ssp_path, date_request, credit_df)

        cols_simple = [
            "БКИ", "UUID договора", "ДатаРасчета", "Сумма", "Валюта", "Дата заявки",
            "Договор в МКК", "Разница дней", "Маркер дубликатов",
            "Комментарии простого договора", "Маркер простого договора", "Критерий простого договора"
        ]
        cols_rutdf = [
            "БКИ", "UUID договора", "ДатаРасчета", "Сумма", "Валюта", "Дата заявки",
            "Договор в МКК", "Разница дней", "Маркер дубликатов",
            "Комментарии простого договора", "Комментарии RUTDF",
            "Маркер RUTDF", "Критерий RUTDF"
        ]

        df_simple_all = df_full[cols_simple].copy()
        df_rutdf_all = df_full[cols_rutdf].copy()

        def extract_contract_number(path):
            filename = os.path.basename(path)
            match = re.search(r'\d+', filename)
            return match.group(0) if match else None

        contract_number_ko = extract_contract_number(ko_path)

        credit_df_full["Номер договора МКК"] = contract_number_ko
        df_simple_all["Номер договора МКК"] = contract_number_ko
        df_rutdf_all["Номер договора МКК"] = contract_number_ko

        df_summary = make_monthly_summary_split(credit_df_full, writer=None, df_simple_all=df_simple_all, return_df=True)
        df_summary["Номер договора МКК"] = contract_number_ko

        # Работа с базой данных
        conn = open_connection(db_name)

        create_table_from_df(conn, "credit_report", credit_df_full)
        rows_added = insert_df_to_table(conn, "credit_report", credit_df_full)

        create_table_from_df(conn, "monthly_payments_preply", df_simple_all)
        insert_df_to_table(conn, "monthly_payments_preply", df_simple_all)

        create_table_from_df(conn, "monthly_payments_preply2", df_rutdf_all)
        insert_df_to_table(conn, "monthly_payments_preply2", df_rutdf_all)

        create_table_from_df(conn, "payments_summary", df_summary)
        insert_df_to_table(conn, "payments_summary", df_summary)

        close_connection(conn)

        if rows_added == 0:
            return f"Блок #{index + 1}: данные не добавлены — дубликат по номеру договора МКК."
        else:
            return f"Блок #{index + 1} успешно обработан и сохранён в базу данных {db_name}."

    except Exception:
        tb = traceback.format_exc()
        return f"Ошибка в блоке #{index + 1}:\n{tb}"

def main():
    app = App()
    app.title("Парсер КО + ССП")
    app.mainloop()

    entries = app.result
    if entries is None:
        print("Нет данных после закрытия окна.")
        return

    valid_entries = []
    for i, entry in enumerate(entries):
        if entry["date_request"] and entry["ko_path"] and entry["ssp_path"]:
            valid_entries.append((entry, i))
        else:
            print(f"Пропущен блок #{i+1} — не все поля заполнены")

    if not valid_entries:
        print("Нет валидных данных для обработки.")
        return

    for entry, index in valid_entries:
        result = process_pair(entry, index)
        print(result)

if __name__ == "__main__":
    main()
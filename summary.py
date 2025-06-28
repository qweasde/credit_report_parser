import pandas as pd

def make_monthly_summary_split(df: pd.DataFrame, writer=None, df_simple_all: pd.DataFrame = None, return_df=False):
    required_cols = {
        "Тип", "Маркер дубликатов", "Дата платежа <paymtDate>",
        "Сумма платежа <paymtAmt>", "Родительский тег", "Дни просрочки <daysPastDue>"
    }
    if not required_cols.issubset(df.columns):
        print("В датафрейме отсутствуют необходимые колонки для сводки.")
        return None

    # Фильтруем только оригинальные платежи
    df_payments = df[
        (df["Тип"] == "Платёж") & 
        (df["Маркер дубликатов"] == "Оригинал")
    ].copy()

    if df_payments.empty:
        print("Нет оригинальных платежей для сводки.")
        return None

    # Преобразование типов
    df_payments["Дата платежа <paymtDate>"] = pd.to_datetime(df_payments["Дата платежа <paymtDate>"], errors="coerce")
    df_payments["Сумма платежа <paymtAmt>"] = pd.to_numeric(
        df_payments["Сумма платежа <paymtAmt>"].astype(str).str.replace(",", "."),
        errors="coerce"
    )
    df_payments["Дни просрочки <daysPastDue>"] = pd.to_numeric(
        df_payments["Дни просрочки <daysPastDue>"].astype(str).str.replace(",", "."),
        errors="coerce"
    )

    # Добавляем столбец "Месяц"
    df_payments["Месяц"] = df_payments["Дата платежа <paymtDate>"].dt.to_period("M")

    # Группировка для preply — без фильтра
    df_preply = df_payments[df_payments["Родительский тег"] == "preply"]
    sum_preply = df_preply.groupby("Месяц")["Сумма платежа <paymtAmt>"].sum()

    # Для preply2 — фильтрация по просрочке < 30 или NaN
    df_preply2 = df_payments[
        (df_payments["Родительский тег"] == "preply2") &
        (df_payments["Маркер дубликатов"] == "Оригинал") &
        ((df_payments["Дни просрочки <daysPastDue>"].isna()) | (df_payments["Дни просрочки <daysPastDue>"] < 30))
    ]
    sum_preply2 = df_preply2.groupby("Месяц")["Сумма платежа <paymtAmt>"].sum()

    # Объединяем периоды месяцев из обеих группировок
    combined = pd.concat([sum_preply, sum_preply2])
    if combined.empty:
        print("Нет данных для формирования сводки.")
        return None

    start = combined.index.min()
    if start.year < 2020:
        start = combined.index[combined.index.to_timestamp() >= pd.Timestamp("2020-01-01")].min()
    end = combined.index.max()

    all_months = pd.period_range(start=start, end=end, freq="M")

    # Формируем итоговую таблицу
    df_summary = pd.DataFrame({"Месяц": all_months})
    df_summary["Сумма платежей в месяц preply"] = df_summary["Месяц"].map(sum_preply).fillna(0)
    df_summary["Сумма платежей в месяц preply2"] = df_summary["Месяц"].map(sum_preply2).fillna(0)
    df_summary["Разница"] = df_summary["Сумма платежей в месяц preply"] - df_summary["Сумма платежей в месяц preply2"]

    # Добавляем колонку комментариев
    df_summary["Комментарий"] = ""
    mask_added = (df_summary["Сумма платежей в месяц preply"] == 0) & (df_summary["Сумма платежей в месяц preply2"] == 0)
    df_summary.loc[mask_added, "Комментарий"] = "Месяц добавлен автоматически"

    # ----- Вычисление СМД (среднемесячного дохода) -----

    # Получаем дату заявки из df_simple_all
    date_request = pd.to_datetime(df_simple_all["Дата заявки"].iloc[0], format="%d.%m.%Y", errors="coerce")
    if pd.isna(date_request):
        print("Ошибка: дата заявки отсутствует или некорректна.")
    else:
        # Приводим к float с заменой запятой на точку
        for col in ["Сумма платежей в месяц preply", "Сумма платежей в месяц preply2", "Разница"]:
            df_summary[col] = df_summary[col].astype(str).str.replace(",", ".").astype(float)

        df_summary["Месяц_дата"] = df_summary["Месяц"].dt.to_timestamp()

        # Стартовый месяц — первый день предыдущего месяца от даты заявки
        start_month = (date_request - pd.DateOffset(months=1)).replace(day=1)

        if start_month not in df_summary["Месяц_дата"].values:
            start_month = df_summary["Месяц_дата"].min()

        def find_actual_start(start_date, series):
            try:
                idx = series.index.get_loc(start_date)
            except KeyError:
                idx = 0

            for i in range(idx, min(idx + 6, len(series))):
                if series.iat[i] != 0:
                    return i
            return min(idx + 6, len(series) - 1)

        df_summary = df_summary.sort_values("Месяц_дата", ascending=True).reset_index(drop=True)

        preply_series = df_summary.set_index("Месяц_дата")["Сумма платежей в месяц preply"]
        preply2_series = df_summary.set_index("Месяц_дата")["Сумма платежей в месяц preply2"]

        actual_start_idx_preply = find_actual_start(start_month, preply_series)
        actual_start_idx_preply2 = find_actual_start(start_month, preply2_series)

        # Срезы по 24 месяца назад с текущего платежа
        start_idx = actual_start_idx_preply
        end_idx = max(start_idx - 23, 0)
        slice_preply = preply_series.iloc[end_idx:start_idx + 1]

        start_idx2 = actual_start_idx_preply2
        end_idx2 = max(start_idx2 - 23, 0)
        slice_preply2 = preply2_series.iloc[end_idx2:start_idx2 + 1]

        def count_months_with_payment(slice_):
            count = (slice_ != 0).sum()
            return max(count, 18)

        count_preply = count_months_with_payment(slice_preply)
        count_preply2 = count_months_with_payment(slice_preply2)

        smd_preply = slice_preply.sum() / count_preply * 1.3 if count_preply > 0 else 0
        smd_preply2 = slice_preply2.sum() / count_preply2 * 1.3 if count_preply2 > 0 else 0

        # Итоговая строка
        new_row = {
            "Месяц": "СМД по КИ",
            "Сумма платежей в месяц preply": smd_preply,
            "Сумма платежей в месяц preply2": smd_preply2,
            "Разница": smd_preply - smd_preply2,
            "Комментарий": ""
        }
        df_summary = pd.concat([df_summary, pd.DataFrame([new_row])], ignore_index=True)

        # Сортируем по дате, кроме итоговой строки
        df_data = df_summary[df_summary["Месяц"] != "СМД по КИ"].copy()
        df_data = df_data.sort_values("Месяц_дата", ascending=False)
        df_data["Месяц"] = df_data["Месяц_дата"].dt.strftime("%d.%m.%Y")

        df_summary = pd.concat([df_data, df_summary[df_summary["Месяц"] == "СМД по КИ"]], ignore_index=True)

    # Если передан writer — записываем в Excel
    if writer is not None:
        df_summary.to_excel(writer, sheet_name="Сводка платежей", index=False)

    if return_df:
        return df_summary

    return None
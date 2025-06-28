import pandas as pd

def mark_duplicates_preply(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["_original_index"] = df.index  # Сохраняем исходный порядок строк

    def mark_group(group):
        if len(group) == 1:
            group["Маркер дубликатов"] = "Оригинал"
            return group

        group = group.sort_values("Дата обновления информации по платежу <lastUpdatedDt>", ascending=False)
        group["Маркер дубликатов"] = "Дубликат"
        group.iloc[0, group.columns.get_loc("Маркер дубликатов")] = "Оригинал"
        return group

    df_payments = df[df["Тип"] == "Платёж"].copy()
    df_others = df[df["Тип"] != "Платёж"].copy()

    df_payments = df_payments.groupby(
        ["UUID договора", "Дата платежа <paymtDate>", "Сумма платежа <paymtAmt>"],
        group_keys=False
    ).apply(mark_group)

    df = pd.concat([df_payments, df_others], ignore_index=True)

    # Восстанавливаем исходный порядок
    df = df.sort_values("_original_index").reset_index(drop=True)

    df = df.drop(columns=["_original_index"])
    return df
    
# Дубликаты платежей в кредитном отчете (preply2)
def mark_duplicates_preply2(df):
    if 'Маркер дубликатов' not in df.columns:
        df['Маркер дубликатов'] = 'Оригинал'
    else:
        df['Маркер дубликатов'] = 'Оригинал'

    mask = (df['Родительский тег'] == 'preply2') & (df['Тип'] == 'Платёж')
    df_payments = df[mask]

    group_cols = ['UUID договора', 'Сумма платежа <paymtAmt>', 'Дата платежа <paymtDate>']

    for key, group in df_payments.groupby(group_cols):
        # Проверяем, одинаковы ли totalAmt в группе
        if group['Общая сумма платежа <totalAmt>'].nunique() == 1:
            # Если да, выделяем только самый свежий по дате обновления как оригинал
            idx_latest = group['Дата обновления информации по платежу <lastUpdatedDt>'].idxmax()
            df.loc[group.index, 'Маркер дубликатов'] = 'Дубликат'
            df.loc[idx_latest, 'Маркер дубликатов'] = 'Оригинал'
        else:
            # Если разные totalAmt — все оригиналы
            df.loc[group.index, 'Маркер дубликатов'] = 'Оригинал'
    return df
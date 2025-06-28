import pandas as pd
import os
import pandas as pd
import xml.etree.ElementTree as ET
from duplicate_markers import mark_duplicates_preply, mark_duplicates_preply2
from constants import ogrn_to_bki

def evaluate_row_conditions(row, preply_df):
    comments_simple = set()
    comments_rutdf = set()
    marker_simple = "Идет в расчет"
    marker_rutdf = "Идет в расчет"
    criteria_simple = set()
    criteria_rutdf = set()

    # Базовая проверка разницы дней
    diff_days = row.get("Разница дней", 0)
    try:
        diff_days = int(diff_days)
    except Exception:
        diff_days = 0

    if pd.isna(row.get("Разница дней")) or diff_days >= 90:
        comments_simple.add("Более 90 дней с даты заявки")
        comments_rutdf.add("Более 90 дней с даты заявки")
        marker_simple = "Не идет в расчет"
        marker_rutdf = "Не идет в расчет"

    # Проверка на дубликат
    if row.get("Маркер дубликатов") == "Дубликат":
        comments_simple.add("Дубликат")
        comments_rutdf.add("Дубликат")
        marker_simple = "Не идет в расчет"
        marker_rutdf = "Не идет в расчет"


    # Только если БКИ = НБКИ
    if row.get("БКИ") == "НБКИ":
        contract_id = row.get("UUID договора")
        contract_rows = preply_df[preply_df["UUID договора"] == contract_id]

        if contract_rows.empty:
            comments_simple.add("Отсутствуют данные по договору")
            marker_simple = "Не идет в расчет"
            comments_rutdf.add("Отсутствуют данные по договору")
            marker_rutdf = "Не идет в расчет"
        else:
            def aggregate_rows(rows):
                if "Дата обновления информации по займу <lastUpdatedDt>" in rows.columns and not rows.empty:
                    rows = rows.copy()
                    rows["Дата обновления информации по займу <lastUpdatedDt>"] = pd.to_datetime(
                        rows["Дата обновления информации по займу <lastUpdatedDt>"], errors="coerce"
                    )
                    idx = rows["Дата обновления информации по займу <lastUpdatedDt>"].idxmax()
                    rows = rows.loc[[idx]]
                else:
                    rows = rows.head(1)

                aggregated = {}
                for col in rows.columns:
                    values = rows[col].dropna().values
                    aggregated[col] = values[0] if len(values) > 0 else None
                return aggregated
            aggregated_preply2 = {}

            # "Договор"
            contract_rows_simple = contract_rows[contract_rows["Тип"] == "Договор"]
            if not contract_rows_simple.empty:
                aggregated_preply = aggregate_rows(contract_rows_simple)

                date_request = row.get("Дата заявки")
                lastupdateDt = aggregated_preply.get("Дата обновления информации по займу <lastUpdatedDt>")
                closedDt = aggregated_preply.get("Плановая дата закрытия <closedDt>")
                openedDt = aggregated_preply.get("Дата открытия <openedDt>")
                acctType = aggregated_preply.get("Тип займа <acctType>")
                principal_outstanding = aggregated_preply.get("Остаток суммы по договору <principalOutstanding>")
                account_rating = aggregated_preply.get("Статус договора <accountRating>")
                account_rating_text = aggregated_preply.get("Статус договора <accountRatingText>")
                ownerIndic = aggregated_preply.get("Отношение к кредиту <ownerIndic>")
                amtPastDue = aggregated_preply.get("Сумма просроченной задолженности <amtPastDue>")

                field_map = aggregated_preply

                field_map = {
                    "Дата открытия <openedDt>": openedDt,
                    "Тип займа <acctType>": acctType,
                    "Статус договора <accountRating>": account_rating,
                    "Отношение к кредиту <ownerIndic>": ownerIndic,
                    "Плановая дата закрытия <closedDt>": closedDt,
                }

                missing_fields = []
                for name, val in field_map.items():
                    if pd.isna(val):
                        missing_fields.append(name)

                if missing_fields:
                    comments_simple.add("Отсутствуют данные в полях: " + ", ".join(missing_fields))
                    marker_simple = "Не идет в расчет"
                    criteria_simple.add("5.1")

                # Условие 1: Дата последнего обновления более 31 дня назад
                if pd.notna(lastupdateDt) and pd.notna(date_request):
                    if (date_request - lastupdateDt).days > 31:
                        comments_simple.add("Последнее обновление информации более 31 дня назад")
                        marker_simple = "Не идет в расчет"
                        criteria_simple.add("1.1")
                
                closedDt_raw = row.get("closedDt")
                try:
                    closedDt = pd.to_datetime(closedDt_raw)
                except Exception:
                    closedDt = pd.NaT

                # Условие 2: Активный договор, но дата закрытия была более чем за 31 дней до подачи заявки
                if pd.notna(closedDt) and pd.notna(date_request):
                    delta_days = (closedDt - date_request).days

                    if account_rating == "0" and closedDt < (date_request - pd.Timedelta(days=31)):
                        comments_simple.add("Активный договор, но дата закрытия более чем за 31 дней до заявки")
                        marker_simple = "Не идет в расчет"
                        criteria_simple.add("2.1")

                    elif closedDt < date_request:
                        comments_simple.add(f"Договор уже закрыт, прошло {abs(delta_days)} дней с даты закрытия")
                        marker_simple = "Не идет в расчет"
                        criteria_simple.add("2.1")

                    elif delta_days < 31:
                        comments_simple.add(f"До плановой даты закрытия менее 31 дня: осталось {delta_days} дней")
                        marker_simple = "Не идет в расчет"
                        criteria_simple.add("2.1")

                # Условие 3: Активный договор и остаток задолженности = 0 или отсутствует
                try:
                    if account_rating == "0" and (principal_outstanding is None or float(str(principal_outstanding).replace(",", ".")) <= 0):
                        comments_simple.add("Активный договор, но остаток задолженности равен нулю или отсутствует")
                        marker_simple = "Не идет в расчет"
                        criteria_simple.add("3.1")
                except:
                    comments_simple.add("Некорректное значение остатка задолженности")
                    marker_simple = "Не идет в расчет"

                # Условие 4: Просрочен, но просроченной задолженности нет
                try:
                    if account_rating == "52" and (amtPastDue is None or amtPastDue == 0):
                        comments_simple.add("Договор с просрочкой, но сумма просроченной задолженности отсутствует или равна 0")
                        marker_simple = "Не идет в расчет"
                        criteria_simple.add("4.1")
                except:
                    comments_simple.add("Некорректное значение просроченной задолженности")
                    marker_simple = "Не идет в расчет"

                # Условие 6: Договор закрыт (account_rating == "13")
                try:
                    if str(account_rating) == "13":
                        comments_simple.add("Статус договора — закрыт")
                        marker_simple = "Не идет в расчет"
                        criteria_simple.add("6.1")
                except:
                    pass

                # Условие 7: Счет закрыт и передан в другую организацию (account_rating == "14") и это единственная запись по UUID
                try:
                    if account_rating == "14" and account_rating_text == "Счет закрыт - переведен на обслуживание в другую организацию":
                        comments_simple.add("Счет закрыт - переведен на обслуживание в другую организацию")
                        marker_simple = "Не идет в расчет"
                        criteria_simple.add("7.1")
                except:
                    pass

            # "Договор RUTDF"
            contract_rows_rutdf = contract_rows[contract_rows["Тип"] == "Договор RUTDF"]
            if not contract_rows_rutdf.empty:
                aggregated_preply2 = aggregate_rows(contract_rows_rutdf)
                field_map = aggregated_preply2

                required_fields = [
                    "Тип займа trade <acctType>",
                    "Отношение к кредиту trade <ownerIndic>",
                    "Дата открытия trade <openedDt>",
                    "Плановая дата закрытия trade <closeDt>",
                    "Код вида займа (кредита) trade <loanKindCode>"
                ]

                def set_marker_rutdf(new_marker):
                    nonlocal marker_rutdf
                    if new_marker == "Не идет в расчет":
                        marker_rutdf = new_marker

                # Условие 3: Проверка отсутствия обязательных полей
                missing_fields = []
                for field in required_fields:
                    val = field_map.get(field)

                    # Преобразуем значение в pd.Timestamp, если возможно
                    try:
                        val_ts = pd.to_datetime(val)
                    except Exception:
                        val_ts = None

                    # Исключаем 9999-12-31 как допустимую дату
                    if val_ts is not None and val_ts.date() == pd.Timestamp("9999-12-31").date():
                        continue

                    if pd.isna(val) or str(val).strip() in {"", "NaT"}:
                        if field == "Дата открытия trade <openedDt>":
                            alt_val = field_map.get("Дата возникновения обязательства субъекта trade <commitDate>")
                            if pd.notna(alt_val) and str(alt_val).strip() not in {"", "NaT"}:
                                field_map[field] = alt_val
                                comments_rutdf.add("Дата открытия подставлена из commitDate")
                                continue
                        missing_fields.append(field)

                if missing_fields:
                    comments_rutdf.add("Отсутствуют данные в полях: " + ", ".join(missing_fields))
                    set_marker_rutdf("Не идет в расчет")
                    criteria_rutdf.add("3.2")

                # Условие 1–2: Проверка просрочки и задолженности
                loan_indicator = field_map.get("loanIndicator")
                pastdue_amtPastDue = field_map.get("Сумма просроченной задолжности -pastdueArrear <amtPastDue>")
                due_amtOutstanding = field_map.get("Сумма задолжности -dueArrear <amtOutstanding>")

                def is_zero_or_empty(val):
                    try:
                        return pd.isna(val) or float(val) == 0.0
                    except:
                        return True


                if pd.isna(loan_indicator):  # Активный договор
                    if not is_zero_or_empty(pastdue_amtPastDue):
                        comments_rutdf.add("Есть просроченная задолженность, идет в расчет")
                        set_marker_rutdf("Идет в расчет")
                        criteria_rutdf.add("1.2")
                    elif not is_zero_or_empty(due_amtOutstanding):
                        comments_rutdf.add("Нет просрочки, но есть задолженность, идет в расчет")
                        set_marker_rutdf("Идет в расчет")
                        criteria_rutdf.add("1.2")
                    else:
                        comments_rutdf.add("Нет просрочки и задолженности")
                        set_marker_rutdf("Не идет в расчет")
                        criteria_rutdf.add("2.2")
                else:
                    comments_rutdf.add("Договор не активен (loanIndicator заполнен)")
                    set_marker_rutdf("Не идет в расчет")
                    criteria_rutdf.add("1.2")

                    # Условие 4: loanIndicator ≠ 2
                    try:
                        if int(loan_indicator) != 2:
                            comments_rutdf.add("loanIndicator присутствует, но не равен 2 — договор закрыт без признака принудительного исполнения")
                            set_marker_rutdf("Не идет в расчет")
                            criteria_rutdf.add("4.2")
                    except:
                        comments_rutdf.add("Ошибка при обработке loanIndicator")
                        set_marker_rutdf("Не идет в расчет")

    return pd.Series([
        "; ".join(sorted(comments_simple)), marker_simple, ", ".join(sorted(criteria_simple)),
        "; ".join(sorted(comments_rutdf)), marker_rutdf, ", ".join(sorted(criteria_rutdf))
    ])

def parse_monthly_payment(xml_path, date_request, preply_df):
    contract_mkk = os.path.splitext(os.path.basename(xml_path))[0]
    first_word = contract_mkk.split()[0]
    tree = ET.parse(xml_path)
    root = tree.getroot()
    if root.tag == "ОтветНаЗапросСведений":
        parents = root.findall("Сведения")
    elif root.tag == "СведенияОПлатежах":
        parents = [root]
    else:
        parents = [root] 

    data = []
    for parent in parents:
        for kbki in parent.findall("КБКИ"):
            ogrn = kbki.attrib.get("ОГРН")
            bki_name = ogrn_to_bki.get(ogrn, ogrn)

            obligations = kbki.find("Обязательства")
            if obligations is None:
                continue

            bki = obligations.find("БКИ")
            if bki is None:
                continue

            for dogovor in bki.findall("Договор"):
                uid = dogovor.attrib.get("УИД")
                payment = dogovor.find("СреднемесячныйПлатеж")
                if payment is None:
                    continue

                date_calc = payment.attrib.get("ДатаРасчета")
                amount = payment.text.strip() if payment.text else None
                currency = payment.attrib.get("Валюта")

                data.append({
                    "БКИ": bki_name,
                    "UUID договора": uid,
                    "ДатаРасчета": date_calc,
                    "Сумма": amount,
                    "Валюта": currency,
                    "Дата заявки": date_request,
                    "Договор в МКК": first_word
                })

    df = pd.DataFrame(data)

    df["ДатаРасчета"] = pd.to_datetime(df["ДатаРасчета"], errors="coerce")
    df["Дата заявки"] = pd.to_datetime(df["Дата заявки"], format="%d.%m.%Y", errors="coerce")

    # Разница дней
    df["Разница дней"] = (df["Дата заявки"] - df["ДатаРасчета"]).dt.days

    # Сумма в числовой формат
    df["Сумма"] = pd.to_numeric(df["Сумма"].astype(str).str.replace(",", "."), errors="coerce")

    # Дальше логика группировки или что у тебя было
    grouped = df.groupby("UUID договора")
    result = []

    for uid, group in grouped:
        group = group.copy()
        group["Маркер дубликатов"] = "Дубликат"

        # 1. Отбор по максимальной дате расчёта
        max_date = group["ДатаРасчета"].max()
        candidates = group[group["ДатаРасчета"] == max_date]

        # 2. Если таких несколько — отбор по максимальной сумме
        if len(candidates) > 1:
            max_amount = candidates["Сумма"].max()
            candidates = candidates[candidates["Сумма"] == max_amount]

        # 3. Если таких несколько — отбор по НБКИ
        if len(candidates) > 1 and (candidates["БКИ"] == "НБКИ").any():
            candidates = candidates[candidates["БКИ"] == "НБКИ"]

        # 4. Финальный выбор — первая подходящая
        idx_final = candidates.index[0]
        group.loc[idx_final, "Маркер дубликатов"] = "Оригинал"

        result.append(group)

    if not result:
        return pd.DataFrame(), pd.DataFrame()

    df_final = pd.concat(result)

    # Применяем комментарии и маркеры и критерии
    df_final[
        [
            "Комментарии простого договора",
            "Маркер простого договора",
            "Критерий простого договора",
            "Комментарии RUTDF",
            "Маркер RUTDF",
            "Критерий RUTDF",
        ]
    ] = df_final.apply(lambda row: evaluate_row_conditions(row, preply_df), axis=1)

    return df_final

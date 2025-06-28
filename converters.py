import pandas as pd

def convert_types_credit_report(df):
    date_fields = [
        "Дата открытия <openedDt>",
        "Дата обновления информации по займу <lastUpdatedDt>",
        "Дата обновления информации по платежу <lastUpdatedDt>",
        "Дата последнего платежа <lastPaymtDt>",
        "Плановая дата закрытия <closedDt>",
        "Плановая дата закрытия RUTDF <closeDt>",
        "Дата статуса договора <accountRatingDate>",
        "Дата создания записи <fileSinceDt>",
        "Дата передачи финансирования <fundDate>",
        "Дата формирования кредитной информации <headerReportingDt>",
        "Дата ближайшего платежа по основному долгу <principalTermsAmtDt>",
        "Дата возникновения обязательства субъекта <commitDate>",
        "Дата расчета <amtDate>",
        "Дата расчета <calcDate>",
        "Дата возникновения срочной задолженности <startDt>",
        "Дата платежа <paymtDate>",
        "Дата расчета -dueArrear <calcDate>",
        "Дата расчета -pastdueArrear <calcDate>",
        "Дата возникновения обязательства субъекта trade <commitDate>",
        "Дата открытия trade <openedDt>",
        "Дата расчета -accountAmt <amtDate>",
        "Дата ближайшего следующего платежа по основному долгу -paymtCondition <principalTermsAmtDt>",
    ]

    numeric_fields = [
        "Кредитный лимит <creditLimit>",
        "Сумма задолженности <amtOutstanding>",
        "Сумма платежа <paymtAmt>",
        "Сумма просроченной задолженности <amtPastDue>",
        "Сумма просроченной задолжности -dueArrear <amtPastDue>",
        "Сумма просроченной задолжности -pastdueArrear <amtPastDue>",
        "Текущий баланс <curBalanceAmt>",
        "Остаток суммы по договору <principalOutstanding>",
        "Просрочка <paymtPat>",
        "Ставка <creditTotalAmt>",
        "Сумма внесенных платежей по процентам <intTotalAmt>",
        "Сумма среднемесячного платежа <averPaymtAmt>",
        "Дни просрочки <daysPastDue>"
    ]

    for col in date_fields:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    for col in numeric_fields:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', '.'), errors='coerce')

    return df
import pandas as pd
from lxml import etree
from datetime import datetime
from constants import combined_fields, acct_type_dict, owner_indic_dict, combined_payment_fields
from duplicate_markers import mark_duplicates_preply, mark_duplicates_preply2

def parse_credit_report(xml_path):
    tree = etree.parse(xml_path)
    root = tree.getroot()
    rows = []

    for parent_block in root:
        parent_tag = parent_block.tag

        for node_type in ["AccountReply", "AccountReplyRUTDF"]:
            for acc in parent_block.findall(f".//{node_type}"):
                serial = acc.findtext("serialNum")
                uuid = acc.findtext("uuid")
                contract = {
                    "Родительский тег": parent_tag,
                    "Тип": f"Договор{' RUTDF' if node_type == 'AccountReplyRUTDF' else ''}",
                    "Тип договора": node_type,
                    "Номер договора": serial,
                    "UUID договора": uuid
                }

                for tag, label in combined_fields.items():
                    val = acc.findtext(tag)
                    if tag == "closeDt" and not val:
                        val = acc.findtext("closedDt")
                    contract[label] = val

                if node_type == "AccountReplyRUTDF":
                    code = contract.get("Тип займа <acctType>")
                    contract["Тип займа <acctTypeText>"] = acct_type_dict.get(code)
                    owner_code = contract.get("Отношение к кредиту <ownerIndic>")
                    contract["Отношение к кредиту <ownerIndicText>"] = owner_indic_dict.get(owner_code)

                    paymtCondition_block = acc.find("paymtCondition")
                    if paymtCondition_block is not None:
                        contract["Дата ближайшего следующего платежа по основному долгу -paymtCondition <principalTermsAmtDt>"] = paymtCondition_block.findtext("principalTermsAmtDt")

                    monthAverPaymt_block = acc.find("monthAverPaymt")
                    if monthAverPaymt_block is not None:
                        contract["Величина среднемесячного платежа -monthAverPaymt <averPaymtAmt>"] = monthAverPaymt_block.findtext("averPaymtAmt")

                    trade_block = acc.find("trade")
                    if trade_block is not None:
                        contract["Код вида займа (кредита) trade <loanKindCode>"] = trade_block.findtext("loanKindCode")
                        contract["Дата возникновения обязательства субъекта trade <commitDate>"] = trade_block.findtext("commitDate")
                        contract["Тип займа trade <acctType>"] = trade_block.findtext("acctType")
                        contract["Отношение к кредиту trade <ownerIndic>"] = trade_block.findtext("ownerIndic")
                        contract["Плановая дата закрытия trade <closeDt>"] = trade_block.findtext("closeDt")
                        contract["Дата открытия trade <openedDt>"] = trade_block.findtext("openedDt")

                    accountAmt_block = acc.find("accountAmt")
                    if accountAmt_block is not None:
                        contract["Дата расчета -accountAmt <amtDate>"] = accountAmt_block.findtext("amtDate")

                    def parse_date(date_str):
                        try:
                            return datetime.strptime(date_str, "%Y-%m-%d")
                        except:
                            return None

                    # Самый свежий pastdueArrear по calcDate
                    pastdue_latest = None
                    pastdue_amt = None

                    for past in acc.findall("pastdueArrear"):
                        calc_date_str = past.findtext("calcDate")
                        calc_date = parse_date(calc_date_str)
                        if calc_date and (pastdue_latest is None or calc_date > pastdue_latest):
                            pastdue_latest = calc_date
                            pastdue_amt = past.findtext("amtPastDue")
                    
                    contract["Дата расчета -pastdueArrear <calcDate>"] = pastdue_latest.strftime("%Y-%m-%d") if pastdue_latest else None
                    contract["Сумма просроченной задолжности -pastdueArrear <amtPastDue>"] = pastdue_amt


                    # Самый свежий dueArrear по calcDate
                    due_latest = None
                    due_outstanding = None

                    for due in acc.findall("dueArrear"):
                        calc_date_str = due.findtext("calcDate")
                        calc_date = parse_date(calc_date_str)
                        if calc_date and (due_latest is None or calc_date > due_latest):
                            due_latest = calc_date
                            due_outstanding = due.findtext("amtOutstanding")

                    contract["Дата расчета -dueArrear <calcDate>"] = due_latest.strftime("%Y-%m-%d") if due_latest else None
                    contract["Сумма задолжности -dueArrear <amtOutstanding>"] = due_outstanding

                rows.append(contract)

                payments = []
                for p in acc.findall(".//payment"):
                    row = {
                        "Родительский тег": parent_tag,
                        "Тип": "Платёж",
                        "Тип договора": node_type,
                        "Номер договора": serial,
                        "UUID договора": uuid,
                        "Маркер дубликатов": ""
                    }
                    for tag, label in combined_payment_fields.items():
                        row[label] = p.findtext(tag)
                    payments.append(row)
                    rows.append(row)

                if payments:
                        total = sum(
                            float(str(p.get("Сумма платежа <paymtAmt>")).replace(",", "."))
                            for p in payments if p.get("Сумма платежа <paymtAmt>")
                        )
                        rows.append({
                            "Родительский тег": parent_tag,
                            "Тип": "Итого",
                            "Тип договора": node_type,
                            "Сумма платежа <paymtAmt>": total,
                            "Номер договора": serial,
                            "UUID договора": uuid
                        })

    df = pd.DataFrame(rows)

    payments_df = df[df["Тип"] == "Платёж"].copy()

    if any(payments_df["Родительский тег"].str.contains("preply2")):
        payments_df = mark_duplicates_preply2(payments_df)
    else:
        payments_df = mark_duplicates_preply(payments_df)

    df.loc[payments_df.index, "Маркер дубликатов"] = payments_df["Маркер дубликатов"]
    
    return df
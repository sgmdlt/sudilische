import re
from datetime import datetime

from bs4 import BeautifulSoup


def parse_page(text, url):
    soup = BeautifulSoup(text.replace("</br>", "<br>"), "html.parser")
    rows = []
    for row in soup.select(
                "table[id=tablcont] tr:not(:has(th)), " +
                "div[id=resultTable] > table > tbody tr, " +
                "div.wrapper-search-tables > table > tbody tr"):
        rows.append(parse_row(row, url))
    return rows


def parse_row(row, url):
    parsed_row = {}

    # Записываем нужные поля
    parsed_row["case_number"] = row.select("td")[0].get_text(" ", strip=True)
    parsed_row["date_reg"] = row.select("td")[1].get_text(" ", strip=True)

    # Выделяем ссылку на карточку из строки
    parsed_row["link"] = url[:url.find(
            "/modules")] + row.select("td")[0].select_one("a")["href"]

    # Создаем идентификатор
    id_date = "NONE"
    if parsed_row["date_reg"]:
        id_date = datetime.strptime(parsed_row["date_reg"],
                                    "%d.%m.%Y").strftime("%Y-%m-%d")
        case_id = url[:url.find(
            "/modules")] + "_" + id_date + "_"
        try:
            if all(char not in parsed_row["case_number"]
               for char in ["(", "~", "∼"]):
                case_id += parsed_row["case_number"][:parsed_row["case_number"].
                                                 rfind("/")].replace("/", "-")
            elif id_date != "NONE" and parsed_row["date_reg"][-4:] in parsed_row[
                "case_number"]:
                case_id += re.search(r"([^ ~∼\(]+)/" + parsed_row["date_reg"][-4:],
                                 parsed_row["case_number"]).group(1).replace(
                                     "/", "-")
            else:
                case_id += re.search(r"([^ ~∼\(]+)/20",
                                 parsed_row["case_number"]).group(1).replace(
                                     "/", "-")
        except Exception:
            case_id += "ID_NOT_PARSED"

        parsed_row["ID"] = case_id

        return parsed_row

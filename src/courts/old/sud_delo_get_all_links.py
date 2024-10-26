import asyncio
import csv
import json
import logging
import os
import random
import re
from datetime import datetime
from pathlib import Path

from bs4 import BeautifulSoup

from courts.captcha.handler import CaptchaHandler
from courts.downloader.async_downloader import Downloader
from courts.url import ParamsDTO, get_url

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Заголовки для запроса
HEADERS = {
    "User-Agent":
    "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) " +
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Mobile Safari/537.36"
}
setattr(asyncio.sslproto._SSLProtocolTransport, "_start_tls_compatible", True)

with open('proxy.txt', 'r') as f:
    PROXIES_LIST = [line.strip() for line in f if line.strip()]

# captcha mock
class CaptchaSolver:
    async def solve_captcha(self, court_link, downloader):
        url = court_link + "name=sud_delo&name_op=sf&delo_id=1540005"
        soup = BeautifulSoup(await downloader.send_request(url), "html.parser")
        captcha_result = "12345"
        return {
            "captcha": captcha_result,
            "captchaid": soup.select_one("input[name=captchaid]")["value"]
        }

# Обработчик строк в результатах поиска
def parse_row(row, court):
    parsed_row = {}

    # Записываем нужные поля
    parsed_row["region"] = court["region"]
    parsed_row["court_name"] = court["name"]
    parsed_row["case_number"] = row.select("td")[0].get_text(" ", strip=True)
    parsed_row["date_reg"] = row.select("td")[1].get_text(" ", strip=True)

    # Выделяем ссылку на карточку из строки
    parsed_row["link"] = court["link"][:court["link"].find(
        "/modules")] + row.select("td")[0].select_one("a")["href"]

    # Создаем идентификатор
    id_date = "NONE"
    if parsed_row["date_reg"]:
        id_date = datetime.strptime(parsed_row["date_reg"],
                                    "%d.%m.%Y").strftime("%Y-%m-%d")
    case_id = court["id"] + "_" + id_date + "_"
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


# Главный обработчик списка судов
async def get_cases(task_number, courts, query_list, output_filename):

    # Инициализируем вывод и капчу
    failed_courts = []
    captcha_pack = {"captcha": "", "captchaid": "", "region": "00"}

    # Случайная задержка по времени для разводки запросов
    await asyncio.sleep(random.uniform(1, 10))

    # Проверяем суды по списку
    total_count = 0
    for court in courts:

        # Формируем ссылку
        for query_set in query_list:

            # Запускаем сессию заново для каждого запроса
            async with Downloader(proxies_list=PROXIES_LIST, captcha_handler=CaptchaHandler(CaptchaSolver())) as downloader:

                params = {'court_url': court["link"], 'court_type': '1', **query_set}
                url = get_url(ParamsDTO(**params))
                #url = court["link"] + get_url(court, query_set)

                # Цикл для нескольких страниц
                next_page = True
                card_count = 0
                target_count = 0
                while next_page:
                    # Если регион тот же и капчу ещё не проверяли, ставим старую
                    if "captcha" not in url and court["id"].startswith(
                            captcha_pack["region"]):
                        url += "&captcha=" + captcha_pack[
                            "captcha"] + "&captchaid=" + captcha_pack[
                                "captchaid"]

                    # Отправляем запрос
                    response = await downloader.send_request(url)

                    # Проверяем, нет ли ошибки капчи
                    while any(error in response for error in [
                            "Неверно указан проверочный код с картинки",
                            "Время жизни сессии закончилось"
                    ]):

                        # Решаем капчу
                        print(f"🤖 {task_number:>2d} | ⏳ решаю капчу...")
                        prev_captcha = captcha_pack["captchaid"]
                        got_new_captcha = False
                        while not got_new_captcha:
                            captcha_pack = await CaptchaSolver().solve_captcha(
                                court["link"], downloader)

                            # Если капча уже один раз не подошла, ждём новую
                            if prev_captcha == captcha_pack["captchaid"]:
                                print(
                                    f"🤖 {task_number:>2d} |",
                                    "☹️ капча распознана неудачно, ждём новую..."
                                )
                                await asyncio.sleep(20)
                            else:
                                got_new_captcha = True

                        captcha_pack.update({"region": court["id"][:2]})

                        # Убираем старую капчу
                        url = re.sub("&captcha=[^&]+", "", url)
                        url = re.sub("&captchaid=[^&]+", "", url)

                        # Добавляем капчу в адрес
                        url += "&captcha=" + captcha_pack[
                            "captcha"] + "&captchaid=" + captcha_pack[
                                "captchaid"]

                        # Пробуем код
                        await asyncio.sleep(1)
                        response = await downloader.send_request(url)

                    soup = BeautifulSoup(response.replace("</br>", "<br>"),
                                         "html.parser")

                    # Один раз записываем, сколько надо скачать
                    if target_count == 0:
                        if target_str := re.search(
                                r"Всего по запросу найдено [-—] (\d+)\.",
                                soup.get_text()):
                            target_count = int(target_str.group(1))

                    # Перебор рядов
                    for row in soup.select(
                            "table[id=tablcont] tr:not(:has(th)), " +
                            "div[id=resultTable] > table > tbody tr, " +
                            "div.wrapper-search-tables > table > tbody tr"):

                        # Записываем в файл по коду суда
                        output_row = parse_row(row, court)
                        open(output_filename + ".jsonl", "a").write(
                            json.dumps(output_row, ensure_ascii=False).replace(
                                u"\u00A0", " ") + "\n")


                        card_count += 1
                        total_count += 1

                    # Перебор страниц
                    next_page = False
                    for link in soup.select(
                            "a[title='Следующая страница'], " +
                            "ul.pagination a:-soup-contains-own('»')"):
                        url = court["link"][:court["link"].
                                            find("/modules")] + link["href"][
                                                link["href"].find("/modules"):]

                        # Если слетел флаг NC, добавляем обратно
                        if "nc=1" in court["link"] and "nc=1" not in url:
                            url += "&nc=1"

                        next_page = True
                        break

                    # Отчитываемся через каждые 500 карточек
                    if card_count % 500 == 0 and target_count > 0:
                        print(
                            f"🤖 {task_number:>2d} | {court['name']} 📇 {card_count / target_count:>4.0%} карточек"
                        )

                print(f"🤖 {task_number:>2d}",
                      f"{court['name']:<40s}",
                      query_set["type"],
                      query_set["instance"],
                      query_set["entry_date_from"],
                      query_set["entry_date_to"],
                      end=" | ",
                      sep=" | ")

                # Проверка результата
                if card_count > 0 and card_count >= target_count:
                    print(f"🏁 {card_count:>4d}")

                # Если ничего не нашли, ставим ноль
                elif soup.select(
                        "div[id=content]:-soup-contains('Данных по запросу не обнаружено'), "
                        +
                        "div[id=search_results]:-soup-contains('Данных по запросу не найдено'), "
                        +
                        "div.resultsearch_text:-soup-contains('По вашему запросу ничего не найдено')"
                ):
                    print("🫙")

                # Если суд не работает
                elif soup.select(
                        "div.box:-soup-contains('Информация временно недоступна'), "
                        +
                        "body:-soup-contains('заблокирован по соображениям безопасности')"
                ):
                    print("⛔️")
                    if court not in failed_courts:
                        failed_courts.append(court)

                # Если что-то незнакомое
                else:
                    print("🟥🟥🟥🟥")
                    print(url)
                    if court not in failed_courts:
                        failed_courts.append(court)

    print(
        f"🤖 {task_number:>2d} | {'🟢 ЗАКОНЧИЛ':>30s}: {total_count:>4d} карточек"
    )
    return total_count, failed_courts

# Удаление дубликатов из файла
def remove_duplicates(filename):

    print("\nУбираем дубликаты в:", filename)

    unique_ids = set()
    count = 0
    with open(filename + ".jsonl", 'w+') as input_file:
        for line in input_file:
            case = json.loads(line)

            if case["ID"] not in unique_ids:
                open(filename + ".jsonl" + "c", "a+").write(line)
                unique_ids.add(case["ID"])
                count += 1

        input_file.close()
    if not Path.exists(Path(f'./{filename}.jsonlc').absolute()):
        open(f'{filename}.jsonlc', 'w+')
    os.rename(filename + ".jsonl" + "c", filename + ".jsonl")

    print("Без дубликатов записано:", count)


# Запуск параллельной обработки судов
async def court_runner(output_filename, court_codes, query_set, tasks):

    # Загружаем список судов
    courts_list = []
    if "Failed" in court_codes:
        courts_list.extend(list(csv.DictReader(open(court_codes + ".csv"))))
    else:
        for court in csv.DictReader(open("./court_list_all.csv")):
            if court["id"][2:4] in court_codes.split("+") and court[
                    "id"] != "77RS0000" and court["id"] != "77OS0000":
                courts_list.append(court)
    print("В списке судов", court_codes, len(courts_list), "строк")

    # Создаем перечень процессов
    coro_list = []
    batch_size = round(len(courts_list) / tasks)
    print("Судов на один процесс:", batch_size, "\n\n")
    for i in range(tasks):
        if i < tasks - 1:
            courts_batch = courts_list[i * batch_size:i * batch_size +
                                       batch_size]
        else:
            courts_batch = courts_list[i * batch_size:]
        coro_list.append(
            get_cases(i + 1, courts_batch, query_set, output_filename))

    # Запускаем все процессы
    gather_start = datetime.now()
    gather_result = await asyncio.gather(*coro_list)

    # После окончания собираем результаты в общий список
    final_count = 0
    final_failed = []
    for total_count, failed_courts in gather_result:
        final_count += total_count
        final_failed.extend(failed_courts)

    print("\n\nЗаписано в файл результатов:", final_count)

    # Удаление дубликатов
    #remove_duplicates(output_filename)

    # Запись судов с ошибками
    if final_failed:
        with open(output_filename + "-Failed.csv", 'w+') as failed_output:
            writer = csv.DictWriter(failed_output, courts_list[0].keys())
            writer.writeheader()
            writer.writerows(final_failed)
        print("\nЗаписано судов с ошибками:", len(final_failed))

    print("Прошло", int(
        (datetime.now() - gather_start).total_seconds() / 3600), "час(ов)")


# ГЛАВНЫЙ ЗАПУСК

# Загружаем прокси
PROXIES_LIST = open("./proxy.txt").read().splitlines()[1:]

# Загружаем модель
""" loaded_model = load_model("sud_rnn_0.6.1")
prediction_model = models.Model(
    loaded_model.get_layer(name="image").input,
    loaded_model.get_layer(name="dense2").output)
characters = "0123456789"
char_to_num = layers.StringLookup(vocabulary=list(characters), mask_token=None)
num_to_char = layers.StringLookup(vocabulary=char_to_num.get_vocabulary(),
                                  mask_token=None,
                                  invert=True) """

# Параметры запроса
QUERY_SET = {
    "type": "",
    "instance": 1,
    "article": "",
    "participant": "",
    "entry_date_from": "",
    "entry_date_to": "",
    "mat_category": ["", "", ""],
    "download_mat_cat": False
}

article_collection = {
    1: ["УК", "ГК", "КАС", "КоАП", "Материал"],
    2: ["УК", "ГК", "КАС", "КоАП"]
}

# Набор последовательных запросов с разбиением по месяца
query_list = []
for number in [1, 2]:
    QUERY_SET["instance"] = number
    for art in article_collection[number]:
        QUERY_SET["type"] = str(art)

        QUERY_SET["entry_date_from"] = ""
        QUERY_SET["entry_date_to"] = "01.01.2010"
        query_list.append(QUERY_SET.copy())
        current_year = datetime.now().year
        current_month = datetime.now().month
        for year in range(2010, current_year):
            for month in range(1, 12):
                QUERY_SET["entry_date_from"] = f"01.{month:02d}.{year}"
                QUERY_SET["entry_date_to"] = f"01.{month + 1:02d}.{year}"
                query_list.append(QUERY_SET.copy())
            QUERY_SET["entry_date_from"] = f"01.12.{year}"
            QUERY_SET["entry_date_to"] = f"01.01.{year + 1}"
            query_list.append(QUERY_SET.copy())

        for month in range(1, current_month):
            QUERY_SET["entry_date_from"] = f"01.{month:02d}.{current_year}"
            QUERY_SET["entry_date_to"] = f"01.{month + 1:02d}.{current_year}"
            query_list.append(QUERY_SET.copy())
        QUERY_SET["entry_date_from"] = f"01.{current_month:02d}.{current_year}"
        QUERY_SET["entry_date_to"] = ""
        query_list.append(QUERY_SET.copy())

# Запуск сбора
# Имя файла для результата, набор судов, список (!) поисковых запросов, количество процессов
#asyncio.run(court_runner("GV-All-8", "RS", query_list, 4))

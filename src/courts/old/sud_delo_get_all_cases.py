import asyncio
import gc
import json
import random
import sys
from datetime import datetime

import aiohttp
from bs4 import BeautifulSoup

# Заголовки для запроса
HEADERS = {
    "User-Agent":
    "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) " +
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Mobile Safari/537.36"
}
setattr(asyncio.sslproto._SSLProtocolTransport, "_start_tls_compatible", True)


# Обработка ошибок при запросе
async def send_request(url, task_number, session):
    # Распределяем по всем прокси
    global PROXIES_LIST
    proxy_config = "http://" + PROXIES_LIST[task_number % len(PROXIES_LIST)]


    # Задержка между запросами
    await asyncio.sleep(random.uniform(1, 3))

    # Цикл ответа
    got_resp = False
    verify_cert = True
    retry_counter = 0
    while not got_resp:
        try:

            # Отправляем запрос без проверки сертификата (прокси при необходимости)
            async with session.get(url, ssl=verify_cert,
                                   proxy=proxy_config) as resp:
                final_resp = await resp.text()
                if resp.ok or resp.status == 499:
                    got_resp = True
                else:
                    final_resp = "ОШИБКА"
                    got_resp = True

                # Трижды перепроверяем нестабильный сервер
                if "Информация временно недоступна" in final_resp and retry_counter < 2:
                    retry_counter += 1
                    got_resp = False

        # Обработка ошибок
        except Exception as e:
            # Если проблема в сертификате, отключаем проверку и пробуем снова
            if "certificate has expired" in str(e):
                verify_cert = False
            elif "Server disconnected" not in str(e):
                print(f"🤖 {task_number:>3d} |", end=" ")
                if "TimeoutError" in type(e).__name__:
                    print("Нет ответа 30 секунд, proxy " + proxy_config)
                    final_resp = "ОШИБКА"
                    got_resp = True
                elif "0x98" in str(e):
                    print("Запрос привёл к ошибке на сервере")
                    final_resp = "ОШИБКА"
                    got_resp = True
                else:
                    print(str(e))
                    print(type(e).__name__)
                    print(url)
                    final_resp = "ОШИБКА"
                    got_resp = True
                await asyncio.sleep(5)

    return final_resp


# Главный обработчик списка решений
async def get_acts(task_number, results, input_filename):
    global PROXIES_LIST

    # Случайная задержка по времени для разводки запросов
    await asyncio.sleep(random.uniform(1, 10))

    # Запускаем сессию для каждого процесса
    async with aiohttp.ClientSession(
        headers=HEADERS, timeout=aiohttp.ClientTimeout(total=30)) as session:

        # Проверяем решения по списку
        for result in results:
            # print(result["Карточка дела"])
            # Отправляем запрос с сокращалкой
            response = await send_request(result["link"] + "&nc=1",
                                          task_number, session)
            soup = BeautifulSoup(response, "html.parser")

            # Если все в порядке
            if all(error not in response for error in [
                    "НЕВЕРНЫЙ ФОРМАТ ЗАПРОСА!",
                    "Информация временно недоступна", "ОШИБКА"
            ]) and soup.select_one("div.title, div.name-instanse"):

                # Заполняем базовые поля из списка карточек
                formatted_case = {
                    "region": result["region"],
                    "court_name": result["court_name"],
                    "case_number": result["case_number"],
                    "date_reg": result["date_reg"],
                    "court_code": result["ID"][:8],
                    "id_final": result["ID"]
                }

                # Выделяем тип дела и инстанцию
                title = soup.select_one("div.title, div.name-instanse"
                                        ).get_text(strip=True).split(" - ")
                match title[0]:
                    case "Гражданские дела" | "Гражданские и административные дела":
                        formatted_case["case_type"] = "Гражданское дело"
                    case "Уголовные дела":
                        formatted_case["case_type"] = "Уголовное дело"
                    case "Дела об административных правонарушениях":
                        formatted_case[
                            "case_type"] = "Дело об административном правонарушении"
                    case "Административные дела (КАC РФ)":
                        formatted_case["case_type"] = "Административное дело"
                    case "Производство по материалам":
                        formatted_case["case_type"] = title[0]
                    case _:
                        print("Неизвестный тип дела:", title[0])
                if len(title) == 1:
                    title.append("первая инстанция")
                else:
                    match title[1]:
                        case "первая инстанция":
                            formatted_case["instance"] = "Первая инстанция"
                        case "апелляция" | "жалобы на постановления" | "первый пересмотр":
                            formatted_case["instance"] = "Апелляция"
                        case "кассация":
                            formatted_case["instance"] = "Кассация"
                        case _:
                            print("Неизвестная инстанция:", title[1])

                # Забираем основные параметры дела
                delo_table = soup.select_one(
                    "table[id=tablcont]:has(tr > th:-soup-contains-own('ДЕЛО')), "
                    + "[id=tab_content_Case]")
                if delo_table:
                    FIELD_CONVERTER = {
                        "case_category_article":
                        "Категория дела', 'Вид материала",
                        "case_category_sub_type":
                        "Предмет представления, ходатайства, жалобы', 'Сущность заявленного требования",
                        "date_result": "Дата рассмотрения",
                        "id_uid": "Уникальный идентификатор дела",
                        "judge": "Судья",
                        "result": "Результат рассмотрения",
                        "adm_case_pr_number": "Номер протокола об АП"
                    }
                    for key in FIELD_CONVERTER:
                        if field := delo_table.select_one(
                                f"td:-soup-contains('{FIELD_CONVERTER[key]}') + td"
                        ):
                            formatted_case[key] = field.get_text(strip=True)
                    if "в силу" in delo_table:
                        print(delo_table.get_text())

                    # Преобразуем даты
                    for key in formatted_case:
                        if "date" in key and formatted_case[key]:
                            formatted_case[key] = datetime.strptime(
                                formatted_case[key],
                                "%d.%m.%Y").strftime("%Y-%m-%d")

                    # Подсудимые
                    if def_table := soup.select_one(
                            "table[id=tablcont]:has(tr > th:-soup-contains-own('ЛИЦА')), "
                            + "table[id=tab_content_DefList]"):

                        # Дополнительная валидация для старых таблиц
                        if ("tablcont" in str(def_table)
                                and ">ЛИЦА<" in str(def_table)
                            ) or "tab_content_DefList" in str(def_table):
                            defendants = []
                            for row in def_table.select(
                                    "tr:-soup-contains('Фамилия') ~ tr"):
                                defendant = []
                                for cell in row.select("td"):
                                    defendant.append(cell.get_text(strip=True))
                                defendants.append(" - ".join(defendant))
                            formatted_case["defendants"] = "\n".join(
                                defendants)

                    # Стороны по делу
                    if part_table := soup.select_one(
                            "table[id=tablcont]:has(tr > th:-soup-contains-own('СТОРОНЫ', 'УЧАСТНИКИ')), "
                            + "table[id=tab_content_PersonList]"):
                        participants = []
                        for row in part_table.select(
                                "tr:-soup-contains('Вид лица') ~ tr"):
                            participant = []
                            for cell in row.select("td"):
                                participant.append(cell.get_text(strip=True))
                            participants.append(" - ".join(participant))
                        formatted_case["participants"] = "\n".join(
                            participants)

                    # Нижестоящий суд
                    if first_table := soup.select_one(
                            "table[id=tablcont]:has(tr > th:-soup-contains-own('РАССМОТРЕНИЕ В НИЖЕСТОЯЩЕМ СУДЕ')), "
                            + "table[id=tab_content_CourtI]"):
                        first_field_converter = {
                            "first_court_name":
                            "Суд (судебный участок) первой инстанции",
                            "first_case_number":
                            "Номер дела в первой инстанции",
                            "first_judge":
                            "Судья (мировой судья) первой инстанции"
                        }
                        for key in first_field_converter:
                            if field := first_table.select_one(
                                    f"td:-soup-contains('{first_field_converter[key]}') + td"
                            ):
                                formatted_case[key] = field.get_text(
                                    strip=True)

                    # Если есть текст, сохраняем его
                    if "<HTML>" in response[10:]:
                        formatted_case["document_text"] = BeautifulSoup(
                            response[response.find("<HTML>", 10):response.
                                     find("</HTML>")],
                            "html.parser").get_text()

                    # Записываем в файл по коду суда
                    open(
                        f"cases_by_court/{sys.argv[2]}/" +
                        formatted_case["court_code"] + ".jsonl", "a").write(
                            json.dumps(formatted_case, ensure_ascii=False).
                            replace(u"\u00A0", " ") + "\n")

            # Все неудачные карточки сохраняем
            else:
                open(input_filename + "-Cases-Failed.jsonl",
                     "a").write(json.dumps(result, ensure_ascii=False) + "\n")

            # Отчитываемся каждые 100 решений
            if results.index(result) % 100 == 0:
                print(f"🤖 {task_number:>3d}",
                      "{:>4.0%}".format(
                          (results.index(result) + 1) / len(results)),
                      sep=" | ")

    print(f"🤖 {task_number:>3d}", "🟢 ЗАКОНЧИЛ", sep=" | ")


# Запуск параллельной обработки решений
async def acts_runner(input_filename, tasks, last_part):
    print("Cписок результатов", input_filename)

    # Загружаем список результатов
    case_list = []
    case_count = 1
    part_count = 1
    gather_start = datetime.now()
    with open(input_filename + ".jsonl") as input_file:
        for line in input_file:
            case_list.append(json.loads(line))

            # Разделяем на куски по миллиону карточек
            if case_count <= 1000000:
                case_count += 1
            else:

                if part_count > last_part:
                    print("Загружена партия", part_count)
                    print("Скачиваем")

                    # Создаем перечень процессов
                    coro_list = []
                    batch_size = round(len(case_list) / tasks)
                    for i in range(tasks):
                        if i < tasks - 1:
                            case_batch = case_list[i *
                                                   batch_size:i * batch_size +
                                                   batch_size]
                        else:
                            case_batch = case_list[i * batch_size:]
                        coro_list.append(
                            get_acts(i + 1, case_batch, input_filename))

                # Очищаем память
                del case_list
                gc.collect()

                if part_count > last_part:
                    # Запускаем все процессы
                    await asyncio.gather(*coro_list)

                    print(
                        "Кусок", part_count, "скачан, прошло",
                        int((datetime.now() - gather_start).total_seconds() /
                            3600), "час(ов)")

                # Обнуляем список
                case_list = []
                case_count = 0
                part_count += 1

    # Последняя пачка
    if case_count > 0:
        print("Загружена последняя партия")
        print("Скачиваем")

        # Создаем перечень процессов
        coro_list = []
        batch_size = round(len(case_list) / tasks)
        for i in range(tasks):
            if i < tasks - 1:
                case_batch = case_list[i * batch_size:i * batch_size +
                                       batch_size]
            else:
                case_batch = case_list[i * batch_size:]
            coro_list.append(get_acts(i + 1, case_batch, input_filename))

        # Очищаем память
        del case_list
        gc.collect()

        # Запускаем все процессы
        await asyncio.gather(*coro_list)

        print("Последний кусок", part_count, "скачан, прошло",
              int((datetime.now() - gather_start).total_seconds() / 3600),
              "час(ов)")


# ГЛАВНЫЙ ЗАПУСК

# Загружаем прокси
PROXIES_LIST = open("proxy.txt").read().splitlines()

# Запуск сбора
# Имя файла с результатами, количество процессов, номер последней пройденной партии (0)
asyncio.run(acts_runner(sys.argv[1], 100, 0))

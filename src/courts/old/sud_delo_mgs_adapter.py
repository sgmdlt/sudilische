import aiohttp
import asyncio
import docx2txt
import gc
import json
import pdfplumber
import random
import subprocess
import sys
import tempfile
import re

from striprtf.striprtf import rtf_to_text

from datetime import datetime

# Заголовки для запроса
HEADERS = {
    "User-Agent":
    "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) " +
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Mobile Safari/537.36"
}
setattr(asyncio.sslproto._SSLProtocolTransport, "_start_tls_compatible", True)


# Главный обработчик списка карточек
async def get_acts(task_number, results, input_filename):
    global PROXIES_LIST

    # Случайная задержка по времени для разводки запросов
    await asyncio.sleep(random.uniform(1, 10))

    # Запускаем сессию для каждого процесса
    async with aiohttp.ClientSession(
        headers=HEADERS, timeout=aiohttp.ClientTimeout(total=30)) as session:

        # Проверяем решения по списку
        for result in results:

            # Заполняем базовые поля из открытых данных
            formatted_case = {
                "region": "Город Москва",
                "court_name": result["courtName"],
                "case_number": result["number"],
                "id_uid": result["uid"],
                "judge": result["judge"],
                "date_reg": result["dateReg"],
                "court_code": result["courtCode"],
                "case_category_article": result["mainArticle"],
                "case_category_sub_type": result["category"],
                "date_result": result["dateFinal"],
                "date_legal": result["dateLegal"],
                "result": result["publishingState"]
            }

            # Формируем идентификатор дела
            case_id = formatted_case["court_code"] + "_" + formatted_case[
                "date_reg"] + "_"
            if all(char not in formatted_case["case_number"]
                   for char in ["(", "~", "∼"]):
                case_id += formatted_case[
                    "case_number"][:formatted_case["case_number"].
                                   rfind("/")].replace("/", "-")
            elif formatted_case["date_reg"][:4] in formatted_case[
                    "case_number"]:
                case_id += re.search(
                    r"([^ ~∼\(]+)/" + formatted_case["date_reg"][:4],
                    formatted_case["case_number"]).group(1).replace("/", "-")
            else:
                case_id += re.search(
                    r"([^ ~∼\(]+)/20",
                    formatted_case["case_number"]).group(1).replace("/", "-")
            formatted_case["id_final"] = case_id

            # Выделяем тип дела и инстанцию
            match result["productionType"]:
                case "Гражданское":
                    formatted_case["case_type"] = "Гражданское дело"
                case "Уголовное":
                    formatted_case["case_type"] = "Уголовное дело"
                case "Об административных правонарушениях":
                    formatted_case[
                        "case_type"] = "Дело об административном правонарушении"
                case "Административное":
                    formatted_case["case_type"] = "Административное дело"
                case "Производства по материалам":
                    formatted_case["case_type"] = "Производство по материалам"
                case "Первичные документы":
                    formatted_case["case_type"] = "Первичные документы"
                case _:
                    print("Неизвестный тип дела:", result["productionType"])

            match result["instance"]:
                case "Первая инстанция":
                    formatted_case["instance"] = "Первая инстанция"
                case "Апелляционная инстанция":
                    formatted_case["instance"] = "Апелляция"
                case "Кассационная инстанция":
                    formatted_case["instance"] = "Кассация"
                case "Надзорная инстанция":
                    formatted_case["instance"] = "Надзор"
                case _:
                    print("Неизвестная инстанция:", result["instance"])

            # Подсудимые
            if formatted_case["case_type"] in [
                    "Уголовное дело",
                    "Дело об административном правонарушении",
                    "Производство по материалам"
            ]:
                if "participants" in result:
                    defendants = []
                    for person in result["participants"]:
                        articles = []
                        if "codexArticles" in person:
                            for article in person["codexArticles"]:
                                articles.append(article["name"])
                        if not articles and "mainArticle" in result and result[
                                "mainArticle"]:
                            articles.append(result["mainArticle"])
                        defendants.append(" - ".join([
                            person["categoryName"], person["displayName"],
                            "; ".join(articles)
                        ]))
                    formatted_case["defendants"] = "\n".join(defendants)

            # Стороны по делу
            if "participants" in result:
                participants = []
                for person in result["participants"]:
                    articles = []
                    if "codexArticles" in person:
                        for article in person["codexArticles"]:
                            articles.append(article["name"])
                    participants.append(" - ".join([
                        person["categoryName"], person["displayName"],
                        "; ".join(articles)
                    ]))
                formatted_case["participants"] = "\n".join(participants)

            # Преобразуем незаполненные поля
            for key in formatted_case:
                if formatted_case[key] is None:
                    formatted_case[key] = ""

            # ТЕКСТЫ РЕШЕНИЙ
            # Если есть тексты решений, скачиваем их
            if "attachments" in result and result["attachments"]:
                formatted_case["document_text"] = ""
                for doc in result["attachments"]:

                    # Скачиваем содержимое по ссылке байтами
                    got_resp = False
                    resp_bytes = b""
                    resp_ext = ""
                    output_string = ""
                    attempts = 0
                    while not got_resp:
                        try:
                            async with session.get(
                                    doc["link"],
                                    proxy="http://" +
                                    PROXIES_LIST[task_number %
                                                 len(PROXIES_LIST)]) as resp:
                                resp_bytes = await resp.read()

                                # Если файл на месте, записываем расширение
                                if resp.status == 200:
                                    resp_ext = resp.headers[
                                        "Content-Disposition"][
                                            resp.headers["Content-Disposition"]
                                            .rfind("."):].lower().replace(
                                                ";", "").replace("\"", "")
                                    got_resp = True
                                    await asyncio.sleep(2)

                                # При ошибке сервера ждём и пробуем снова
                                elif resp.status != 404:
                                    await asyncio.sleep(5)

                                # Если файл не найден, сразу пропускаем
                                elif resp.status == 404:
                                    got_resp = True
                                    await asyncio.sleep(2)

                        # Обработка ошибок
                        except Exception as e:
                            if isinstance(e, TimeoutError):
                                print(f"🤖 {task_number:>2d}",
                                      "Нет ответа 30 секунд... (" +
                                      doc["link"][:doc["link"].find("/", 10)] +
                                      ")",
                                      sep=" | ")
                            elif "Connect call failed" in str(e):
                                print(f"🤖 {task_number:>2d}",
                                      "Сбой прокси",
                                      sep=" | ")
                                await asyncio.sleep(60)
                                attempts = 0

                            else:
                                print(f"🤖 {task_number:>2d}",
                                      str(e),
                                      sep=" | ")
                                print(doc["link"])
                                await asyncio.sleep(5)

                        if attempts > 10:
                            got_resp = True
                        else:
                            attempts += 1

                    # Сохраняем во временный файл и забираем весь текст
                    with tempfile.NamedTemporaryFile(suffix=resp_ext) as temp:
                        temp.write(resp_bytes)
                        temp.flush()

                        # Пробуем все виды расшифровки сразу
                        # Сначала пробудем .doc
                        p = subprocess.Popen("antiword -m UTF-8.txt -w 0 " +
                                             temp.name,
                                             stdout=subprocess.PIPE,
                                             shell=True)
                        stdout, stderr = p.communicate()
                        output_string = stdout.decode("utf-8", "ignore")

                        # Если он ничего не дал, пробуем .docx
                        if not output_string:
                            try:
                                output_string = docx2txt.process(temp.name)
                            except Exception:
                                pass

                        # Если он ничего не дал, пробуем .pdf
                        if not output_string:
                            try:
                                pdf = pdfplumber.open(temp.name)
                                for page in pdf.pages:
                                    output_string += page.extract_text()
                            except Exception:
                                pass

                        # Если он ничего не дал, пробуем RTF
                        if not output_string:
                            try:
                                output_string = rtf_to_text(
                                    resp_bytes.decode(), errors="ignore")
                            except Exception:
                                pass

                        # В последнюю очередь пробуем TXT с двумя кодировками
                        if not output_string:
                            try:
                                output_string += resp_bytes.decode(
                                    "1251", "ignore")
                                output_string += resp_bytes.decode(
                                    "utf-8", "ignore")
                            except Exception:
                                pass

                        # Если необычное расширение, сигналим
                        if resp_ext not in [
                                "", ".doc", ".docx", ".rtf", ".pdf", ".txt",
                                ".png", ".jpg", ".jpeg", ".tiff"
                        ]:
                            print(f"🤖 {task_number:>2d}",
                                  "НОВЫЙ ФОРМАТ ДОКУМЕНТА:",
                                  resp_ext,
                                  sep=" | ")
                            print(result["url"])

                        temp.close()

                    # Если получен текст, сохраняем результат в файл
                    if output_string and "временно недоступен" not in output_string:

                        if resp_ext not in [
                                ".png", ".jpg", ".jpeg", ".tiff"
                        ] and "/Image" not in output_string:
                            # Очищаем от неразрывных пробелов
                            output_string = output_string.replace(
                                u"\u00A0", " ")

                            result_str = ""
                            for key in doc:
                                result_str += key + ": " + doc[key] + "\n"
                            formatted_case[
                                "document_text"] += result_str + "\n\n\n" + output_string + "\n\n\n"

                        # TODO Сохраняем файл отдельно
                        else:
                            pass

            # Записываем в файл по коду суда
            open(
                "cases_by_court/" + formatted_case["court_code"][2:4] + "/" +
                formatted_case["court_code"] + ".jsonl", "a").write(
                    json.dumps(formatted_case, ensure_ascii=False).replace(
                        u"\u00A0", " ") + "\n")

            # Отчитываемся каждые 100 решений
            if results.index(result) % 1000 == 0:
                print(f"🤖 {task_number:>2d}",
                      "{:>4.0%}".format(
                          (results.index(result) + 1) / len(results)),
                      sep=" | ")

    print(f"🤖 {task_number:>2d}", "🟢 ЗАКОНЧИЛ", sep=" | ")


# Запуск параллельной обработки решений
async def acts_runner(input_filename, tasks, last_part):
    print("Список дел", input_filename)

    # Загружаем список результатов
    case_list = []
    case_count = 1
    part_count = 1
    gather_start = datetime.now()
    with open(input_filename) as input_file:
        for line in input_file:
            if line not in ["[\n", "]"]:
                line = line[line.find("{"):line.rfind("}") + 1]
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
                                case_batch = case_list[i * batch_size:i *
                                                       batch_size + batch_size]
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
                            int((datetime.now() -
                                 gather_start).total_seconds() / 3600),
                            "час(ов)")

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
asyncio.run(acts_runner(sys.argv[1], 20, 0))

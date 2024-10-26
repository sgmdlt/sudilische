import logging

import requests

logger = logging.getLogger(__name__)

class Downloader:
    def __init__(self, proxies_list=None, captcha_handler=None):
        """
        Инициализация загрузчика.

        :param proxies_list: Список прокси-серверов.
        :param captcha_handler: Обработчик капчи.
        """
        self.proxies_list = proxies_list or []
        self.captcha_handler = captcha_handler
        self.proxy_index = 0  # Индекс текущего прокси-сервера
        self.session = None

    def __enter__(self):
        """
        Начало контекстного менеджера.
        """
        self.session = requests.Session()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Завершение контекстного менеджера.
        """
        if self.session:
            self.session.close()

    def _get_next_proxy(self):
        """
        Получает следующий прокси из списка.

        :return: Строка с адресом прокси или None.
        """
        if not self.proxies_list:
            return None
        proxy = self.proxies_list[self.proxy_index % len(self.proxies_list)]
        self.proxy_index += 1
        return f"http://{proxy}"

    def send_request(self, url: str) -> str:
        """
        Отправляет запрос по указанному URL и возвращает ответ.
        Обрабатывает капчу, если это необходимо.

        :param url: URL для запроса.
        :return: Текст ответа.
        """
        if not self.session:
            raise RuntimeError("Сессия не инициализирована. Используйте 'with Downloader()'.")

        verify_cert = True

        proxy = self._get_next_proxy()
        logger.info(f"Отправка запроса к {url} через прокси {proxy}")
        try:
            resp = self.session.get(url, proxies={'http': proxy, 'https': proxy}, verify=verify_cert)
            resp.encoding = resp.encoding or 'windows-1251'
            resp.raise_for_status()

        except requests.RequestException as e:
            logger.warning("Суд временно недоступен")
            logger.error(f"{resp.status_code} {resp.reason} при обращении к {url}")
            raise e

        text = resp.text
        if resp.ok or resp.status_code == 499:
            if self._is_captcha_required(text):
                logger.warning("Капча обнаружена. Начинаем обработку капчи.")
                text = self.captcha_handler.handle_captcha(self.session, url)
                if text:
                    return text
                else:
                    logger.error("Не удалось решить капчу.")
                    return None
        return text

    def _is_captcha_required(self, text: str) -> bool:
        """
        Проверяет, требуется ли ввод капчи на основе текста ответа.

        :param text: Текст ответа сервера.
        :return: True, если требуется капча, иначе False.
        """
        CAPTCHA_ERROR_MESSAGES = ["Ожидается ввод номера с картинки", "введите символы с картинки"]
        return any(error in text for error in CAPTCHA_ERROR_MESSAGES)

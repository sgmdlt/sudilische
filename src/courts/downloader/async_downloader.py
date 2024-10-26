import asyncio
import logging
import random

import aiohttp

logger = logging.getLogger(__name__)

class Downloader:
    def __init__(self, proxies_list=None, captcha_handler=None):
        """
        Инициализация загрузчика.

        :param proxies_list: Список прокси-серверов.
        :param captcha_handler: Обработчик капчи.
        :param max_retries: Максимальное количество попыток запроса.
        """
        self.proxies_list = proxies_list or []
        self.captcha_handler = captcha_handler
        self.proxy_index = 0  # Индекс текущего прокси-сервера
        self.session = None

    async def __aenter__(self):
        """
        Начало контекстного менеджера.
        """
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        """
        Завершение контекстного менеджера.
        """
        if self.session:
            await self.session.close()

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

    async def send_request(self, url: str) -> str:
        """
        Отправляет запрос по указанному URL и возвращает ответ.
        Обрабатывает капчу, если это необходимо.

        :param url: URL для запроса.
        :return: Текст ответа.
        """
        if not self.session:
            raise RuntimeError("Сессия не инициализирована. Используйте 'async with Downloader()'.")

        verify_cert = True

        proxy = self._get_next_proxy()
        logger.info(f"Отправка запроса к {url} через прокси {proxy}")
        await asyncio.sleep(random.uniform(1, 3))

        try:
            async with self.session.get(url, proxy=proxy, ssl=verify_cert) as resp:
                encoding = resp.get_encoding() or 'windows-1251'
                text = await resp.text(encoding=encoding)

                if resp.ok or resp.status == 499:
                    if self._is_captcha_required(text):
                        logger.warning("Капча обнаружена. Начинаем обработку капчи.")
                        text = await self.captcha_handler.handle_captcha(self.session, url)
                        if text:
                            return text
                        else:
                            logger.error("Не удалось решить капчу.")
                            return None
                    else:
                        logger.debug(f"Успешно получен ответ от {url}")
                        return text

                if resp.status in [502, 503]:
                    logger.warning("Суд временно недоступен")
                    await asyncio.sleep(60)

                    logger.error(f"{resp.status} {resp.reason} при обращении к {url}")
                    await asyncio.sleep(5)
                    return text
        except aiohttp.ClientError as e:
            logger.exception(f"Ошибка сети при запросе к {url}: {e}")
        await asyncio.sleep(5)
        raise Exception(f"Не удалось получить данные с {url}")

    def _is_captcha_required(self, text: str) -> bool:
        """
        Проверяет, требуется ли ввод капчи на основе текста ответа.

        :param text: Текст ответа сервера.
        :return: True, если требуется капча, иначе False.
        """
        CAPTCHA_ERROR_MESSAGES = ["Ожидается ввод номера с картинки", "введите символы с картинки"]
        return any(error in text for error in CAPTCHA_ERROR_MESSAGES)

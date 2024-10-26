import re
import base64
from bs4 import BeautifulSoup
import logging

logger = logging.getLogger(__name__)

class CaptchaHandler:
    def __init__(self, captcha_solver):
        self.captcha_solver = captcha_solver

    async def handle_captcha(self, session, url: str) -> str:
        """
        Обрабатывает капчу и возвращает текст страницы после успешного решения.

        :param url: Оригинальный URL запроса.
        :return: Текст ответа страницы после решения капчи.
        """
        if not self.captcha_solver:
            logger.error("Сервис для решения капчи не задан.")
            return None

        # Загружаем страницу с капчей
        async with session.get(url) as resp:
            encoding = resp.get_encoding() or 'windows-1251'
            text = await resp.text(encoding=encoding)
            soup = BeautifulSoup(text, 'html.parser')

        # Извлекаем captchaid
        captcha_id = self._extract_captcha_id(soup)
        if not captcha_id:
            logger.error("Не удалось извлечь captchaid.")
            return None

        # Извлекаем изображение капчи
        captcha_image_bytes = self._extract_captcha_image(soup)
        if not captcha_image_bytes:
            logger.error("Не удалось извлечь изображение капчи.")
            return None

        # Вызываем внешний модуль для решения капчи
        captcha_solution = await self.captcha_solver.solve_captcha(captcha_image_bytes)
        if not captcha_solution or len(captcha_solution) != 5:
            logger.error("Не удалось решить капчу.")
            return None

        logger.info(f"Капча решена: {captcha_solution}")

        # Формируем captcha_pack
        captcha_pack = {
            "captcha": captcha_solution,
            "captchaid": captcha_id
        }

        # Обновляем URL с капчей
        url_with_captcha = self._update_url_with_captcha(url, captcha_pack)

        # Повторяем запрос с решенной капчей
        return await self.send_request(url_with_captcha)

    def _update_url_with_captcha(self, url: str, captcha_pack: dict) -> str:
        """
        Обновляет URL, добавляя в него параметры капчи.

        :param url: Оригинальный URL.
        :param captcha_pack: Словарь с captcha и captchaid.
        :return: Обновленный URL с параметрами капчи.
        """
        # Убираем старые параметры капчи из URL
        url = re.sub(r"&captcha=[^&]+", "", url)
        url = re.sub(r"&captchaid=[^&]+", "", url)

        # Добавляем новые параметры капчи
        url += f"&captcha={captcha_pack['captcha']}&captchaid={captcha_pack['captchaid']}"
        return url

    def _extract_captcha_id(self, soup: BeautifulSoup) -> str:
        """
        Извлекает идентификатор капчи из HTML.

        :param soup: Объект BeautifulSoup с загруженной страницей.
        :return: Идентификатор капчи.
        """
        input_tag = soup.find('input', {'name': 'captchaid'})
        if input_tag and 'value' in input_tag.attrs:
            return input_tag['value']
        else:
            logger.error("Не удалось найти captchaid на странице.")
            return None

    def _extract_captcha_image(self, soup: BeautifulSoup) -> bytes:
        """
        Извлекает изображение капчи в байтах из HTML.

        :param soup: Объект BeautifulSoup с загруженной страницей.
        :return: Байтовое представление изображения капчи.
        """
        img_tag = soup.select_one("td:has(> input[name=captcha]) img, div:has(> input[name=captcha]) img")
        if img_tag and 'src' in img_tag.attrs:
            image_base64 = img_tag['src'].split(',', 1)[-1]
            try:
                image_bytes = base64.b64decode(image_base64)
                return image_bytes
            except Exception as e:
                logger.error(f"Ошибка при декодировании изображения капчи: {e}")
                return None
        else:
            logger.error("Не удалось найти изображение капчи на странице.")
            return None

from bs4 import BeautifulSoup


class PageLoader:
    def __init__(self, url, downloader):
        self.url = url
        self.downloader = downloader

    def load_pages(self):
        while self.url:
            text = self.downloader.send_request(self.url)
            if text:
                yield text

            next_url = self._get_next_url(text)
            self.url = next_url

    def _get_next_url(self, text):
        soup = BeautifulSoup(text, 'html.parser')
        for link in soup.select(
                "a[title='Следующая страница'], " +
                "ul.pagination a:-soup-contains-own('»')"):
            return link["href"]
        return None

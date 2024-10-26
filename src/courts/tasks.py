from courts.captcha.handler import CaptchaHandler
from courts.captcha.solver_stub import CaptchaSolver
from courts.celery_app import app
from courts.downloader.pager import PageLoader
from courts.downloader.sync_downloader import Downloader
from courts.parser.parser import parse_page

captcha_solver = CaptchaSolver()

with open('proxy.txt', 'r') as f:
    PROXIES_LIST = [line.strip() for line in f if line.strip()]

# TODO: add retry for custom exceptions
@app.task(autoretry_for=(Exception,),
          retry_kwargs={'max_retries': 2, 'default_retry_delay': 30})
def search_cases(url):
    with Downloader(captcha_handler=CaptchaHandler(captcha_solver), proxies_list=PROXIES_LIST) as downloader:
        result = []
        loader = PageLoader(url, downloader)
        for page in loader.load_pages():
            case_rows = parse_page(page, url)
            result.extend(case_rows)
        #parsed_row["region"] = court["region"]
        #parsed_row["court_name"] = court["name"]
        return result

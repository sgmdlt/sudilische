from pathlib import Path

import pytest
from vcr import VCR

my_vcr = VCR(decode_compressed_response=True, record_mode='once')

def custom_before_record_response(response):
    if 'content-type' in response['headers']:
        if 'charset' not in response['headers']['content-type']:
            response['headers']['content-type'] += '; charset=windows-1251'

    if 'body' in response and isinstance(response['body'], bytes):
        encodings = ['windows-1251', 'utf-8', 'iso-8859-1', 'cp1252']
        for encoding in encodings:
            try:
                response['body'] = response['body'].decode(encoding)
                print(f"Successfully decoded using {encoding}")
                break
            except UnicodeDecodeError:
                print(f"Failed to decode using {encoding}")
        else:
            print("Failed to decode with all attempted encodings")
            # Если все попытки декодирования не удались, оставляем тело как есть
            response['body'] = str(response['body'])

    return response

my_vcr.before_record_response = custom_before_record_response

@pytest.fixture(scope='module')
def vcr_cassette_dir(request):
    return 'tests/vcr_cassettes'


@pytest.fixture(scope='module')
def vcr_config():
    return {
        "decode_compressed_response": True,
        "record_mode": "once",
        "before_record_response": custom_before_record_response,
    }


def get_fixtures_path():
    return Path(__file__).parent / "fixtures"

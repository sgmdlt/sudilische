import json
from pathlib import Path

import pytest

from courts.old.sud_delo_get_all_links import court_runner


@pytest.mark.asyncio
@pytest.mark.vcr()
async def test_court_runner(tmp_path):
    # Подготовка тестовых данных
    output_filename = "test_output"
    court_codes = "RS+OS"
    query_set = [
        {
            "type": "УК",
            "instance": 1,
            "article": "",
            "participant": "",
            "entry_date_from": "01.01.2023",
            "entry_date_to": "31.01.2023",
            "mat_category": ["", "", ""],
            "download_mat_cat": False
        }
    ]
    tasks = 1

    output_path = str(tmp_path / output_filename)
    await court_runner(output_path, court_codes, query_set, tasks)

    output_file = Path(f"{output_path}.jsonl")
    assert output_file.exists(), "Файл с результатами не создан"

    with open(output_file, "r", encoding="utf-8") as f:
        lines = f.readlines()
        assert len(lines) > 0, "Файл с результатами пуст"

        first_result = json.loads(lines[0])
        required_fields = ["region", "court_name", "case_number", "date_reg", "link", "ID"]
        for field in required_fields:
            assert field in first_result, f"Поле {field} отсутствует в результате"

    assert first_result["region"] == "Республика Адыгея"
    assert first_result["court_name"] == "Гиагинский районный суд"
    assert first_result["case_number"] == "1-25/2023"


    error_file = Path(f"{output_path}_errors.jsonl")
    assert not error_file.exists(), "Файл с ошибками был создан"

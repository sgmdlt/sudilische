import json

import pytest
from conftest import get_fixtures_path

from courts.tasks import search_cases


def get_links_fixture():
    links = []
    with open(get_fixtures_path() / "links.jsonl") as f:
        for line in f:
            links.append(json.loads(line.strip()))
    return links


@pytest.mark.vcr()
def test_search_cases():
    url = "http://giaginsky.adg.sudrf.ru/modules.php?name=sud_delo&name_op=r&nc=1&case_type=0&delo_id=1540006&U1_CASE__JUDGE=&delo_table=u1_case&u1_case__ENTRY_DATE1D=01.01.2023&u1_case__ENTRY_DATE2D=31.01.2023&U1_DEFENDANT__NAMESS=&U1_DEFENDANT__LAW_ARTICLESS="
    result = search_cases(url)
    assert result[0]["link"] == get_links_fixture()[0]["link"]

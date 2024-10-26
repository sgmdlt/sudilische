import pytest

from courts.generator.query_builder import QueryParams, get_url
from courts.generator.url_generator import generate_urls


def create_params(case_type, instance, entry_date_from, entry_date_to, court_type="1", court_id="12RS0001"):
    return {
        "court_url": "http://example.com/court?",
        "court_type": court_type,
        "court_id": court_id,
        "type": case_type,
        "instance": instance,
        "article": "105",
        "participant": "Иванов",
        "entry_date_from": entry_date_from,
        "entry_date_to": entry_date_to,
        "mat_category": ["", "", ""],
        "download_mat_cat": False
    }

@pytest.mark.parametrize("case_type, delo_id, case_stub, parts_stub", [
    ("УК", "1540006", "u1_case", "U1_DEFENDANT"),
    ("КоАП", "1500001", "adm_case", "adm_parts"),
    ("Материал", "1610001", "m_case", "M_PARTS"),
    ("ГК", "1540005", "g1_case", "G1_PARTS"),
    ("КАС", "41", "p1_case", "P1_PARTS"),
    ("ГДП", "1610002", "gdp_case", "GDP"),
])
def test_get_url_type_1_instance_1(case_type, delo_id, case_stub, parts_stub):
    params = create_params(case_type, 1, "01.01.2023", "31.01.2023")
    result = get_url(QueryParams(**params))
    assert "http://example.com/court?name=sud_delo&name_op=r&nc=1&case_type=0" in result
    assert f"delo_id={delo_id}" in result
    assert f"delo_table={case_stub}" in result
    assert f"{case_stub}__ENTRY_DATE1D=01.01.2023" in result
    assert f"{case_stub}__ENTRY_DATE2D=31.01.2023" in result

@pytest.mark.parametrize("case_type, delo_id, case_stub, parts_stub", [
    ("УК", "4", "u2_case", "U2_DEFENDANT"),
    ("КоАП", "1502001", "adm1_case", "adm1_parts"),
    ("ГК", "5", "g2_case", "G2_PARTS"),
    ("КАС", "42", "p2_case", "P2_PARTS"),
])
def test_get_url_type_1_instance_2(case_type, delo_id, case_stub, parts_stub):
    params = create_params(case_type, 2, "01.02.2023", "28.02.2023")
    result = get_url(QueryParams(**params))
    assert "http://example.com/court?name=sud_delo&name_op=r&nc=1&case_type=0" in result
    assert f"delo_id={delo_id}" in result
    assert f"delo_table={case_stub}" in result
    assert f"{case_stub}__ENTRY_DATE1D=01.02.2023" in result
    assert f"{case_stub}__ENTRY_DATE2D=28.02.2023" in result

def test_get_url_type_1_instance_3_uk():
    params = create_params("УК", 3, "01.03.2023", "31.03.2023")
    result = get_url(QueryParams(**params))
    assert "http://example.com/court?name=sud_delo&name_op=r&nc=1&case_type=0" in result
    assert "delo_id=2450001" in result
    assert "delo_table=u33_case" in result
    assert "u33_case__ENTRY_DATE1D=01.03.2023" in result
    assert "u33_case__ENTRY_DATE2D=31.03.2023" in result

@pytest.mark.parametrize("case_type, delo_id", [
    ("УК", "1540006"),
    ("КоАП", "1500001"),
    ("Материал", "1610001"),
    ("ГК", "1540005"),
    ("КАС", "41"),
    ("ГДП", "1610002"),
])
def test_get_url_type_2_instance_1(case_type, delo_id):
    params = create_params(case_type, 1, "01.04.2023", "30.04.2023", court_type="2", court_id="12RS0002")
    result = get_url(QueryParams(**params))
    assert "http://example.com/court?name=sud_delo&name_op=r" in result
    assert f"_deloId={delo_id}" in result
    assert "case__vnkod=12RS0002" in result
    assert "case__entry_date1d=01.04.2023" in result
    assert "case__entry_date2d=30.04.2023" in result

@pytest.mark.parametrize("case_type, delo_id", [
    ("УК", "4"),
    ("КоАП", "1502001"),
    ("ГК", "5"),
    ("КАС", "42"),
])
def test_get_url_type_2_instance_2(case_type, delo_id):
    params = create_params(case_type, 2, "01.05.2023", "31.05.2023", court_type="2", court_id="12RS0002")
    result = get_url(QueryParams(**params))
    assert "http://example.com/court?name=sud_delo&name_op=r" in result
    assert f"_deloId={delo_id}" in result
    assert "case__vnkod=12RS0002" in result
    assert "case__entry_date1d=01.05.2023" in result
    assert "case__entry_date2d=31.05.2023" in result


def test_generate_urls():
    params = {
        "court_url": ["http://example.com/court?"],
        "instance": [1],
        "type": ["УК"],
        "entry_date_from": "01.01.2023",
        "entry_date_to": "01.10.2023",
    }
    urls = list(generate_urls(params))
    assert len(urls) == 9
    assert urls[0] == "http://example.com/court?name=sud_delo&name_op=r&nc=1&case_type=0&delo_id=1540006&U1_CASE__JUDGE=&delo_table=u1_case&u1_case__ENTRY_DATE1D=01.01.2023&u1_case__ENTRY_DATE2D=01.02.2023&U1_DEFENDANT__NAMESS=&U1_DEFENDANT__LAW_ARTICLESS="
    assert urls[-1] == "http://example.com/court?name=sud_delo&name_op=r&nc=1&case_type=0&delo_id=1540006&U1_CASE__JUDGE=&delo_table=u1_case&u1_case__ENTRY_DATE1D=01.09.2023&u1_case__ENTRY_DATE2D=01.10.2023&U1_DEFENDANT__NAMESS=&U1_DEFENDANT__LAW_ARTICLESS="

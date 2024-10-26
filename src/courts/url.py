import urllib
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List


@dataclass
class ParamsDTO:
    type: str
    instance: int
    article: str = ""
    participant: str = ""
    entry_date_from: str = ""
    entry_date_to: str = ""
    mat_category: List[str] = field(default_factory=lambda: ["", "", ""])
    download_mat_cat: bool = False
    court_url: str = ""
    court_type: str = ""
    court_id: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

def get_url(params: ParamsDTO) -> str:
    if params.court_type == "1":
        return _get_url_type_1(params)
    elif params.court_type == "2":
        return _get_url_type_2(params)
    else:
        raise ValueError(f"Unsupported court type: {params.court_type}")

def _get_url_type_1(params: ParamsDTO) -> str:
    query = "name=sud_delo&name_op=r&nc=1&case_type=0"
    case_stub = ""
    parts_stub = ""

    if params.instance == 1:
        if params.type == "УК":
            query += "&delo_id=1540006&U1_CASE__JUDGE="
            case_stub = "u1_case"
            parts_stub = "U1_DEFENDANT"
        elif params.type == "КоАП":
            query += "&delo_id=1500001"
            case_stub = "adm_case"
            parts_stub = "adm_parts"
        elif params.type == "Материал":
            query += f"&delo_id=1610001&M_CASE__M_SUB_TYPE={urllib.parse.quote(params.mat_category[0], encoding='1251')}"
            case_stub = "m_case"
            parts_stub = "M_PARTS"
        elif params.type == "ГК":
            query += "&delo_id=1540005"
            case_stub = "g1_case"
            parts_stub = "G1_PARTS"
        elif params.type == "КАС":
            query += "&delo_id=41"
            case_stub = "p1_case"
            parts_stub = "P1_PARTS"
        elif params.type == "ГДП":
            query += "&delo_id=1610002"
            case_stub = "gdp_case"
            parts_stub = "GDP"
    elif params.instance == 2:
        if params.type == "УК":
            query += "&delo_id=4&new=4&U2_CASE__JUDGE="
            case_stub = "u2_case"
            parts_stub = "U2_DEFENDANT"
        elif params.type == "КоАП":
            query += "&delo_id=1502001"
            case_stub = "adm1_case"
            parts_stub = "adm1_parts"
        elif params.type == "ГК":
            query += "&delo_id=5&new=5"
            case_stub = "g2_case"
            parts_stub = "G2_PARTS"
        elif params.type == "КАС":
            query += "&delo_id=42"
            case_stub = "p2_case"
            parts_stub = "P2_PARTS"
    elif params.instance == 3:
        if params.type == "УК":
            query += "&delo_id=2450001&new=2450001"
            case_stub = "u33_case"
            parts_stub = "U33_DEFENDANT"

    query += f"&delo_table={case_stub}"
    query += f"&{case_stub}__ENTRY_DATE1D={params.entry_date_from}"
    query += f"&{case_stub}__ENTRY_DATE2D={params.entry_date_to}"
    query += f"&{parts_stub}__NAMESS={urllib.parse.quote_plus(params.participant, encoding='1251')}"

    if params.type in ["УК", "КоАП"]:
        query += f"&{parts_stub}__LAW_ARTICLESS={urllib.parse.quote_plus(params.article, encoding='1251')}"
    elif params.type in ["ГК", "КАС"] and params.mat_category[0]:
        query += f"&lawbookarticles%5B%5D={urllib.parse.quote_plus(params.mat_category[0], encoding='1251')}"

    return f"{params.court_url}{query}"

def _get_url_type_2(params: ParamsDTO) -> str:
    query = "name=sud_delo&name_op=r"

    if params.instance == 1:
        if params.type == "УК":
            query += "&_deloId=1540006"
        elif params.type == "КоАП":
            query += "&_deloId=1500001"
        elif params.type == "Материал":
            query += "&_deloId=1610001"
        elif params.type == "ГК":
            query += "&_deloId=1540005"
        elif params.type == "КАС":
            query += "&_deloId=41"
        elif params.type == "ГДП":
            query += "&_deloId=1610002"
    elif params.instance == 2:
        if params.type == "УК":
            query += "&_deloId=4&_new=4"
        elif params.type == "КоАП":
            query += "&_deloId=1502001"
        elif params.type == "ГК":
            query += "&_deloId=5&_new=5"
        elif params.type == "КАС":
            query += "&_deloId=42"

    query += f"&case__vnkod={params.court_id}"
    query += f"&case__entry_date1d={params.entry_date_from}"
    query += f"&case__entry_date2d={params.entry_date_to}"

    return f"{params.court_url}{query}"

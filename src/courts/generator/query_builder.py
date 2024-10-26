import urllib.parse
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Tuple, Union


class Instance(Enum):
    FIRST = 1
    SECOND = 2
    THIRD = 3

class CaseType(Enum):
    CRIMINAL = "УК"
    ADMINISTRATIVE = "КоАП"
    MATERIAL = "Материал"
    CIVIL = "ГК"
    KAS = "КАС"
    GDP = "ГДП"

class CourtType(Enum):
    TYPE_1 = "1"
    TYPE_2 = "2"

@dataclass
class CaseConfig:
    delo_id: str
    case_stub: str
    parts_stub: str
    additional_query: str = ""

@dataclass
class QueryParams:
    type: Union[CaseType, str]
    instance: Union[Instance, int]
    article: str = ""
    participant: str = ""
    entry_date_from: str = ""
    entry_date_to: str = ""
    mat_category: List[str] = field(default_factory=lambda: ["", "", ""])
    download_mat_cat: bool = False
    court_url: str = ""
    court_type: Union[CourtType, str] = CourtType.TYPE_1
    court_id: str = ""

    def __post_init__(self):
        if isinstance(self.type, str):
            self.type = CaseType(self.type)
        if isinstance(self.instance, int):
            self.instance = Instance(self.instance)
        if isinstance(self.court_type, str):
            self.court_type = CourtType(self.court_type)

CASE_CONFIGS: Dict[Tuple[Instance, CaseType], CaseConfig] = {
    (Instance.FIRST, CaseType.CRIMINAL): CaseConfig("1540006", "u1_case", "U1_DEFENDANT", "&U1_CASE__JUDGE="),
    (Instance.FIRST, CaseType.ADMINISTRATIVE): CaseConfig("1500001", "adm_case", "adm_parts"),
    (Instance.FIRST, CaseType.MATERIAL): CaseConfig("1610001", "m_case", "M_PARTS"),
    (Instance.FIRST, CaseType.CIVIL): CaseConfig("1540005", "g1_case", "G1_PARTS"),
    (Instance.FIRST, CaseType.KAS): CaseConfig("41", "p1_case", "P1_PARTS"),
    (Instance.FIRST, CaseType.GDP): CaseConfig("1610002", "gdp_case", "GDP"),
    (Instance.SECOND, CaseType.CRIMINAL): CaseConfig("4", "u2_case", "U2_DEFENDANT", "&new=4&U2_CASE__JUDGE="),
    (Instance.SECOND, CaseType.ADMINISTRATIVE): CaseConfig("1502001", "adm1_case", "adm1_parts"),
    (Instance.SECOND, CaseType.CIVIL): CaseConfig("5", "g2_case", "G2_PARTS", "&new=5"),
    (Instance.SECOND, CaseType.KAS): CaseConfig("42", "p2_case", "P2_PARTS"),
    (Instance.THIRD, CaseType.CRIMINAL): CaseConfig("2450001", "u33_case", "U33_DEFENDANT", "&new=2450001"),
}

def build_query(params: QueryParams) -> str:
    if params.court_type == CourtType.TYPE_1:
        return _build_query_type_1(params)
    elif params.court_type == CourtType.TYPE_2:
        return _build_query_type_2(params)
    else:
        raise ValueError(f"Unsupported court type: {params.court_type}")

def _build_query_type_1(params: QueryParams) -> str:
    query = "name=sud_delo&name_op=r&nc=1&case_type=0"

    config = CASE_CONFIGS.get((params.instance, params.type))
    if not config:
        raise ValueError(f"Unsupported combination: instance={params.instance}, type={params.type}")

    query += f"&delo_id={config.delo_id}{config.additional_query}"

    if params.type == CaseType.MATERIAL:
        query += f"&M_CASE__M_SUB_TYPE={urllib.parse.quote(params.mat_category[0], encoding='1251')}"

    query += f"&delo_table={config.case_stub}"
    query += f"&{config.case_stub}__ENTRY_DATE1D={params.entry_date_from}"
    query += f"&{config.case_stub}__ENTRY_DATE2D={params.entry_date_to}"
    query += f"&{config.parts_stub}__NAMESS={urllib.parse.quote_plus(params.participant, encoding='1251')}"

    if params.type in [CaseType.CRIMINAL, CaseType.ADMINISTRATIVE]:
        query += f"&{config.parts_stub}__LAW_ARTICLESS={urllib.parse.quote_plus(params.article, encoding='1251')}"
    elif params.type in [CaseType.CIVIL, CaseType.KAS] and params.mat_category[0]:
        query += f"&lawbookarticles%5B%5D={urllib.parse.quote_plus(params.mat_category[0], encoding='1251')}"

    return query

def _build_query_type_2(params: QueryParams) -> str:
    query = "name=sud_delo&name_op=r"

    delo_id_map = {
        (Instance.FIRST, CaseType.CRIMINAL): "1540006",
        (Instance.FIRST, CaseType.ADMINISTRATIVE): "1500001",
        (Instance.FIRST, CaseType.MATERIAL): "1610001",
        (Instance.FIRST, CaseType.CIVIL): "1540005",
        (Instance.FIRST, CaseType.KAS): "41",
        (Instance.FIRST, CaseType.GDP): "1610002",
        (Instance.SECOND, CaseType.CRIMINAL): "4",
        (Instance.SECOND, CaseType.ADMINISTRATIVE): "1502001",
        (Instance.SECOND, CaseType.CIVIL): "5",
        (Instance.SECOND, CaseType.KAS): "42",
    }

    delo_id = delo_id_map.get((params.instance, params.type))
    if not delo_id:
        raise ValueError(f"Unsupported combination: instance={params.instance}, type={params.type}")

    query += f"&_deloId={delo_id}"
    if params.instance == Instance.SECOND and params.type in [CaseType.CRIMINAL, CaseType.CIVIL]:
        query += "&_new=4" if params.type == CaseType.CRIMINAL else "&_new=5"

    query += f"&case__vnkod={params.court_id}"
    query += f"&case__entry_date1d={params.entry_date_from}"
    query += f"&case__entry_date2d={params.entry_date_to}"

    return query

def get_url(params: QueryParams) -> str:
    query = build_query(params)
    return f"{params.court_url}{query}"

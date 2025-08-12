from typing import List
from .dal import DAL

def get_domains(db: DAL) -> List[str]:
    df = db.query("SELECT DOMAIN_ID FROM GOV_PLATFORM.CATALOG.DIM_DOMAIN ORDER BY DOMAIN_ID")
    return df["DOMAIN_ID"].tolist() if df is not None and len(df) else []

def get_systems(db: DAL) -> List[str]:
    df = db.query("SELECT SYSTEM_ID FROM GOV_PLATFORM.CATALOG.DIM_SYSTEM ORDER BY SYSTEM_ID")
    return df["SYSTEM_ID"].tolist() if df is not None and len(df) else []

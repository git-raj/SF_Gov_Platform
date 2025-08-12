from typing import Tuple, Optional
from .dal import DAL

def page_visible(db: DAL, role_name: Optional[str], page_name: str, required_level: str = "READ") -> Tuple[bool, str]:
    """Return (allowed, reason). Default-allow when no rules exist."""
    role = (role_name or "").upper() or "PUBLIC"
    page = (page_name or "").upper()

    exists = db.scalar(
        """
        SELECT COUNT(*) FROM GOV_APP.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA='CONFIG' AND TABLE_NAME='ROLE_PAGE_ACCESS'
        """
    )
    if not exists:
        return True, "default_allow (no ROLE_PAGE_ACCESS table)"

    access_level = db.scalar(
        """
        SELECT ACCESS_LEVEL
        FROM GOV_APP.CONFIG.ROLE_PAGE_ACCESS
        WHERE UPPER(ROLE_NAME)=:role AND UPPER(PAGE_NAME)=:page
        QUALIFY ROW_NUMBER() OVER (ORDER BY 1)=1
        """
    , {"role": role, "page": page})

    if access_level is None:
        return True, "default_allow (no matching rule)"

    rank = {"READ": 1, "WRITE": 2, "ADMIN": 3}
    have = rank.get(str(access_level).upper(), 0)
    need = rank.get(required_level.upper(), 1)
    return (have >= need, f"rule={access_level}")

from __future__ import annotations
from typing import Any, Dict, Optional
import pandas as pd
from snowflake.snowpark.context import get_active_session

class DAL:
    """Lightweight Data Access Layer using Snowpark get_active_session()."""
    def __init__(self):
        self.session = get_active_session()
        self._ensure_access_log_table()

    # --- internal helpers ---
    def _subst_params(self, sql: str, params: Optional[Dict[str, Any]]) -> str:
        if not params:
            return sql
        out = sql
        for k, v in params.items():
            placeholder = f":{k}"
            if isinstance(v, str):
                vv = "'" + v.replace("'", "''") + "'"
            elif v is None:
                vv = "NULL"
            else:
                vv = str(v)
            out = out.replace(placeholder, vv)
        return out

    def query(self, sql: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        q = self._subst_params(sql, params)
        try:
            return self.session.sql(q).to_pandas()
        except Exception as e:
            print(f"[DAL.query] {e}")
            return pd.DataFrame()

    def scalar(self, sql: str, params: Optional[Dict[str, Any]] = None):
        df = self.query(sql, params)
        if df is not None and len(df) > 0:
            return list(df.iloc[0].values)[0]
        return None

    def exec(self, sql: str, params: Optional[Dict[str, Any]] = None) -> None:
        q = self._subst_params(sql, params)
        try:
            self.session.sql(q).collect()
        except Exception as e:
            print(f"[DAL.exec] {e}")

    def get_current_role(self) -> Optional[str]:
        try:
            df = self.session.sql("select current_role()").to_pandas()
            return df.iloc[0,0]
        except Exception:
            return None

    # --- telemetry table ---
    def _ensure_access_log_table(self) -> None:
        try:
            exists = self.scalar(
                """
                SELECT COUNT(*) FROM GOV_APP.INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = 'CONFIG' AND TABLE_NAME = 'ACCESS_LOG'
                """
            )
            if not exists:
                self.exec(
                    """
                    CREATE TABLE IF NOT EXISTS GOV_APP.CONFIG.ACCESS_LOG (
                      TS TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
                      ROLE_NAME STRING,
                      PAGE_NAME STRING,
                      ALLOWED BOOLEAN,
                      REASON STRING
                    )
                    """
                )
        except Exception as e:
            print(f"[DAL._ensure_access_log_table] {e}")

    def log_access_attempt(self, role: str, page: str, allowed: bool, reason: str) -> None:
        try:
            self.exec(
                """
                INSERT INTO GOV_APP.CONFIG.ACCESS_LOG(ROLE_NAME, PAGE_NAME, ALLOWED, REASON)
                VALUES (:role, :page, :allowed, :reason)
                """
            , {"role": role, "page": page, "allowed": allowed, "reason": reason})
        except Exception as e:
            print(f"[DAL.log_access_attempt] {e}")

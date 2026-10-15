import asyncio
import json
from typing import Any, Dict, List, Optional
import httpx
from app.core.config import settings
from app.core.exceptions import DatabaseError
from app.core.logger import logger

try:
    import clickhouse_connect
    from clickhouse_connect.driver.client import Client
except ImportError:
    clickhouse_connect = None
    Client = None


class ClickHouseManager:
    """Resilient ClickHouse manager supporting both clickhouse-connect native driver and high-speed HTTP fallback."""

    def __init__(self):
        self._client: Optional[Any] = None
        self.host = settings.CLICKHOUSE_HOST
        self.port = settings.CLICKHOUSE_PORT
        self.user = settings.CLICKHOUSE_USER
        self.password = settings.CLICKHOUSE_PASSWORD
        self.database = settings.CLICKHOUSE_DB
        self.secure = settings.CLICKHOUSE_SECURE
        self._http_url = f"http{'s' if self.secure else ''}://{self.host}:{self.port}"

    def connect(self) -> Any:
        if clickhouse_connect is not None:
            if self._client is None:
                try:
                    logger.info(f"Connecting to ClickHouse via native driver at {self.host}:{self.port}...")
                    self._client = clickhouse_connect.get_client(
                        host=self.host,
                        port=self.port,
                        username=self.user,
                        password=self.password,
                        database=self.database,
                        secure=self.secure,
                        connect_timeout=10,
                        send_receive_timeout=30,
                    )
                    logger.info("✓ ClickHouse client successfully established.")
                except Exception as e:
                    logger.error(f"ClickHouse driver connection failed: {e}")
                    raise DatabaseError(f"ClickHouse connection failed: {str(e)}")
            return self._client
        return None

    def close(self) -> None:
        if self._client is not None:
            try:
                self._client.close()
                logger.info("ClickHouse connection closed.")
            except Exception as e:
                logger.warning(f"Error closing ClickHouse client: {e}")
            finally:
                self._client = None

    async def ping(self) -> bool:
        """Asynchronously verify ClickHouse server liveness."""
        if clickhouse_connect is not None:
            def _ping() -> bool:
                try:
                    client = self.connect()
                    res = client.command("SELECT 1")
                    return res == 1
                except Exception as e:
                    logger.warning(f"ClickHouse ping failed: {e}")
                    return False
            return await asyncio.to_thread(_ping)

        # HTTP Fallback
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                resp = await client.get(f"{self._http_url}/ping")
                return resp.status_code == 200 and resp.text.strip() == "Ok."
        except Exception as e:
            logger.warning(f"ClickHouse HTTP ping failed: {e}")
            return False

    async def query(self, query_str: str, parameters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Execute an analytical SELECT query asynchronously and return dictionaries."""
        if clickhouse_connect is not None:
            def _execute():
                try:
                    client = self.connect()
                    result = client.query(query_str, parameters=parameters)
                    column_names = result.column_names
                    rows = [dict(zip(column_names, row)) for row in result.result_rows]
                    return rows
                except Exception as e:
                    logger.error(f"ClickHouse query execution failed: {e} | Query: {query_str}")
                    raise DatabaseError(f"ClickHouse query failed: {str(e)}")
            return await asyncio.to_thread(_execute)

        # HTTP Fallback with FORMAT JSONEachRow
        trimmed_query = query_str.strip().rstrip(";")
        if "FORMAT" not in trimmed_query.upper():
            trimmed_query += " FORMAT JSONEachRow"

        params = {"database": self.database}
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                auth = (self.user, self.password) if self.user else None
                resp = await client.post(self._http_url, params=params, content=trimmed_query, auth=auth)
                if resp.status_code != 200:
                    raise DatabaseError(f"ClickHouse HTTP query failed ({resp.status_code}): {resp.text}")

                lines = resp.text.strip().splitlines()
                return [json.loads(line) for line in lines if line.strip()]
        except Exception as e:
            logger.error(f"ClickHouse HTTP query error: {e}")
            raise DatabaseError(f"ClickHouse query failed: {str(e)}")

    async def command(self, cmd_str: str, parameters: Optional[Dict[str, Any]] = None) -> Any:
        """Execute DDL or state-altering commands."""
        if clickhouse_connect is not None:
            def _command():
                try:
                    client = self.connect()
                    return client.command(cmd_str, parameters=parameters)
                except Exception as e:
                    logger.error(f"ClickHouse command failed: {e} | Command: {cmd_str}")
                    raise DatabaseError(f"ClickHouse command error: {str(e)}")
            return await asyncio.to_thread(_command)

        # HTTP Fallback
        params = {"database": self.database}
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                auth = (self.user, self.password) if self.user else None
                resp = await client.post(self._http_url, params=params, content=cmd_str, auth=auth)
                if resp.status_code != 200:
                    raise DatabaseError(f"ClickHouse HTTP command failed ({resp.status_code}): {resp.text}")
                return resp.text.strip()
        except Exception as e:
            logger.error(f"ClickHouse command error: {e}")
            raise DatabaseError(f"ClickHouse command error: {str(e)}")

    async def insert(self, table: str, data: List[List[Any]], column_names: List[str]) -> None:
        """Execute high-speed batch insert into ClickHouse."""
        if not data:
            return

        if clickhouse_connect is not None:
            def _insert():
                try:
                    client = self.connect()
                    client.insert(table=table, data=data, column_names=column_names)
                except Exception as e:
                    logger.error(f"ClickHouse batch insert failed on table '{table}': {e}")
                    raise DatabaseError(f"ClickHouse insert failed: {str(e)}")
            await asyncio.to_thread(_insert)
            return

        # HTTP Fallback using JSONEachRow format
        target_table = f"{self.database}.{table}" if "." not in table else table
        cols_clause = f"({', '.join(f'`{c}`' for c in column_names)})"
        insert_header = f"INSERT INTO {target_table} {cols_clause} FORMAT JSONEachRow\n"

        rows_json = []
        for row in data:
            row_dict = {}
            for col, val in zip(column_names, row):
                # Handle dates and datetimes
                if hasattr(val, "isoformat"):
                    row_dict[col] = val.isoformat()
                else:
                    row_dict[col] = val
            rows_json.append(json.dumps(row_dict))

        payload = insert_header + "\n".join(rows_json)

        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                auth = (self.user, self.password) if self.user else None
                resp = await client.post(self._http_url, content=payload, auth=auth)
                if resp.status_code != 200:
                    raise DatabaseError(f"ClickHouse batch insert failed ({resp.status_code}): {resp.text}")
        except Exception as e:
            logger.error(f"ClickHouse batch insert error on table '{table}': {e}")
            raise DatabaseError(f"ClickHouse insert failed: {str(e)}")


ch_manager = ClickHouseManager()


def get_clickhouse_manager() -> ClickHouseManager:
    return ch_manager

import aiomysql
import json
import datetime
from astrbot import logger
from typing import Optional
from .base import BaseStorage
from .mysql_migrations import MigrationManager

class MySQLStorage(BaseStorage):
    def __init__(self, host, port, database, username, password):
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password
        self.pool: Optional[aiomysql.Pool] = None

    async def initialize(self) -> None:
        try:
            self.pool = await aiomysql.create_pool(
                host=self.host,
                port=self.port,
                user=self.username,
                password=self.password,
                db=self.database,
                autocommit=True,
                minsize=1,
                maxsize=5
            )

            # 使用迁移管理器进行数据库初始化和升级
            migration_mgr = MigrationManager(self.pool)
            await migration_mgr.upgrade_to_latest()
        except Exception as e:
            logger.error(f"MySQL连接或初始化失败: {str(e)}")
            raise

    async def terminate(self) -> None:
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()

    async def check_image_exists(self, sha256_hash: str) -> bool:
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    "SELECT 1 FROM image_assets WHERE image_hash = %s",
                    (sha256_hash,)
                )
                result = await cursor.fetchone()
                return bool(result)

    async def get_image_info(self, sha256_hash: str) -> Optional[dict]:
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(
                    "SELECT file_ext, cf_url, cf_uploaded FROM image_assets WHERE image_hash = %s",
                    (sha256_hash,)
                )
                result = await cursor.fetchone()
                return result

    async def save_image_record(self, image_hash: str, file_ext: str, file_size: int, cf_url: Optional[str], cf_uploaded: bool) -> None:
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                cf_upload_time = datetime.datetime.now() if cf_uploaded else None
                await cursor.execute("""
                    INSERT INTO image_assets (image_hash, file_ext, file_size, cf_url, cf_uploaded, cf_upload_time, created_time)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    image_hash,
                    file_ext,
                    file_size,
                    cf_url,
                    1 if cf_uploaded else 0,
                    cf_upload_time,
                    datetime.datetime.now()
                ))

    async def update_image_cf_status(self, image_hash: str, cf_url: Optional[str], cf_uploaded: bool) -> None:
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                cf_upload_time = datetime.datetime.now() if cf_uploaded else None
                await cursor.execute("""
                    UPDATE image_assets
                    SET cf_url = %s, cf_uploaded = %s, cf_upload_time = %s
                    WHERE image_hash = %s
                """, (
                    cf_url,
                    1 if cf_uploaded else 0,
                    cf_upload_time,
                    image_hash
                ))

    async def save_message(self, message_id: str, platform_type: str, self_id: str, session_id: str, group_id: Optional[str], sender_data: dict, message_str: str, raw_message: dict, image_hashes: list, timestamp: int) -> None:
        dt_object = datetime.datetime.fromtimestamp(timestamp)
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("""
                    INSERT INTO messages (message_id, platform_type, self_id, session_id, group_id,
                                          sender, message_str, raw_message, image_ids, timestamp,
                                          created_time)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    message_id,
                    platform_type,
                    self_id,
                    session_id,
                    group_id,
                    json.dumps(sender_data),
                    message_str,
                    json.dumps(raw_message),
                    json.dumps(image_hashes),
                    timestamp,
                    dt_object
                ))

    async def get_sessions(self) -> list[dict]:
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute("""
                    SELECT session_id, group_id, sender, message_str, timestamp 
                    FROM messages 
                    WHERE id IN (SELECT MAX(id) FROM messages GROUP BY session_id)
                    ORDER BY timestamp DESC
                """)
                return await cursor.fetchall()

    async def get_messages(self, session_id: str, limit: int, offset: int) -> list[dict]:
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute("""
                    SELECT * FROM messages 
                    WHERE session_id = %s 
                    ORDER BY timestamp ASC 
                    LIMIT %s OFFSET %s
                """, (session_id, limit, offset))
                return await cursor.fetchall()

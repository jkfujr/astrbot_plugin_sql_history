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
                    "SELECT file_path FROM image_assets WHERE image_hash = %s",
                    (sha256_hash,)
                )
                result = await cursor.fetchone()
                return bool(result)

    async def save_image_record(self, image_hash: str, file_ext: str, file_path: str, file_size: int) -> None:
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("""
                    INSERT INTO image_assets (image_hash, file_ext, file_path, file_size, created_time)
                    VALUES (%s, %s, %s, %s, %s)
                """, (
                    image_hash,
                    file_ext,
                    file_path,
                    file_size,
                    datetime.datetime.now()
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

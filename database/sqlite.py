import aiosqlite
import json
import datetime
import os
from astrbot import logger
from typing import Optional
from .base import BaseStorage
from .sqlite_migrations import SQLiteMigrationManager

class SQLiteStorage(BaseStorage):
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn: Optional[aiosqlite.Connection] = None

    async def initialize(self) -> None:
        try:
            # 确保目录存在
            os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
            self.conn = await aiosqlite.connect(self.db_path)
            
            # 使用 SQLite 迁移管理器进行表结构初始化和升级
            migration_mgr = SQLiteMigrationManager(self.conn)
            await migration_mgr.upgrade_to_latest()
        except Exception as e:
            logger.error(f"SQLite连接或初始化失败: {str(e)}")
            raise
    async def terminate(self) -> None:
        if self.conn:
            await self.conn.close()

    async def check_image_exists(self, sha256_hash: str) -> bool:
        async with self.conn.cursor() as cursor:
            await cursor.execute(
                "SELECT file_path FROM image_assets WHERE image_hash = ?",
                (sha256_hash,)
            )
            result = await cursor.fetchone()
            return bool(result)

    async def save_image_record(self, image_hash: str, file_ext: str, file_size: int) -> None:
        async with self.conn.cursor() as cursor:
            await cursor.execute("""
                INSERT INTO image_assets (image_hash, file_ext, file_size, created_time)
                VALUES (?, ?, ?, ?)
            """, (
                image_hash,
                file_ext,
                file_size,
                datetime.datetime.now()
            ))
            await self.conn.commit()

    async def save_message(self, message_id: str, platform_type: str, self_id: str, session_id: str, group_id: Optional[str], sender_data: dict, message_str: str, raw_message: dict, image_hashes: list, timestamp: int) -> None:
        dt_object = datetime.datetime.fromtimestamp(timestamp)
        async with self.conn.cursor() as cursor:
            await cursor.execute("""
                INSERT INTO messages (message_id, platform_type, self_id, session_id, group_id,
                                      sender, message_str, raw_message, image_ids, timestamp,
                                      created_time)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            await self.conn.commit()

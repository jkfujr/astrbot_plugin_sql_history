import aiosqlite
import json
import datetime
import os
from astrbot import logger
from typing import Optional
from .base import BaseStorage

class SQLiteStorage(BaseStorage):
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn: Optional[aiosqlite.Connection] = None

    async def initialize(self) -> None:
        try:
            # 确保目录存在
            os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
            self.conn = await aiosqlite.connect(self.db_path)
            # SQLite 初始化建表
            await self._init_tables()
        except Exception as e:
            logger.error(f"SQLite连接或初始化失败: {str(e)}")
            raise

    async def _init_tables(self):
        async with self.conn.cursor() as cursor:
            # 创建版本追踪表（虽然 SQLite 这里没做严格的管理，但保留兼容语义）
            await cursor.execute("""
                CREATE TABLE IF NOT EXISTS plugin_schema_version (
                    version    INTEGER PRIMARY KEY,
                    applied_at DATETIME NOT NULL
                )
            """)
            
            # 创建图片资源表
            await cursor.execute("""
                CREATE TABLE IF NOT EXISTS image_assets (
                    image_hash   VARCHAR(64) PRIMARY KEY,
                    file_ext     VARCHAR(10),
                    file_path    TEXT NOT NULL,
                    file_size    INTEGER,
                    created_time DATETIME NOT NULL
                )
            """)

            # 创建消息表
            await cursor.execute("""
                CREATE TABLE IF NOT EXISTS messages (
                    message_id    VARCHAR(255) PRIMARY KEY,
                    platform_type VARCHAR(50)  NOT NULL,
                    self_id       VARCHAR(255) NOT NULL,
                    session_id    VARCHAR(255) NOT NULL,
                    group_id      VARCHAR(255),
                    sender        JSON         NOT NULL,
                    message_str   TEXT         NOT NULL,
                    raw_message   TEXT,
                    image_ids     JSON,
                    timestamp     INTEGER      NOT NULL,
                    created_time  DATETIME     NOT NULL
                )
            """)
            await self.conn.commit()

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

    async def save_image_record(self, image_hash: str, file_ext: str, file_path: str, file_size: int) -> None:
        async with self.conn.cursor() as cursor:
            await cursor.execute("""
                INSERT INTO image_assets (image_hash, file_ext, file_path, file_size, created_time)
                VALUES (?, ?, ?, ?, ?)
            """, (
                image_hash,
                file_ext,
                file_path,
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

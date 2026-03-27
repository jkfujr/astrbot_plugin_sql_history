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
                "SELECT 1 FROM image_assets WHERE image_hash = ?",
                (sha256_hash,)
            )
            result = await cursor.fetchone()
            return bool(result)

    async def get_image_info(self, sha256_hash: str) -> Optional[dict]:
        async with self.conn.execute(
            "SELECT image_hash, file_ext, file_size, cf_url, cf_uploaded FROM image_assets WHERE image_hash = ?",
            (sha256_hash,)
        ) as cursor:
            row = await cursor.fetchone()
            if row:
                return {
                    'image_hash': row[0],
                    'file_ext': row[1],
                    'file_size': row[2],
                    'cf_url': row[3],
                    'cf_uploaded': bool(row[4])
                }
            return None

    async def save_image_record(self, image_hash: str, file_ext: str, file_size: int, cf_url: Optional[str], cf_uploaded: bool) -> None:
        async with self.conn.cursor() as cursor:
            await cursor.execute("""
                INSERT OR REPLACE INTO image_assets (image_hash, file_ext, file_size, cf_url, cf_uploaded, created_time)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                image_hash,
                file_ext,
                file_size,
                cf_url,
                1 if cf_uploaded else 0,
                datetime.datetime.now()
            ))
            await self.conn.commit()

    async def update_image_cf_status(self, image_hash: str, cf_url: Optional[str], cf_uploaded: bool) -> None:
        async with self.conn.cursor() as cursor:
            cf_upload_time = datetime.datetime.now() if cf_uploaded else None
            await cursor.execute("""
                UPDATE image_assets
                SET cf_url = ?, cf_uploaded = ?, cf_upload_time = ?
                WHERE image_hash = ?
            """, (
                cf_url,
                1 if cf_uploaded else 0,
                cf_upload_time,
                image_hash
            ))
            await self.conn.commit()

    async def save_message(self, message_id: str, platform_type: str, self_id: str, 
                           session_id: str, group_id: Optional[str], sender_data: dict, 
                           message_str: str, raw_message: dict, image_hashes: list, 
                           timestamp: int, forward_data: Optional[list] = None) -> None:
        dt_object = datetime.datetime.fromtimestamp(timestamp)
        async with self.conn.cursor() as cursor:
            await cursor.execute("""
                INSERT INTO messages (message_id, platform_type, self_id, session_id, group_id,
                                      sender, message_str, raw_message, image_ids, forward_data, timestamp,
                                      created_time)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                json.dumps(forward_data) if forward_data else None,
                timestamp,
                dt_object
            ))
            await self.conn.commit()

    async def get_sessions(self) -> list[dict]:
        async with self.conn.execute("""
            SELECT session_id, group_id, sender, message_str, timestamp 
            FROM messages 
            WHERE rowid IN (SELECT MAX(rowid) FROM messages GROUP BY session_id)
            ORDER BY timestamp DESC
        """) as cursor:
            rows = await cursor.fetchall()
            sessions = []
            for row in rows:
                sessions.append({
                    'session_id': row[0],
                    'group_id': row[1],
                    'sender': json.loads(row[2]),
                    'message_str': row[3],
                    'timestamp': row[4]
                })
            return sessions

    async def get_messages(self, session_id: str, page: int = 1, page_size: int = 20) -> list[dict]:
        offset = (page - 1) * page_size
        async with self.conn.execute("""
            SELECT * FROM (
                SELECT message_id, platform_type, self_id, session_id, group_id, 
                       sender, message_str, raw_message, image_ids, forward_data, timestamp 
                FROM messages 
                WHERE session_id = ? 
                ORDER BY timestamp DESC 
                LIMIT ? OFFSET ?
            ) ORDER BY timestamp ASC
        """, (session_id, page_size, offset)) as cursor:
            rows = await cursor.fetchall()
            messages = []
            for row in rows:
                messages.append({
                    'message_id': row[0],
                    'platform_type': row[1],
                    'self_id': row[2],
                    'session_id': row[3],
                    'group_id': row[4],
                    'sender': json.loads(row[5]),
                    'message_str': row[6],
                    'raw_message': json.loads(row[7]),
                    'image_ids': json.loads(row[8]),
                    'forward_data': json.loads(row[9]) if row[9] else None,
                    'timestamp': row[10]
                })
            return messages

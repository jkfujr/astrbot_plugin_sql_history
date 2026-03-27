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
                    "SELECT image_hash, file_ext, file_size, cf_url, cf_uploaded FROM image_assets WHERE image_hash = %s",
                    (sha256_hash,)
                )
                row = await cursor.fetchone()
                if row:
                    row['cf_uploaded'] = bool(row['cf_uploaded'])
                    return row
                return None

    async def save_image_record(self, image_hash: str, file_ext: str, file_size: int, cf_url: Optional[str], cf_uploaded: bool) -> None:
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("""
                    INSERT INTO image_assets (image_hash, file_ext, file_size, cf_url, cf_uploaded, created_time)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE 
                        file_ext = VALUES(file_ext),
                        file_size = VALUES(file_size),
                        cf_url = VALUES(cf_url),
                        cf_uploaded = VALUES(cf_uploaded)
                """, (
                    image_hash,
                    file_ext,
                    file_size,
                    cf_url,
                    1 if cf_uploaded else 0,
                    datetime.datetime.now()
                ))
                await conn.commit()

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
                await conn.commit()

    async def save_message(self, message_id: str, platform_type: str, self_id: str, 
                           session_id: str, group_id: Optional[str], sender_data: dict, 
                           message_str: str, raw_message: dict, image_hashes: list, 
                           timestamp: int, forward_data: Optional[list] = None) -> None:
        dt_object = datetime.datetime.fromtimestamp(timestamp)
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("""
                    INSERT INTO messages (message_id, platform_type, self_id, session_id, group_id,
                                          sender, message_str, raw_message, image_ids, forward_data, timestamp,
                                          created_time)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                await conn.commit()

    async def get_sessions(self) -> list[dict]:
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute("""
                    SELECT m1.session_id, m1.group_id, m1.sender, m1.message_str, m1.timestamp 
                    FROM messages m1
                    JOIN (
                        SELECT session_id, MAX(timestamp) as max_ts
                        FROM messages
                        GROUP BY session_id
                    ) m2 ON m1.session_id = m2.session_id AND m1.timestamp = m2.max_ts
                    GROUP BY m1.session_id
                    ORDER BY m1.timestamp DESC
                """)
                rows = await cursor.fetchall()
                sessions = []
                for row in rows:
                    sessions.append({
                        'session_id': row['session_id'],
                        'group_id': row['group_id'],
                        'sender': json.loads(row['sender']),
                        'message_str': row['message_str'],
                        'timestamp': row['timestamp']
                    })
                return sessions

    async def get_messages(self, session_id: str, page: int = 1, page_size: int = 20) -> list[dict]:
        offset = (page - 1) * page_size
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                # 使用嵌套查询来实现倒序 LIMIT 但正序展示
                await cursor.execute("""
                    SELECT * FROM (
                        SELECT message_id, platform_type, self_id, session_id, group_id, 
                               sender, message_str, raw_message, image_ids, forward_data, timestamp 
                        FROM messages 
                        WHERE session_id = %s 
                        ORDER BY timestamp DESC 
                        LIMIT %s OFFSET %s
                    ) AS t ORDER BY timestamp ASC
                """, (session_id, page_size, offset))
                rows = await cursor.fetchall()
                messages = []
                for row in rows:
                    messages.append({
                        'message_id': row['message_id'],
                        'platform_type': row['platform_type'],
                        'self_id': row['self_id'],
                        'session_id': row['session_id'],
                        'group_id': row['group_id'],
                        'sender': json.loads(row['sender']),
                        'message_str': row['message_str'],
                        'raw_message': json.loads(row['raw_message']),
                        'image_ids': json.loads(row['image_ids']),
                        'forward_data': json.loads(row['forward_data']) if row['forward_data'] else None,
                        'timestamp': row['timestamp']
                    })
                return messages

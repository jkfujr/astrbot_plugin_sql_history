from astrbot import logger
from astrbot.api.event import filter, AstrMessageEvent
from astrbot.api.star import Context, Star, register
from astrbot.api import AstrBotConfig
from astrbot.api.message_components import Image
import aiomysql
import json
import os
import aiohttp
import aiofiles
import hashlib
import datetime
from pathlib import Path
from typing import Optional
from .migrations import MigrationManager


@register("astrbot_plugin_sql_history", "LW", "MySQL日志(Hash去重版)", "1.1.0")
class MySQLPlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config = config
        self.pool: Optional[aiomysql.Pool] = None

        # 图片保存路径
        self.is_save_image = self.config.get("is_save_image", False)
        self.image_save_path = self.config.get("image_save_path", "./data/chat_images") or "./data/chat_images"
        if self.is_save_image and not os.path.exists(self.image_save_path):
            os.makedirs(self.image_save_path)

    async def initialize(self):
        logger.info("正在初始化 astrbot_plugin_sql_history 插件...")

        host = self.config.get("host")
        database = self.config.get("database")
        username = self.config.get("username")
        password = self.config.get("password")

        if not all([host, database, username, password]):
            logger.error("MySQL 连接配置不完整，请在插件管理面板填写 host, database, username 和 password。")
            return

        try:
            self.pool = await aiomysql.create_pool(
                host=host,
                port=self.config.get("port", 3306),
                user=username,
                password=password,
                db=database,
                autocommit=True,
                minsize=1,
                maxsize=5
            )

            # 使用迁移管理器进行数据库初始化和升级
            migration_mgr = MigrationManager(self.pool)
            await migration_mgr.upgrade_to_latest()

        except Exception as e:
            logger.error(f"插件初始化失败: {str(e)}")
            raise

    async def _process_image(self, url: str) -> Optional[str]:
        """
        处理图片：下载 -> Hash -> 查重 -> 保存/返回Hash
        返回: image_hash (如果失败返回 None)
        """
        if not url:
            return None

        try:
            # 1. 下载图片到内存
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as resp:
                    if resp.status != 200:
                        return None
                    img_data = await resp.read()  # 读取二进制数据

            # 2. 计算 SHA256 Hash
            sha256_hash = hashlib.sha256(img_data).hexdigest()

            # 3. 检查数据库是否存在该 Hash
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(
                        "SELECT file_path FROM image_assets WHERE image_hash = %s",
                        (sha256_hash,)
                    )
                    result = await cursor.fetchone()

                    if result:
                        # 3.1 存在：直接返回 Hash，不再下载
                        logger.debug(f"图片已存在，Hash: {sha256_hash}")
                        return sha256_hash
                    else:
                        # 3.2 不存在：保存文件并入库

                        # 确定后缀
                        # 简单判断，实际可根据 magic number 判断
                        file_ext = ".jpg"
                        if img_data[:4].startswith(b'\x89PNG'):
                            file_ext = ".png"
                        elif img_data[:3].startswith(b'GIF'):
                            file_ext = ".gif"
                        elif img_data[:4].startswith(b'RIFF') and img_data[8:12] == b'WEBP':
                            file_ext = ".webp"

                        # 使用 Hash 作为文件名
                        file_name = f"{sha256_hash}{file_ext}"
                        save_path = Path(self.image_save_path) / file_name
                        abs_path = str(save_path.absolute())

                        # 写入磁盘
                        async with aiofiles.open(save_path, mode='wb') as f:
                            await f.write(img_data)

                        # 写入 image_assets 表 (同时存储 file_path 以保持兼容，后续版本可移除)
                        await cursor.execute("""
                                             INSERT INTO image_assets (image_hash, file_ext, file_path, file_size, created_time)
                                             VALUES (%s, %s, %s, %s, %s)
                                             """, (
                                                 sha256_hash,
                                                 file_ext,
                                                 abs_path,
                                                 len(img_data),
                                                 datetime.datetime.now()
                                             ))

                        logger.info(f"新图片已归档: {sha256_hash}")
                        return sha256_hash

        except Exception as e:
            logger.error(f"处理图片出错 {url}: {e}")
            return None

    @filter.event_message_type(filter.EventMessageType.ALL)
    async def on_all_message(self, event: AstrMessageEvent):
        try:
            msg = event.message_obj
            meta = event.platform_meta

            # 收集所有图片的 Hash
            image_hashes = []

            # 遍历消息组件寻找图片
            if self.is_save_image:
                for component in msg.message:
                    if isinstance(component, Image):
                        if component.url:
                            # 调用处理函数，获取 Hash
                            img_hash = await self._process_image(component.url)
                            if img_hash:
                                image_hashes.append(img_hash)

            # 准备插入数据
            sender_data = {
                'user_id': msg.sender.user_id,
                'nickname': msg.sender.nickname,
                'platform_id': meta.id
            }

            dt_object = datetime.datetime.fromtimestamp(msg.timestamp)

            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute("""
                                         INSERT INTO messages (message_id, platform_type, self_id, session_id, group_id,
                                                               sender, message_str, raw_message, image_ids, timestamp,
                                                               created_time)
                                         VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                         """, (
                                             msg.message_id,
                                             meta.name,
                                             event.get_self_id(),
                                             event.session_id,
                                             msg.group_id or None,
                                             json.dumps(sender_data),
                                             event.message_str,
                                             json.dumps(msg.raw_message),
                                             json.dumps(image_hashes),
                                             msg.timestamp,
                                             dt_object
                                         ))

        except Exception as e:
            logger.error(f"日志记录异常: {e}")

    async def terminate(self):
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()

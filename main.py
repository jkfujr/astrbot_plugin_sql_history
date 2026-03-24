from astrbot import logger
from astrbot.api.event import filter, AstrMessageEvent
from astrbot.api.star import Context, Star, register
from astrbot.api import AstrBotConfig
from astrbot.api.message_components import Image
import json
import os
import aiohttp
import aiofiles
import hashlib
import datetime
from pathlib import Path
from typing import Optional
from .database import MySQLStorage, SQLiteStorage, BaseStorage


@register("astrbot_plugin_sql_history", "LW", "MySQL日志(Hash去重版)", "1.1.0")
class MySQLPlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config = config
        self.storage: Optional[BaseStorage] = None

        # 读取分类配置
        db_conf = self.config.get("database", {})
        img_conf = self.config.get("image", {})

        # 图片保存路径
        self.is_save_image = img_conf.get("is_save_image", False)
        self.image_save_path = img_conf.get("image_save_path", "data/plugins/astrbot_plugin_sql_history/image") or "data/plugins/astrbot_plugin_sql_history/image"
        if self.is_save_image and not os.path.exists(self.image_save_path):
            os.makedirs(self.image_save_path)

    async def initialize(self):
        logger.info("正在初始化 astrbot_plugin_sql_history 插件...")
        
        db_conf = self.config.get("database", {})
        db_type = db_conf.get("db_type", "sqlite")

        try:
            if db_type == "mysql":
                host = db_conf.get("host")
                database = db_conf.get("database")
                username = db_conf.get("username")
                password = db_conf.get("password")
                
                if not all([host, database, username, password]):
                    logger.info("MySQL 尚未配置。若需启用数据库日志，请在插件面板填写连接信息。")
                    return
                self.storage = MySQLStorage(
                    host=host,
                    port=db_conf.get("port", 3306),
                    database=database,
                    username=username,
                    password=password
                )
            elif db_type == "sqlite":
                db_path = db_conf.get("sqlite_db_path", "data/plugins/astrbot_plugin_sql_history/history.db")
                self.storage = SQLiteStorage(db_path=db_path)
            else:
                logger.error(f"不支持的数据库类型: {db_type}")
                return
            
            await self.storage.initialize()
            logger.info(f"成功连接至数据库: {db_type}")

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
            is_exists = await self.storage.check_image_exists(sha256_hash)

            if is_exists:
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
                await self.storage.save_image_record(
                    image_hash=sha256_hash,
                    file_ext=file_ext,
                    file_path=abs_path,
                    file_size=len(img_data)
                )

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

            if self.storage:
                await self.storage.save_message(
                    message_id=msg.message_id,
                    platform_type=meta.name,
                    self_id=event.get_self_id(),
                    session_id=event.session_id,
                    group_id=msg.group_id or None,
                    sender_data=sender_data,
                    message_str=event.message_str,
                    raw_message=msg.raw_message,
                    image_hashes=image_hashes,
                    timestamp=msg.timestamp
                )

        except Exception as e:
            logger.error(f"日志记录异常: {e}")

    async def terminate(self):
        if self.storage:
            await self.storage.terminate()

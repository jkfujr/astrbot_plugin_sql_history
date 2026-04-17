import json
import os
import aiohttp
import aiofiles
import hashlib
import datetime
import traceback
from pathlib import Path
from typing import Optional

from astrbot import logger
from astrbot.api.event import filter, AstrMessageEvent
from astrbot.api.star import Context, Star, register
from astrbot.api import AstrBotConfig
from astrbot.api.message_components import Image, Forward
from astrbot.core.platform.sources.aiocqhttp.aiocqhttp_platform_adapter import AiocqhttpAdapter


from .database import MySQLStorage, SQLiteStorage, BaseStorage
from .webui import WebUIServer


@register("astrbot_plugin_sql_history", "LW&jkfujr", "MySQL日志(Hash去重版)", "1.2.1")
class MySQLPlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config = config
        self.storage: Optional[BaseStorage] = None
        self.webui_server: Optional[WebUIServer] = None

        # 读取分类配置
        db_conf = self.config.get("database", {})
        img_conf = self.config.get("image", {})
        remote_conf = img_conf.get("remote", {}) or {}
        cf_conf = remote_conf.get("cloudflare", {}) or {}
        imgbed_conf = remote_conf.get("imgbed", {}) or {}
        adv_conf = self.config.get("advanced", {})

        # 图片保存路径
        self.is_save_image = img_conf.get("is_save_image", False)
        self.image_save_path = img_conf.get("image_save_path", "data/plugin_data/astrbot_plugin_sql_history/image") or "data/plugin_data/astrbot_plugin_sql_history/image"

        # 读取远端图床配置
        self.remote_provider = remote_conf.get("provider", "none")
        self.remote_api_endpoint = self._normalize_api_endpoint(remote_conf.get("api_endpoint", ""))
        self.remote_upload_directory = (remote_conf.get("upload_directory", "QQ") or "").strip()
        self.auto_reupload_old = remote_conf.get("auto_reupload_old", True)

        # CloudFlare-ImgBed 配置
        self.cf_api_endpoint = self.remote_api_endpoint if self.remote_provider == "cloudflare" else ""
        self.cf_api_token = str(cf_conf.get("api_token", "") or "").strip()
        self.cf_channel_mode = cf_conf.get("channel_mode", "manual")  # manual / auto
        self.cf_upload_channel = cf_conf.get("upload_channel", "telegram")
        self.cf_server_compress = cf_conf.get("server_compress", True)
        self.cf_return_full_url = cf_conf.get("return_full_url", True)
        self.cf_upload_folder = self._normalize_cloudflare_upload_directory(self.remote_upload_directory)

        # ImgBed 配置
        self.imgbed_api_endpoint = self.remote_api_endpoint if self.remote_provider == "imgbed" else ""
        self.imgbed_api_token = str(imgbed_conf.get("api_token", "") or "").strip()
        self.imgbed_upload_directory = self._normalize_imgbed_upload_directory(self.remote_upload_directory)

        self.debug_log = adv_conf.get("debug_log", False)

        # 自动渠道轮询相关
        self._available_channels: list[dict] = []  # 存储为 {'name': str, 'type': str}
        self._round_robin_index = 0
        self._channels_fetched = False  # 标记是否已经获取过渠道列表，避免重复获取重置索引
        self._verified_remote_hashes: set[str] = set()  # 当前进程内已确认远端存在的图片

        if self.is_save_image and not os.path.exists(self.image_save_path):
            os.makedirs(self.image_save_path, exist_ok=True)

    def _normalize_api_endpoint(self, api_endpoint: str) -> str:
        """规范化远端服务地址。"""
        if not api_endpoint:
            return ""
        return api_endpoint.rstrip("/")

    def _normalize_cloudflare_upload_directory(self, upload_directory: str) -> str:
        """CloudFlare-ImgBed 使用相对目录。"""
        upload_directory = (upload_directory or "").strip()
        return upload_directory.lstrip("/")

    def _normalize_imgbed_upload_directory(self, upload_directory: str) -> str:
        """ImgBed 使用以 / 开头的目录。"""
        upload_directory = (upload_directory or "").strip()
        if not upload_directory:
            return ""
        if upload_directory.startswith("/"):
            return upload_directory
        return f"/{upload_directory}"

    def _get_upload_content_type(self, file_ext: str) -> str:
        """根据文件后缀生成上传时使用的 MIME 类型。"""
        ext = (file_ext or "").lower()
        if ext == ".png":
            return "image/png"
        if ext == ".gif":
            return "image/gif"
        if ext == ".webp":
            return "image/webp"
        return "image/jpeg"

    def _build_remote_file_url(self, remote_url: str) -> str:
        """将图床返回的相对地址补全为可访问的完整 URL。"""
        remote_url = str(remote_url or "").strip()
        if not remote_url:
            return ""
        if remote_url.startswith("http://") or remote_url.startswith("https://"):
            return remote_url

        base_url = self.remote_api_endpoint
        if self.remote_provider == "cloudflare":
            base_url = self.cf_api_endpoint or base_url
        elif self.remote_provider == "imgbed":
            base_url = self.imgbed_api_endpoint or base_url

        if not base_url:
            return remote_url

        if remote_url.startswith("/"):
            return f"{base_url}{remote_url}"
        return f"{base_url}/{remote_url}"

    def _is_likely_image_bytes(self, data: bytes) -> bool:
        """基于文件头判断是否像图片。"""
        if not data:
            return False
        if data.startswith(b"\x89PNG"):
            return True
        if data.startswith(b"\xff\xd8\xff"):
            return True
        if data.startswith(b"GIF"):
            return True
        if len(data) >= 12 and data[:4] == b"RIFF" and data[8:12] == b"WEBP":
            return True
        return False

    async def _check_remote_file_exists(self, remote_url: str) -> Optional[bool]:
        """
        检查远端图床文件是否存在。
        返回:
            True: 文件存在
            False: 文件不存在
            None: 无法确认
        """
        check_url = self._build_remote_file_url(remote_url)
        if not check_url:
            return False

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    check_url,
                    headers={"Range": "bytes=0-15"},
                    allow_redirects=True,
                ) as resp:
                    if resp.status in (200, 206):
                        content_type = str(resp.headers.get("Content-Type", "") or "").split(";")[0].strip().lower()
                        preview = await resp.read()
                        if content_type.startswith("image/") or content_type == "application/octet-stream":
                            return True
                        if self._is_likely_image_bytes(preview):
                            return True
                        if content_type in {"application/json", "text/html", "text/plain"}:
                            logger.warning(f"远端文件检查命中非图片响应，URL: {check_url}，Content-Type: {content_type}")
                            return False
                        logger.warning(
                            f"远端文件检查返回 200/206，但无法确认为图片，URL: {check_url}，"
                            f"Content-Type: {content_type or 'unknown'}，响应前缀: {preview[:32]!r}"
                        )
                        return False
                    if resp.status == 404:
                        return False
                    logger.warning(f"远端文件存在性检查返回异常状态码: {resp.status}，URL: {check_url}")
                    return None
        except Exception as e:
            logger.warning(f"远端文件存在性检查失败，URL: {check_url}，异常: {str(e)}")
            return None

    async def initialize(self):
        logger.info("正在初始化 astrbot_plugin_sql_history 插件...")
        logger.info(f"插件入口文件: {__file__}")

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
                db_path = db_conf.get("sqlite_db_path", "data/plugin_data/astrbot_plugin_sql_history/sqlite/data_v1.db")
                self.storage = SQLiteStorage(db_path=db_path)
            else:
                logger.error(f"不支持的数据库类型: {db_type}")
                return

            await self.storage.initialize()
            logger.info(f"成功连接至数据库: {db_type}")

            # 判断是否需要使用远端图床
            need_remote = self.remote_provider != "none"

            if need_remote:
                if self.remote_provider == "cloudflare":
                    if not self.cf_api_endpoint:
                        logger.warning("CloudFlare-ImgBed 已启用，但API端点未配置")
                    # 如果是自动渠道模式，初始化时获取可用渠道列表
                    if self.cf_channel_mode == "auto" and (not self._channels_fetched or len(self._available_channels) == 0):
                        await self._fetch_available_channels()
                elif self.remote_provider == "imgbed":
                    if not self.imgbed_api_endpoint:
                        logger.warning("ImgBed 已启用，但API端点未配置")
                    if not self.imgbed_api_token:
                        logger.warning("ImgBed 已启用，但API Token未配置")

            await self._start_webui()

        except Exception as e:
            logger.error(f"插件初始化失败: {str(e)}")
            logger.error(traceback.format_exc())
            raise

    async def _upload_to_imgbed(self, img_data: bytes, sha256_hash: str, file_ext: str) -> Optional[str]:
        """
        上传图片到 ImgBed
        返回: 上传成功返回图片URL，失败返回 None
        """
        if not self.imgbed_api_endpoint:
            logger.error("ImgBed API 端点未配置，跳过上传")
            return None

        if not self.imgbed_api_token:
            logger.error("ImgBed API Token 未配置，跳过上传")
            return None

        upload_url = f"{self.imgbed_api_endpoint}/api/upload"
        file_name = f"{sha256_hash}{file_ext}"

        try:
            async with aiohttp.ClientSession() as session:
                headers = {
                    "Authorization": f"Bearer {self.imgbed_api_token}"
                }

                # 构建 form data
                form = aiohttp.FormData()
                form.add_field(
                    'file',
                    img_data,
                    filename=file_name,
                    content_type=self._get_upload_content_type(file_ext)
                )

                # 添加目录参数
                if self.imgbed_upload_directory and self.imgbed_upload_directory.strip():
                    form.add_field('directory', self.imgbed_upload_directory.strip())

                async with session.post(upload_url, headers=headers, data=form) as resp:
                    if resp.status == 401:
                        logger.error("ImgBed API Token 无效或已失效")
                        return None
                    elif resp.status != 200:
                        error_text = await resp.text()
                        logger.error(f"ImgBed 上传失败，状态码: {resp.status}，响应: {error_text[:500]}")
                        return None
                    else:
                        result = await resp.json()

                    # 解析响应
                    if result.get("code") == 0 and result.get("data", {}).get("url"):
                        image_url = result["data"]["url"]
                        logger.info(f"图片成功上传至 ImgBed: {sha256_hash} -> {image_url}")
                        return image_url
                    else:
                        logger.error(f"ImgBed 上传失败: {result.get('message', '未知错误')}")
                        return None

        except Exception as e:
            logger.error(f"ImgBed 上传异常 {sha256_hash}: {str(e)}")
            return None

    async def _upload_to_cf_imgbed(self, img_data: bytes, sha256_hash: str, file_ext: str) -> Optional[str]:
        """
        上传图片到 CloudFlare-ImgBed
        返回: 上传成功返回图片URL，失败返回 None
        """
        if not self.cf_api_endpoint:
            logger.error("CloudFlare-ImgBed API 端点未配置，跳过上传")
            return None

        # 构建上传URL
        upload_url = f"{self.cf_api_endpoint}/upload"

        # 构建查询参数
        params = {}
        params['serverCompress'] = str(self.cf_server_compress).lower()
        # 根据渠道模式选择
        if self.cf_channel_mode == "auto" and self._available_channels:
            channel_obj = self._get_next_channel()
            upload_channel = channel_obj.get('type')
            channel_name = channel_obj.get('name')
            
            if upload_channel:
                params['uploadChannel'] = upload_channel
            if channel_name:
                params['channelName'] = channel_name
                
            # auto模式下同时传递 uploadChannel 和 channelName
            logger.info(f"CloudFlare-ImgBed 轮询选择渠道: [{channel_name}] (类型: {upload_channel}, 索引: {self._round_robin_index}/{len(self._available_channels)})")
        else:
            params['uploadChannel'] = self.cf_upload_channel
        # 添加上传目录
        if self.cf_upload_folder and self.cf_upload_folder.strip():
            params['uploadFolder'] = self.cf_upload_folder.strip()
        params['autoRetry'] = 'true'
        params['returnFormat'] = 'full' if self.cf_return_full_url else 'default'

        if self.debug_log:
            logger.debug(f"CF上传最终请求参数: {params}")

        # 文件名
        file_name = f"{sha256_hash}{file_ext}"

        try:
            # 使用 aiohttp 进行 multipart/form-data 上传
            async with aiohttp.ClientSession() as session:
                # 添加认证头
                headers = {}
                if self.cf_api_token:
                    headers['Authorization'] = f"Bearer {self.cf_api_token}"

                # 构建 form data
                form = aiohttp.FormData()
                form.add_field(
                    'file',
                    img_data,
                    filename=file_name,
                    content_type=self._get_upload_content_type(file_ext)
                )

                async with session.post(upload_url, params=params, headers=headers, data=form) as resp:
                    if resp.status != 200:
                        error_text = await resp.text()
                        logger.error(f"CloudFlare-ImgBed 上传失败，状态码: {resp.status}，响应: {error_text[:500]}")
                        return None

                    # 解析响应: [{"src": "/file/abc123.jpg"}]
                    result = await resp.json()
                    if not result or not isinstance(result, list) or len(result) == 0:
                        logger.error("CloudFlare-ImgBed 返回空响应或响应格式错误")
                        return None

                    image_url = result[0].get('src')
                    if not image_url:
                        logger.error("CloudFlare-ImgBed 响应中未找到 src 字段")
                        return None

                    logger.info(f"图片成功上传至 CloudFlare-ImgBed: {sha256_hash} -> {image_url}")
                    return image_url

        except Exception as e:
            logger.error(f"CloudFlare-ImgBed 上传异常 {sha256_hash}: {str(e)}")
            return None

    async def _fetch_available_channels(self) -> None:
        """从 /api/channels 获取可用上传渠道列表"""
        if not self.cf_api_endpoint:
            return

        channels_url = f"{self.cf_api_endpoint}/api/channels"

        try:
            async with aiohttp.ClientSession() as session:
                headers = {}
                if self.cf_api_token:
                    headers['Authorization'] = f"Bearer {self.cf_api_token}"

                async with session.get(channels_url, headers=headers) as resp:
                    if resp.status != 200:
                        logger.error(f"获取渠道列表失败，状态码: {resp.status}")
                        return

                    result = await resp.json()
                    if not result:
                        logger.error(f"渠道列表返回为空，实际内容: {result}")
                        return

                    # 兼容多种可能的返回格式
                    channels_data = result
                    if isinstance(result, dict) and result.get('data'):
                        # 有些 API 会包装在 data 字段里
                        channels_data = result.get('data')
                        if self.debug_log:
                            logger.debug(f"从 result.data 提取渠道列表，数据类型: {type(channels_data)}")

                    available = []
                    if isinstance(channels_data, dict):
                        # CloudFlare-ImgBed 实际返回格式: 按类型分组的字典
                        # { 'telegram': [{'name': 'xxx', 'type': 'TelegramNew'}], 's3': [...], ... }
                        if self.debug_log:
                            logger.debug(f"检测到分组字典格式，共 {len(channels_data)} 个类型分组")
                        for channel_type, channel_list in channels_data.items():
                            if isinstance(channel_list, list):
                                for channel in channel_list:
                                    if isinstance(channel, dict):
                                        if channel.get('enabled', True):
                                            name = channel.get('name')
                                            if name:
                                                available.append({
                                                    'name': name if isinstance(name, str) else str(name),
                                                    'type': channel_type
                                                })
                                                if self.debug_log:
                                                    logger.debug(f"添加渠道: {name} (类型: {channel_type})")
                    elif isinstance(channels_data, list):
                        # 直接是列表格式
                        if self.debug_log:
                            logger.debug(f"检测到直接列表格式，共 {len(channels_data)} 个渠道")
                        for idx, channel in enumerate(channels_data):
                            if isinstance(channel, str):
                                # 直接是字符串列表: ["telegram", "cfr2", ...]
                                # 这种格式没有类型信息，默认使用 self.cf_upload_channel 的类型或者尝试猜测
                                available.append({
                                    'name': channel,
                                    'type': self.cf_upload_channel # 回退到手动配置的类型
                                })
                            elif isinstance(channel, dict):
                                # 对象格式: {"name": "telegram", "enabled": true}
                                if channel.get('enabled', True):
                                    name = channel.get('name') or channel.get('channelName') or channel.get('type')
                                    # type 字段可能是类型也可能是名称，尽量取准确
                                    c_type = channel.get('type') or self.cf_upload_channel
                                    if name:
                                        available.append({
                                            'name': name if isinstance(name, str) else str(name),
                                            'type': c_type
                                        })
                            else:
                                if self.debug_log:
                                    logger.debug(f"跳过第 {idx} 个渠道，类型不支持: {type(channel)}")
                    else:
                        logger.error(f"渠道列表返回格式错误，期望 dict 或 list，实际得到 {type(channels_data)}，内容: {str(channels_data)[:500]}")
                        return

                    if self.debug_log:
                        logger.debug(f"解析完成，共得到 {len(available)} 个可用渠道: {available}")

                    if available:
                        if not self._channels_fetched:
                            self._available_channels = available
                            self._round_robin_index = 0
                            self._channels_fetched = True
                            logger.info(f"成功获取 {len(available)} 个可用渠道: {', '.join([c['name'] for c in available])}")
                            if self.debug_log:
                                logger.debug(f"渠道列表已设置，轮询索引已重置为 0")
                        else:
                            # 已经获取过，只更新渠道列表不重置轮询索引
                            self._available_channels = available
                            logger.info(f"更新渠道列表，共 {len(available)} 个可用渠道: {', '.join([c['name'] for c in available])}")
                            if self.debug_log:
                                logger.debug(f"渠道列表已更新，轮询索引保持当前值: {self._round_robin_index}")
                    else:
                        logger.warning("未获取到任何可用渠道，将回退到手动配置")
                        if self.cf_upload_channel:
                            self._available_channels = [{'name': self.cf_upload_channel, 'type': self.cf_upload_channel}]

        except Exception as e:
            logger.error(f"获取渠道列表异常: {str(e)}")
            if self.cf_upload_channel:
                self._available_channels = [{'name': self.cf_upload_channel, 'type': self.cf_upload_channel}]

    def _get_next_channel(self) -> dict:
        """轮询获取下一个上传渠道"""
        if not self._available_channels:
            # 如果没有获取到，回退到手动配置
            return {'name': self.cf_upload_channel, 'type': self.cf_upload_channel}

        # Round-robin 轮询
        current_index = self._round_robin_index
        channel_obj = self._available_channels[current_index]
        self._round_robin_index = (self._round_robin_index + 1) % len(self._available_channels)
        
        name = channel_obj.get('name')
        c_type = channel_obj.get('type')
        
        logger.info(f"CF轮询渠道: 当前索引={current_index}, 选中=[{name}](类型: {c_type}), 下一个索引={self._round_robin_index}, 总渠道数={len(self._available_channels)}")
        if self.debug_log:
            logger.debug(f"所有可用渠道: {self._available_channels}")
        return channel_obj

    async def _process_image(self, url: str) -> Optional[str]:
        """
        处理图片：下载 -> Hash -> 查重 -> 根据配置存储到对应位置
        支持：仅本地 / 仅远端 / 本地+远端
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

            # 3. 判断存储需求
            need_local = self.is_save_image
            need_remote = self.remote_provider != "none"

            # 4. 查询数据库获取完整信息
            img_info = await self.storage.get_image_info(sha256_hash)

            # 情况1: 数据库中已存在这张图片（内容相同）
            if img_info:
                # 不需要远端图床时，直接返回
                if not need_remote:
                    logger.debug(f"图片已存在，Hash: {sha256_hash}")
                    return sha256_hash

                remote_uploaded = bool(img_info.get('remote_uploaded', False) and img_info.get('remote_url'))
                remote_missing_detected = False

                if remote_uploaded:
                    if sha256_hash in self._verified_remote_hashes:
                        logger.debug(f"图片已存在且远端已校验通过，Hash: {sha256_hash}")
                        return sha256_hash

                    remote_exists = await self._check_remote_file_exists(img_info.get('remote_url'))
                    if remote_exists is True:
                        self._verified_remote_hashes.add(sha256_hash)
                        logger.debug(f"图片已存在且远端文件可访问，Hash: {sha256_hash}")
                        return sha256_hash

                    if remote_exists is None:
                        logger.warning(f"无法确认远端文件是否存在，沿用数据库记录，Hash: {sha256_hash}")
                        return sha256_hash

                    remote_missing_detected = True
                    img_info = dict(img_info)
                    img_info['remote_uploaded'] = False
                    img_info['remote_url'] = None
                    self._verified_remote_hashes.discard(sha256_hash)
                    logger.warning(f"数据库记录显示图片已上传，但远端文件不存在，Hash: {sha256_hash}")

                # 需要补传: 已存在但未上传到图床，且开启了自动补传
                if need_remote and self.auto_reupload_old:
                    logger.info(f"检测到未上传旧图，自动补传至图床: {sha256_hash}")

                    # 如果本地有文件，优先从本地读取（避免重新下载）
                    if self.is_save_image:
                        found = False
                        for ext in ['.jpg', '.png', '.gif', '.webp']:
                            local_path = Path(self.image_save_path).joinpath(f"{sha256_hash}{ext}")
                            if local_path.exists():
                                async with aiofiles.open(local_path, mode='rb') as f:
                                    img_data = await f.read()
                                # 重新计算哈希验证内容一致性
                                check_hash = hashlib.sha256(img_data).hexdigest()
                                if check_hash != sha256_hash:
                                    logger.warning(f"本地文件哈希不匹配，跳过: {sha256_hash} vs {check_hash}")
                                else:
                                    found = True
                                break

                    # 获取文件扩展名（从数据库记录）
                    file_ext = img_info.get('file_ext', '.jpg')

                    # 根据配置上传到对应图床
                    remote_url = await self._upload_to_remote_by_config(img_data, sha256_hash, file_ext)

                    # 更新数据库记录（即使失败也要更新状态）
                    await self.storage.update_image_remote_status(
                        image_hash=sha256_hash,
                        remote_url=remote_url,
                        remote_uploaded=remote_url is not None,
                        remote_provider=self.remote_provider if remote_url else None,
                        remote_endpoint=self.remote_api_endpoint if remote_url else None
                    )
                    if remote_url is not None:
                        self._verified_remote_hashes.add(sha256_hash)
                    else:
                        self._verified_remote_hashes.discard(sha256_hash)

                elif remote_missing_detected:
                    await self.storage.update_image_remote_status(
                        image_hash=sha256_hash,
                        remote_url=None,
                        remote_uploaded=False,
                        remote_provider=None,
                        remote_endpoint=None
                    )

                return sha256_hash

            # 情况2: 新图片，数据库不存在，需要全新处理
            # 确定文件后缀
            # 通过文件头魔法数字判断，比猜测更准确
            file_ext = ".jpg"
            if len(img_data) >= 4 and img_data[:4].startswith(b'\x89PNG'):
                file_ext = ".png"
            elif len(img_data) >= 3 and img_data[:3].startswith(b'GIF'):
                file_ext = ".gif"
            elif len(img_data) >= 12 and img_data[:4].startswith(b'RIFF') and img_data[8:12] == b'WEBP':
                file_ext = ".webp"

            # 先尝试上传到图床（如果需要）
            remote_url = None
            remote_uploaded = False
            if need_remote:
                remote_url = await self._upload_to_remote_by_config(img_data, sha256_hash, file_ext)
                remote_uploaded = remote_url is not None

            # 本地保存（如果需要）
            if need_local:
                # 使用 Hash 作为文件名
                file_name = f"{sha256_hash}{file_ext}"
                save_path = Path(self.image_save_path).joinpath(file_name)
                # 写入磁盘
                async with aiofiles.open(save_path, mode='wb') as f:
                    await f.write(img_data)

            # 保存到数据库
            await self.storage.save_image_record(
                image_hash=sha256_hash,
                file_ext=file_ext,
                file_size=len(img_data),
                remote_url=remote_url,
                remote_uploaded=remote_uploaded,
                remote_provider=self.remote_provider if remote_uploaded else None,
                remote_endpoint=self.remote_api_endpoint if remote_uploaded else None
            )
            if remote_uploaded:
                self._verified_remote_hashes.add(sha256_hash)
            else:
                self._verified_remote_hashes.discard(sha256_hash)

            # 输出日志
            if need_remote and remote_uploaded:
                logger.info(f"新图片已归档并上传至图床: {sha256_hash}")
            elif need_local:
                logger.info(f"新图片已归档至本地: {sha256_hash}")

            return sha256_hash

        except Exception as e:
            logger.error(f"处理图片出错 {url}: {e}")
            return None

    async def _upload_to_remote_by_config(self, img_data: bytes, sha256_hash: str, file_ext: str) -> Optional[str]:
        """
        根据配置选择对应的远端图床进行上传
        返回: 上传成功返回图片URL，失败返回 None
        """
        if self.remote_provider == "cloudflare":
            return await self._upload_to_cf_imgbed(img_data, sha256_hash, file_ext)
        elif self.remote_provider == "imgbed":
            return await self._upload_to_imgbed(img_data, sha256_hash, file_ext)
        else:
            logger.error(f"未知的远端图床类型: {self.remote_provider}")
            return None

    @filter.event_message_type(filter.EventMessageType.ALL)
    async def on_all_message(self, event: AstrMessageEvent):
        try:
            msg = event.message_obj
            meta = event.platform_meta

            # 收集所有图片的 Hash 和 转发消息数据
            image_hashes = []
            forward_data = None

            # 判断是否需要处理图片或转发消息
            if self.storage:
                for component in msg.message:
                    if isinstance(component, Image):
                        if component.url:
                            # 调用处理函数，获取 Hash
                            img_hash = await self._process_image(component.url)
                            if img_hash:
                                image_hashes.append(img_hash)
                    elif isinstance(component, Forward):
                        # 处理转发消息
                        try:
                            adapter = self.context.get_platform_inst(event.get_platform_id())
                            if isinstance(adapter, AiocqhttpAdapter):
                                # 获取转发详情
                                nodes_res = await adapter.bot.call_action("get_forward_msg", message_id=component.id)
                                if nodes_res and "messages" in nodes_res:
                                    forward_data = []
                                    for node in nodes_res["messages"]:
                                        # 简化节点信息用于展示
                                        forward_data.append({
                                            "sender": node.get("sender", {}).get("nickname", "未知"),
                                            "content": node.get("message", "[未知内容]")
                                        })
                        except Exception as fe:
                            logger.error(f"获取转发消息详情失败: {fe}")

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
                    forward_data=forward_data,
                    timestamp=msg.timestamp
                )

        except Exception as e:
            logger.error(f"日志记录异常: {e}")

    async def terminate(self):
        await self._stop_webui()
        if self.storage:
            await self.storage.terminate()

    async def _start_webui(self):
        webui_config = self.config.get("webui", {})
        if not webui_config.get("enabled", False):
            return
        
        if self.webui_server:
            return
        
        try:
            self.webui_server = WebUIServer(
                storage=self.storage,
                config=webui_config,
                image_save_path=self.image_save_path
            )
            await self.webui_server.start()
        except Exception as e:
            logger.error(f"启动 WebUI 失败: {e}")

    async def _stop_webui(self):
        if self.webui_server:
            await self.webui_server.stop()
            self.webui_server = None

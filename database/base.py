import abc
from typing import Optional

class BaseStorage(abc.ABC):
    @abc.abstractmethod
    async def initialize(self) -> None:
        """初始化数据库连接及各类迁移表结构"""
        pass

    @abc.abstractmethod
    async def terminate(self) -> None:
        """关闭数据库连接"""
        pass

    @abc.abstractmethod
    async def check_image_exists(self, sha256_hash: str) -> bool:
        """检查数据库中是否存在该图片hash记录"""
        pass

    @abc.abstractmethod
    async def save_image_record(self, image_hash: str, file_ext: str, file_size: int) -> None:
        """保存图片记录至数据库"""
        pass

    @abc.abstractmethod
    async def save_message(self, message_id: str, platform_type: str, self_id: str, session_id: str, group_id: Optional[str], sender_data: dict, message_str: str, raw_message: dict, image_hashes: list, timestamp: int) -> None:
        """保存消息记录至数据库"""
        pass

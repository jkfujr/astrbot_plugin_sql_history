import aiomysql
import datetime
from astrbot import logger

class MigrationManager:
    def __init__(self, pool: aiomysql.Pool):
        self.pool = pool
        self.target_version = 2 # 当前代码支持的最高版本

    async def get_current_version(self) -> int:
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                # 检查版本追踪表是否存在
                await cursor.execute("SHOW TABLES LIKE 'plugin_schema_version'")
                if not await cursor.fetchone():
                    return 0
                
                await cursor.execute("SELECT MAX(version) FROM plugin_schema_version")
                result = await cursor.fetchone()
                return result[0] if result and result[0] is not None else 0

    async def upgrade_to_latest(self):
        current_version = await self.get_current_version()
        if current_version >= self.target_version:
            logger.debug(f"数据库已是最新版本 (v{current_version})")
            return

        logger.info(f"检测到数据库版本 v{current_version}，正在升级至 v{self.target_version}...")
        
        for version in range(current_version + 1, self.target_version + 1):
            await self._apply_migration(version)
            logger.info(f"数据库升级成功: v{version}")

    async def _apply_migration(self, version: int):
        migration_func = getattr(self, f"_migration_v{version}", None)
        if not migration_func:
            raise Exception(f"未找到版本 v{version} 的迁移逻辑")

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                try:
                    # 1. 在升级前进行备份 (仅对已有表进行快照)
                    if version > 1: # v1 是初始建表，无需备份
                        tables_to_backup = ["image_assets", "messages"]
                        for table in tables_to_backup:
                            backup_name = f"{table}_bak_v{version-1}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"
                            logger.info(f"正在备份表 {table} 至 {backup_name}...")
                            await cursor.execute(f"CREATE TABLE {backup_name} SELECT * FROM {table}")

                    # 2. 执行特定版本的迁移逻辑
                    await migration_func(cursor)
                    
                    # 3. 记录迁移版本
                    await cursor.execute(
                        "INSERT INTO plugin_schema_version (version, applied_at) VALUES (%s, %s)",
                        (version, datetime.datetime.now())
                    )
                    await conn.commit()
                except Exception as e:
                    await conn.rollback()
                    logger.error(f"执行数据库迁移 v{version} 时出错 (已尝试回滚): {e}")
                    raise

    async def _migration_v1(self, cursor):
        """初始建表"""
        # 创建版本追踪表
        await cursor.execute("""
            CREATE TABLE IF NOT EXISTS plugin_schema_version (
                version    INT PRIMARY KEY,
                applied_at DATETIME NOT NULL
            )
        """)
        
        # 创建图片资源表
        await cursor.execute("""
            CREATE TABLE IF NOT EXISTS image_assets (
                image_hash   VARCHAR(64) PRIMARY KEY,
                file_path    TEXT NOT NULL,
                file_size    INT,
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
                raw_message   LONGTEXT,
                image_ids     JSON,
                timestamp     INT          NOT NULL,
                created_time  DATETIME     NOT NULL
            )
        """)

    async def _migration_v2(self, cursor):
        """新增 file_ext 字段，并从 file_path 中迁移后缀数据"""
        # 1. 添加 file_ext 字段
        await cursor.execute("SHOW COLUMNS FROM image_assets LIKE 'file_ext'")
        if not await cursor.fetchone():
             await cursor.execute("ALTER TABLE image_assets ADD COLUMN file_ext VARCHAR(10) AFTER image_hash")
        
        # 2. 存量数据提取后缀名
        # 通过 SQL 提取最后一个点之后的字符作为后缀
        # 注意：这里假设 file_path 存在且包含有效的扩展名
        await cursor.execute("""
            UPDATE image_assets 
            SET file_ext = CONCAT('.', SUBSTRING_INDEX(file_path, '.', -1))
            WHERE file_ext IS NULL AND file_path IS NOT NULL AND file_path LIKE '%.%';
        """)
        
        # 3. 如果依然有 NULL（比如文件没后缀），设置默认值
        await cursor.execute("UPDATE image_assets SET file_ext = '.jpg' WHERE file_ext IS NULL")

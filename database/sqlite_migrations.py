import aiosqlite
import datetime
from astrbot import logger

class SQLiteMigrationManager:
    def __init__(self, conn: aiosqlite.Connection):
        self.conn = conn
        self.target_version = 3 # 当前代码支持的最高版本

    async def get_current_version(self) -> int:
        async with self.conn.cursor() as cursor:
            # 检查版本追踪表是否存在
            await cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='plugin_schema_version'")
            if not await cursor.fetchone():
                # 兼容：如果 plugin_schema_version 不存在，但 image_assets 存在，说明是从之前的隐式 v2 升级来的
                await cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='image_assets'")
                if await cursor.fetchone():
                    return 2
                return 0
            
            await cursor.execute("SELECT MAX(version) FROM plugin_schema_version")
            result = await cursor.fetchone()
            return result[0] if result and result[0] is not None else 0

    async def upgrade_to_latest(self):
        current_version = await self.get_current_version()
        if current_version >= self.target_version:
            logger.debug(f"SQLite 数据库已是最新版本 (v{current_version})")
            return

        logger.info(f"检测到 SQLite 数据库版本 v{current_version}，正在升级至 v{self.target_version}...")
        
        for version in range(current_version + 1, self.target_version + 1):
            await self._apply_migration(version)
            logger.info(f"SQLite 数据库升级成功: v{version}")

    async def _apply_migration(self, version: int):
        migration_func = getattr(self, f"_migration_v{version}", None)
        if not migration_func:
            raise Exception(f"未找到版本 v{version} 的迁移逻辑")

        async with self.conn.cursor() as cursor:
            try:
                # 1. 如果不是 v1，执行备份。SQLite 可以直接 CREATE TABLE AS SELECT
                if version > 1:
                    tables_to_backup = ["image_assets", "messages"]
                    for table in tables_to_backup:
                        # 先检查表是否存在
                        await cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,))
                        if await cursor.fetchone():
                            timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
                            backup_name = f"{table}_v{version-1}_{timestamp}_bak"
                            logger.info(f"正在备份 SQLite 表 {table} 至 {backup_name}...")
                            await cursor.execute(f"CREATE TABLE {backup_name} AS SELECT * FROM {table}")

                # 2. 执行特定版本的迁移逻辑
                await migration_func(cursor)
                
                # 3. 记录迁移版本
                # 确保版本表存在
                await cursor.execute("""
                    CREATE TABLE IF NOT EXISTS plugin_schema_version (
                        version    INTEGER PRIMARY KEY,
                        applied_at DATETIME NOT NULL
                    )
                """)
                await cursor.execute(
                    "INSERT INTO plugin_schema_version (version, applied_at) VALUES (?, ?)",
                    (version, datetime.datetime.now())
                )
                await self.conn.commit()
            except Exception as e:
                await self.conn.rollback()
                logger.error(f"执行 SQLite 数据库迁移 v{version} 时出错 (已尝试回滚): {e}")
                raise

    async def _migration_v1(self, cursor):
        """初始建表"""
        await cursor.execute("""
            CREATE TABLE IF NOT EXISTS image_assets (
                image_hash   VARCHAR(64) PRIMARY KEY,
                file_path    TEXT NOT NULL,
                file_size    INTEGER,
                created_time DATETIME NOT NULL
            )
        """)

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

    async def _migration_v2(self, cursor):
        """新增 file_ext 字段"""
        await cursor.execute("PRAGMA table_info(image_assets)")
        columns = [row[1] for row in await cursor.fetchall()]
        if 'file_ext' not in columns:
            await cursor.execute("ALTER TABLE image_assets ADD COLUMN file_ext VARCHAR(10)")
            await cursor.execute("""
                UPDATE image_assets 
                SET file_ext = '.jpg'
                WHERE file_ext IS NULL
            """)

    async def _migration_v3(self, cursor):
        """物理移除废弃的 file_path 字段"""
        await cursor.execute("PRAGMA table_info(image_assets)")
        columns = [row[1] for row in await cursor.fetchall()]
        if 'file_path' in columns:
            try:
                # SQLite 3.35.0 (2021-03) 之后支持 DROP COLUMN
                await cursor.execute("ALTER TABLE image_assets DROP COLUMN file_path")
            except Exception as e:
                logger.warning(f"当前 SQLite 版本不支持 DROP COLUMN，启用表重建来移除 file_path: {e}")
                # 旧版 SQLite 移除字段的变通方案
                await cursor.execute("""
                    CREATE TABLE image_assets_new (
                        image_hash   VARCHAR(64) PRIMARY KEY,
                        file_ext     VARCHAR(10),
                        file_size    INTEGER,
                        created_time DATETIME NOT NULL
                    )
                """)
                await cursor.execute("""
                    INSERT INTO image_assets_new (image_hash, file_ext, file_size, created_time)
                    SELECT image_hash, file_ext, file_size, created_time FROM image_assets
                """)
                await cursor.execute("DROP TABLE image_assets")
                await cursor.execute("ALTER TABLE image_assets_new RENAME TO image_assets")

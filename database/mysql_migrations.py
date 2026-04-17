import aiomysql
import datetime
from astrbot import logger

class MigrationManager:
    def __init__(self, pool: aiomysql.Pool):
        self.pool = pool
        self.target_version = 7 # 当前代码支持的最高版本

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
                            timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
                            backup_name = f"{table}_v{version-1}_{timestamp}_bak"
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
                forward_data  JSON,
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

    async def _migration_v3(self, cursor):
        """彻底移除遗留的 file_path 字段以解耦绝对路径"""
        await cursor.execute("SHOW COLUMNS FROM image_assets LIKE 'file_path'")
        if await cursor.fetchone():
             await cursor.execute("ALTER TABLE image_assets DROP COLUMN file_path")

    async def _migration_v4(self, cursor):
        """添加多群组检索的联合索引以提升百万级别海量聊天记录拉取性能"""
        # MySQL 不支持 CREATE INDEX IF NOT EXISTS，先查询
        await cursor.execute("SHOW INDEX FROM messages WHERE Key_name = 'idx_group_time'")
        if not await cursor.fetchone():
            await cursor.execute("CREATE INDEX idx_group_time ON messages (platform_type, group_id, timestamp)")

        await cursor.execute("SHOW INDEX FROM messages WHERE Key_name = 'idx_session_time'")
        if not await cursor.fetchone():
            await cursor.execute("CREATE INDEX idx_session_time ON messages (session_id, timestamp)")

    async def _migration_v5(self, cursor):
        """为 image_assets 添加 CloudFlare ImgBed 相关字段"""
        # 添加 CF 相关字段
        await cursor.execute("SHOW COLUMNS FROM image_assets LIKE 'cf_url'")
        if not await cursor.fetchone():
            await cursor.execute("""
                ALTER TABLE image_assets
                ADD COLUMN cf_url TEXT AFTER file_size,
                ADD COLUMN cf_uploaded TINYINT(1) DEFAULT 0 AFTER cf_url,
                ADD COLUMN cf_upload_time DATETIME NULL AFTER cf_uploaded
            """)

    async def _migration_v6(self, cursor):
        """新增 forward_data 字段"""
        await cursor.execute("SHOW COLUMNS FROM messages LIKE 'forward_data'")
        if not await cursor.fetchone():
            await cursor.execute("ALTER TABLE messages ADD COLUMN forward_data JSON AFTER image_ids")

    async def _migration_v7(self, cursor):
        """重构图床字段：cf_* → remote_*，新增 provider 和 endpoint"""

        # 步骤1: 添加新字段
        await cursor.execute("SHOW COLUMNS FROM image_assets LIKE 'remote_url'")
        if not await cursor.fetchone():
            await cursor.execute("""
                ALTER TABLE image_assets
                ADD COLUMN remote_url TEXT AFTER file_size,
                ADD COLUMN remote_uploaded TINYINT(1) DEFAULT 0 AFTER remote_url,
                ADD COLUMN remote_upload_time DATETIME NULL AFTER remote_uploaded,
                ADD COLUMN remote_provider VARCHAR(20) AFTER remote_upload_time,
                ADD COLUMN remote_endpoint TEXT AFTER remote_provider
            """)

        # 步骤2: 迁移旧数据
        await cursor.execute("""
            SELECT image_hash, cf_url, cf_uploaded, cf_upload_time
            FROM image_assets
            WHERE cf_url IS NOT NULL OR cf_uploaded = 1
        """)
        old_records = await cursor.fetchall()

        logger.info(f"开始迁移 {len(old_records)} 条旧图床记录...")

        migrated_count = 0
        invalidated_count = 0

        for row in old_records:
            image_hash, cf_url, cf_uploaded, cf_upload_time = row
            provider, endpoint = _extract_provider_endpoint(cf_url)

            if provider == 'unknown':
                await cursor.execute("""
                    UPDATE image_assets
                    SET remote_url = NULL, remote_uploaded = 0, remote_upload_time = NULL,
                        remote_provider = NULL, remote_endpoint = NULL
                    WHERE image_hash = %s
                """, (image_hash,))
                invalidated_count += 1
                logger.warning(f"无法识别旧URL，已失效: {image_hash[:8]}... -> {cf_url}")
            else:
                await cursor.execute("""
                    UPDATE image_assets
                    SET remote_url = %s, remote_uploaded = %s, remote_upload_time = %s,
                        remote_provider = %s, remote_endpoint = %s
                    WHERE image_hash = %s
                """, (cf_url, cf_uploaded, cf_upload_time, provider, endpoint, image_hash))
                migrated_count += 1

        logger.info(f"迁移完成: {migrated_count} 条有效, {invalidated_count} 条失效")

        # 步骤3: 删除旧字段
        await cursor.execute("SHOW COLUMNS FROM image_assets LIKE 'cf_url'")
        if await cursor.fetchone():
            await cursor.execute("""
                ALTER TABLE image_assets
                DROP COLUMN cf_url,
                DROP COLUMN cf_uploaded,
                DROP COLUMN cf_upload_time
            """)

        # 步骤4: 验证迁移结果
        await cursor.execute("SHOW COLUMNS FROM image_assets")
        final_columns = [row[0] for row in await cursor.fetchall()]

        required_fields = ['remote_url', 'remote_uploaded', 'remote_upload_time',
                           'remote_provider', 'remote_endpoint']
        for field in required_fields:
            if field not in final_columns:
                raise Exception(f"迁移验证失败: 新字段 {field} 不存在")

        deprecated_fields = ['cf_url', 'cf_uploaded', 'cf_upload_time']
        for field in deprecated_fields:
            if field in final_columns:
                raise Exception(f"迁移验证失败: 旧字段 {field} 未删除")

        logger.info("✓ v7 迁移验证通过")


def _extract_provider_endpoint(cf_url: str) -> tuple[str, str]:
    """从旧 cf_url 识别 provider 和 endpoint"""
    if not cf_url or not isinstance(cf_url, str):
        return ('unknown', '')

    try:
        from urllib.parse import urlparse
        parsed = urlparse(cf_url.strip())

        if not parsed.scheme or not parsed.netloc:
            return ('unknown', '')

        base_url = f"{parsed.scheme}://{parsed.netloc}"
        path = parsed.path.lower()

        # CloudFlare-ImgBed 特征：/file/ 路径
        if '/file/' in path:
            return ('cloudflare', base_url)

        # ImgBed 特征：/uploads/ 或 /api/ 路径
        if '/uploads/' in path or '/api/' in path:
            return ('imgbed', base_url)

        return ('unknown', base_url)
    except Exception:
        return ('unknown', '')

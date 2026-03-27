import asyncio
import json
import secrets
import time
from pathlib import Path
from typing import Any, Optional

import uvicorn
from fastapi import Depends, FastAPI, HTTPException, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, FileResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles

from astrbot.api import logger

class WebUIServer:
    def __init__(self, storage, config: dict[str, Any], image_save_path: str):
        self.storage = storage
        self.config = config
        self.image_save_path = image_save_path
        
        self.host = str(config.get("host", "0.0.0.0"))
        self.port = int(config.get("port", 11451))
        self.session_timeout = 3600
        self._access_password = str(config.get("access_password", "")).strip()
        
        if not self._access_password:
            self._access_password = secrets.token_urlsafe(16)
            logger.info(f"WebUI 未设置访问密码，已自动生成随机密码: {self._access_password}")
            
        self._tokens: dict[str, dict[str, float]] = {}
        self._token_lock = asyncio.Lock()
        
        self._server: uvicorn.Server | None = None
        self._server_task: asyncio.Task | None = None
        
        self._app = FastAPI(title="AstrBot SQL History WebUI", version="1.0.0")
        self._setup_routes()

    async def start(self):
        if self._server_task and not self._server_task.done():
            return

        config = uvicorn.Config(
            app=self._app,
            host=self.host,
            port=self.port,
            log_level="info",
            loop="asyncio",
        )
        self._server = uvicorn.Server(config)
        self._server_task = asyncio.create_task(self._server.serve())
        logger.info(f"WebUI started at: http://{self.host}:{self.port}")

    async def stop(self):
        if self._server:
            self._server.should_exit = True
        if self._server_task:
            await self._server_task
        self._server = None
        self._server_task = None

    def _auth_dependency(self):
        async def dependency(request: Request) -> str:
            auth_header = request.headers.get("Authorization", "")
            token = auth_header[7:] if auth_header.startswith("Bearer ") else request.headers.get("X-Auth-Token", "")
            
            if not token:
                raise HTTPException(status_code=401, detail="未提供认证Token")
                
            async with self._token_lock:
                token_info = self._tokens.get(token)
                if not token_info or (time.time() - token_info["last_active"] > self.session_timeout):
                    self._tokens.pop(token, None)
                    raise HTTPException(status_code=401, detail="Token无效或已过期")
                token_info["last_active"] = time.time()
            return token
        return dependency

    def _setup_routes(self):
        static_dir = Path(__file__).resolve().parent.parent / "static"
        
        self._app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_methods=["*"],
            allow_headers=["*"],
        )

        if static_dir.exists():
            # 挂载 assets 目录在 /assets 路径下，因为 index.html 引用的是 /assets/
            assets_dir = static_dir / "assets"
            if assets_dir.exists():
                self._app.mount("/assets", StaticFiles(directory=str(assets_dir)), name="assets")
            # 同时也保留根路径 index.html 的手动提供，或者挂载整个 static 到根（放在最后）
            # 但为了不影响 /api，我们手动处理 / 并在 assets 目录下挂载
            
        @self._app.get("/", response_class=HTMLResponse)
        async def serve_index():
            index_path = static_dir / "index.html"
            if not index_path.exists():
                return HTMLResponse("Frontend files not found. Please build the frontend first.")
            return HTMLResponse(index_path.read_text(encoding="utf-8"))

        @self._app.post("/api/login")
        async def login(payload: dict[str, Any]):
            if payload.get("password") == self._access_password:
                token = secrets.token_urlsafe(32)
                async with self._token_lock:
                    self._tokens[token] = {"created_at": time.time(), "last_active": time.time()}
                return {"token": token}
            raise HTTPException(status_code=401, detail="密码错误")

        @self._app.get("/api/sessions")
        async def get_sessions(token: str = Depends(self._auth_dependency())):
            sessions = await self.storage.get_sessions()
            return {"data": sessions}

        @self._app.get("/api/messages")
        async def get_messages(session_id: str, page: int = 1, page_size: int = 20, token: str = Depends(self._auth_dependency())):
            offset = (page - 1) * page_size
            messages = await self.storage.get_messages(session_id, page_size, offset)
            return {"data": messages}

        @self._app.get("/api/images/{image_hash}")
        async def get_image(image_hash: str):
            # 1. 优先尝试本地
            info = await self.storage.get_image_info(image_hash)
            if info:
                ext = info.get('file_ext', '.jpg')
                local_path = Path(self.image_save_path) / f"{image_hash}{ext}"
                if local_path.exists():
                    return FileResponse(local_path)
                
                # 2. 本地没有，尝试图床
                if info.get('cf_url'):
                    return RedirectResponse(info['cf_url'])
            
            raise HTTPException(status_code=404, detail="图片不存在")

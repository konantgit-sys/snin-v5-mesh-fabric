"""
mesh_config.py — единый конфиг SNIN V5 Mesh Fabric.

Использование:
    from mesh_config import config
    port = config.get("transport.smart_router.port")        # → 9932
    port = config.health_port_for("smart_router", 9932)     # → 19932

Все порты читаются из mesh_config.yaml. Одно место правки.
"""
import os
import yaml
from typing import Any, Optional

_CONFIG_PATH = os.path.join(os.path.dirname(__file__), "mesh_config.yaml")


class MeshConfig:
    """Прокси для доступа к конфигу через dot-нотацию"""

    def __init__(self, path: str = _CONFIG_PATH):
        self.path = path
        self._data: dict = {}
        self._reload()

    def _reload(self):
        with open(self.path) as f:
            self._data = yaml.safe_load(f) or {}

    def get(self, key: str, default: Any = None) -> Any:
        """config.get('transport.smart_router.port') → 9932"""
        parts = key.split(".")
        val = self._data
        for p in parts:
            if isinstance(val, dict):
                val = val.get(p)
            else:
                return default
            if val is None:
                return default
        return val

    def set(self, key: str, value: Any):
        """config.set('transport.smart_router.port', 9933) — временно, в runtime"""
        parts = key.split(".")
        d = self._data
        for p in parts[:-1]:
            d = d.setdefault(p, {})
        d[parts[-1]] = value

    def health_port_for(self, service_key: str, service_port: int = None) -> int:
        """Рассчитать mesh_health порт (service_port + offset).
        Если service_port не указан — пытается найти в конфиге."""
        if service_port is None:
            service_port = self.get(service_key) or 0
        offset = self.get("global.health_port_offset", 10000)
        return service_port + offset

    def as_dict(self) -> dict:
        return dict(self._data)

    def reload(self):
        self._reload()

    def __getattr__(self, name):
        return self._data.get(name)

    # Поддержка контекстного менеджера для временных изменений
    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


# ─── SINGLETON ───
config = MeshConfig()

# ─── QUICK TEST ───
if __name__ == "__main__":
    import json
    print("=== MeshConfig Test ===")
    print(f"smart_router port:   {config.get('transport.smart_router.port')}")
    print(f"health offset:       {config.get('global.health_port_offset')}")
    print(f"health port (SR):    {config.health_port_for('transport.smart_router.port', 9932)}")
    print(f"nostr bridge base:   {config.get('nostr.bridge_base_port')}")
    print(f"redis host:port:     {config.get('redis.host')}:{config.get('redis.port')}")
    print(f"health engine port:  {config.get('orchestration.health_engine.port')}")
    print("\n✅ Config loaded OK")

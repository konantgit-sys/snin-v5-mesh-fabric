"""SNIN Mesh Network Config — для внутренних и удалённых агентов."""

# Публичный API (relay-mesh registry)
PUBLIC_API_URL = "https://snin-gossip.v2.site"

# Локальный API (нужен пока агенты регистрируются на localhost)
LOCAL_API_URL = "http://127.0.0.1:9907"

# Внешний IP этого сервера
EXTERNAL_IP = "155.212.133.195"

# Gossip порты агентов
GOSSIP_PORTS = {
    "forecaster_ai": 9911,
    "archivist_ai": 9912,
    "anton_ai": 9913,
}

# Тип узла: "hub" — наш сервер, "remote" — внешняя нода
NODE_TYPE = "hub"

# Для удалённого агента:
REMOTE_CONFIG = {
    "api_url": "https://snin-gossip.v2.site",
    "gossip_host": "0.0.0.0",  # слушать на всех интерфейсах
    "heartbeat_interval": 30,
    "test_duration_minutes": 60,  # тест на 1 час
    "auto_cleanup": True,  # удалить регистрацию после теста
}

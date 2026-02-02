import os

# ============================================
# CONFIGURATION TELEGRAM
# ============================================
API_ID = 29177661
API_HASH = "a8639172fa8d35dbfd8ea46286d349ab"
BOT_TOKEN = "8131011456:AAGPWIFCfQoGuSlL-GcAw2s96rLbOn5I_c0"
ADMIN_ID = 1190237801

# ============================================
# CANAUX
# ============================================
SOURCE_CHANNEL_ID = -1003376569543
BILAN_CHANNEL_ID = -1003869393224

# ============================================
# CONFIGURATION JOURNÉE
# ============================================
# Début et fin de la journée de jeu
JOUR_START = 6      # La journée commence au numéro 6
JOUR_END = 1436     # La journée finit au numéro 1436

# ============================================
# SERVEUR
# ============================================
RENDER_DEPLOYMENT = True
PORT = 10000
TELEGRAM_SESSION = ""
BILAN_INTERVAL_MINUTES = 30

# ============================================
# POSTGRESQL
# ============================================
DB_HOST = "dpg-d60gldpr0fns73fcee9g-a.oregon-postgres.render.com"
DB_PORT = 5432
DB_NAME = "base_de_donnees_s4aq"
DB_USER = "base_de_donnees_s4aq_user"
DB_PASSWORD = "qYcfWpSs7FvZQgLpM4M8ZK5koAMEYZI4"
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Validation
if not API_ID or API_ID == 0:
    raise ValueError("API_ID est requis")
if not API_HASH:
    raise ValueError("API_HASH est requis")
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN est requis")


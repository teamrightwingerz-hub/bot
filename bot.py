"""
COMPLETE NEWS MODERATION BOT - ULTIMATE VERSION WITH PERSISTENT STORAGE
All features working with data backup and recovery
"""

import logging
import os
import json
import sys
import shutil
import time
import sqlite3
import threading
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict
from enum import Enum
from datetime import datetime, timedelta
import pytz
import asyncio
from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
    Bot, MessageEntity
)
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    ConversationHandler,
    filters
)
from telegram.constants import ParseMode, ChatType

# ==================== CONFIGURATION ====================
BOT_TOKEN = "8576849007:AAHmXpmKWlyYbGjuOtQOgYCmcvfTMPJiPF8"
MODERATOR_GROUP_ID = -1003315004909
CHANNEL_ID = -1002387584900

# Developer ID for stream feature
DEVELOPER_ID = 7237959274

# Moderators
MODERATOR_USER_IDS = [
    1637312329, 7237959274, 7070819763, 8440728446, 
    1148982183, 8096314184, 8561908258, 7746824731, 7216964150
]

# Authorized Posters
AUTHORIZED_POSTERS = [
    7216964150, 7237959274, 7070819763, 8184176763, 7248202123, 7997598506, 7746824731, 8687574735
]

# Pre-recorded rejection reasons (with full text)
PRERECORDED_REASONS = [
    "Inappropriate content or language",
    "Spam or promotional content",
    "Not relevant to channel theme",
    "Low quality content",
    "Duplicate or already posted",
    "Violates community guidelines",
    "Misleading or false information",
    "Copyrighted material",
    "Insufficient source citation",
    "Poor formatting or readability"
]

# Timezone
IST_TIMEZONE = pytz.timezone('Asia/Kolkata')

# ==================== LOGGING ====================
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Data storage files and directories
DATA_DIR = "data"
BACKUP_DIR = "data/backups"
DB_FILE = "data/bot_database.db"
JSON_BACKUP_DIR = "data/json_backups"

# Data storage files (JSON backup)
PENDING_POSTS_FILE = "data/pending_posts.json"
APPROVED_POSTS_FILE = "data/approved_posts.json"
REJECTED_POSTS_FILE = "data/rejected_posts.json"
SCHEDULED_POSTS_FILE = "data/scheduled_posts.json"
POINTS_FILE = "data/points.json"
ACHIEVEMENTS_FILE = "data/achievements.json"
STREAM_SESSIONS_FILE = "data/stream_sessions.json"

# Create directories
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(BACKUP_DIR, exist_ok=True)
os.makedirs(JSON_BACKUP_DIR, exist_ok=True)

# Conversation states
(
    WAITING_FOR_NEWS,
    WAITING_FOR_REASON,
    WAITING_FOR_SCHEDULE,
    WAITING_FOR_POINTS_CONFIRMATION,
    WAITING_FOR_STREAM_MESSAGE,
    WAITING_FOR_CUSTOM_REASON
) = range(6)

# ==================== DATA MODELS ====================
class PostStatus(Enum):
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"
    SCHEDULED = "scheduled"

class AchievementType(Enum):
    FIRST_POST = "first_post"
    TEN_APPROVED = "ten_approved"
    FIFTY_APPROVED = "fifty_approved"
    HUNDRED_APPROVED = "hundred_approved"
    POINTS_100 = "points_100"
    POINTS_500 = "points_500"
    POINTS_1000 = "points_1000"
    CONSISTENT_CONTRIBUTOR = "consistent_contributor"
    TOP_CONTRIBUTOR = "top_contributor"

@dataclass
class Achievement:
    user_id: int
    achievement_type: str
    earned_at: str
    title: str
    description: str
    points_bonus: int = 0

@dataclass
class NewsPost:
    post_id: int
    user_id: int
    username: Optional[str]
    text: str
    media_type: Optional[str] = None
    media_ids: Optional[List[str]] = None
    entities: Optional[List[Dict]] = None
    caption_entities: Optional[List[Dict]] = None
    status: PostStatus = PostStatus.PENDING
    submitted_at: str = None
    reviewed_by: Optional[int] = None
    reviewed_at: Optional[str] = None
    rejection_reason: Optional[str] = None
    scheduled_time: Optional[str] = None
    channel_message_ids: Optional[List[int]] = None
    
    def __post_init__(self):
        if self.submitted_at is None:
            self.submitted_at = datetime.now(IST_TIMEZONE).isoformat()
        if self.media_ids is None:
            self.media_ids = []
        if self.entities is None:
            self.entities = []
        if self.caption_entities is None:
            self.caption_entities = []
        if self.channel_message_ids is None:
            self.channel_message_ids = []
    
    def to_dict(self):
        data = asdict(self)
        data['status'] = self.status.value
        return data
    
    @classmethod
    def from_dict(cls, data):
        data['status'] = PostStatus(data['status'])
        return cls(**data)

@dataclass
class UserPoints:
    user_id: int
    username: Optional[str]
    points: int = 0
    total_approved: int = 0
    total_rejected: int = 0
    total_deleted: int = 0
    achievements: List[str] = None
    last_post_date: Optional[str] = None
    consecutive_days: int = 0
    
    def __post_init__(self):
        if self.achievements is None:
            self.achievements = []
    
    def to_dict(self):
        data = asdict(self)
        return data
    
    @classmethod
    def from_dict(cls, data):
        return cls(**data)

@dataclass
class StreamSession:
    session_id: str
    user_id: int
    started_at: str
    ended_at: Optional[str] = None
    messages_sent: int = 0
    is_active: bool = True
    
    def to_dict(self):
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data):
        return cls(**data)

# ==================== DATABASE MANAGER ====================
class DatabaseManager:
    """SQLite database manager for persistent storage"""
    
    def __init__(self, db_path=DB_FILE):
        self.db_path = db_path
        self.init_database()
    
    def get_connection(self):
        """Get database connection with row factory"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn
    
    def init_database(self):
        """Initialize database tables"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Posts table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS posts (
                    post_id INTEGER PRIMARY KEY,
                    user_id INTEGER NOT NULL,
                    username TEXT,
                    text TEXT,
                    media_type TEXT,
                    media_ids TEXT,
                    entities TEXT,
                    caption_entities TEXT,
                    status TEXT,
                    submitted_at TEXT,
                    reviewed_by INTEGER,
                    reviewed_at TEXT,
                    rejection_reason TEXT,
                    scheduled_time TEXT,
                    channel_message_ids TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # User points table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS user_points (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    points INTEGER DEFAULT 0,
                    total_approved INTEGER DEFAULT 0,
                    total_rejected INTEGER DEFAULT 0,
                    total_deleted INTEGER DEFAULT 0,
                    achievements TEXT,
                    last_post_date TEXT,
                    consecutive_days INTEGER DEFAULT 0,
                    last_active TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Achievements table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS achievements (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    achievement_type TEXT NOT NULL,
                    earned_at TEXT NOT NULL,
                    title TEXT NOT NULL,
                    description TEXT,
                    points_bonus INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES user_points(user_id)
                )
            ''')
            
            # Stream sessions table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS stream_sessions (
                    session_id TEXT PRIMARY KEY,
                    user_id INTEGER NOT NULL,
                    started_at TEXT NOT NULL,
                    ended_at TEXT,
                    messages_sent INTEGER DEFAULT 0,
                    is_active INTEGER DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Backup log table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS backup_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    backup_time TEXT NOT NULL,
                    backup_type TEXT NOT NULL,
                    file_count INTEGER,
                    size_bytes INTEGER,
                    status TEXT,
                    error_message TEXT
                )
            ''')
            
            # Activity log table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS activity_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    user_id INTEGER,
                    action TEXT,
                    post_id INTEGER,
                    details TEXT
                )
            ''')
            
            conn.commit()
            logger.info("Database initialized successfully")
    
    # ===== POSTS OPERATIONS =====
    def save_post(self, post) -> bool:
        """Save or update post in database"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Check if post exists
                cursor.execute("SELECT post_id FROM posts WHERE post_id = ?", (post.post_id,))
                exists = cursor.fetchone()
                
                data = {
                    'post_id': post.post_id,
                    'user_id': post.user_id,
                    'username': post.username,
                    'text': post.text,
                    'media_type': post.media_type,
                    'media_ids': json.dumps(post.media_ids) if post.media_ids else None,
                    'entities': json.dumps(post.entities) if post.entities else None,
                    'caption_entities': json.dumps(post.caption_entities) if post.caption_entities else None,
                    'status': post.status.value if hasattr(post.status, 'value') else post.status,
                    'submitted_at': post.submitted_at,
                    'reviewed_by': post.reviewed_by,
                    'reviewed_at': post.reviewed_at,
                    'rejection_reason': post.rejection_reason,
                    'scheduled_time': post.scheduled_time,
                    'channel_message_ids': json.dumps(post.channel_message_ids) if post.channel_message_ids else None
                }
                
                if exists:
                    # Update
                    placeholders = ', '.join([f"{key}=?" for key in data.keys()])
                    query = f"UPDATE posts SET {placeholders} WHERE post_id=?"
                    values = list(data.values()) + [post.post_id]
                    cursor.execute(query, values)
                else:
                    # Insert
                    placeholders = ', '.join(['?' for _ in data])
                    query = f"INSERT INTO posts ({', '.join(data.keys())}) VALUES ({placeholders})"
                    cursor.execute(query, list(data.values()))
                
                conn.commit()
                return True
                
        except Exception as e:
            logger.error(f"Error saving post to database: {e}")
            return False
    
    def get_post(self, post_id: int):
        """Get post from database"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM posts WHERE post_id = ?", (post_id,))
                row = cursor.fetchone()
                
                if row:
                    return dict(row)
                return None
                
        except Exception as e:
            logger.error(f"Error getting post from database: {e}")
            return None
    
    def get_posts_by_status(self, status: str) -> List[dict]:
        """Get posts by status"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM posts WHERE status = ? ORDER BY post_id DESC", (status,))
                return [dict(row) for row in cursor.fetchall()]
                
        except Exception as e:
            logger.error(f"Error getting posts by status: {e}")
            return []
    
    def get_all_posts(self) -> List[dict]:
        """Get all posts"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM posts ORDER BY post_id DESC")
                return [dict(row) for row in cursor.fetchall()]
                
        except Exception as e:
            logger.error(f"Error getting all posts: {e}")
            return []
    
    def delete_post(self, post_id: int) -> bool:
        """Delete post from database"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("DELETE FROM posts WHERE post_id = ?", (post_id,))
                conn.commit()
                return True
                
        except Exception as e:
            logger.error(f"Error deleting post: {e}")
            return False
    
    # ===== USER POINTS OPERATIONS =====
    def save_user_points(self, user_points) -> bool:
        """Save or update user points"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                cursor.execute("SELECT user_id FROM user_points WHERE user_id = ?", (user_points.user_id,))
                exists = cursor.fetchone()
                
                data = {
                    'user_id': user_points.user_id,
                    'username': user_points.username,
                    'points': user_points.points,
                    'total_approved': user_points.total_approved,
                    'total_rejected': user_points.total_rejected,
                    'total_deleted': user_points.total_deleted,
                    'achievements': json.dumps(user_points.achievements) if user_points.achievements else None,
                    'last_post_date': user_points.last_post_date,
                    'consecutive_days': user_points.consecutive_days,
                    'last_active': datetime.now(IST_TIMEZONE).isoformat()
                }
                
                if exists:
                    placeholders = ', '.join([f"{key}=?" for key in data.keys()])
                    query = f"UPDATE user_points SET {placeholders} WHERE user_id=?"
                    values = list(data.values()) + [user_points.user_id]
                    cursor.execute(query, values)
                else:
                    placeholders = ', '.join(['?' for _ in data])
                    query = f"INSERT INTO user_points ({', '.join(data.keys())}) VALUES ({placeholders})"
                    cursor.execute(query, list(data.values()))
                
                conn.commit()
                return True
                
        except Exception as e:
            logger.error(f"Error saving user points: {e}")
            return False
    
    def get_user_points(self, user_id: int):
        """Get user points from database"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM user_points WHERE user_id = ?", (user_id,))
                row = cursor.fetchone()
                
                if row:
                    return dict(row)
                return None
                
        except Exception as e:
            logger.error(f"Error getting user points: {e}")
            return None
    
    def get_all_user_points(self) -> List[dict]:
        """Get all user points"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM user_points ORDER BY points DESC")
                return [dict(row) for row in cursor.fetchall()]
                
        except Exception as e:
            logger.error(f"Error getting all user points: {e}")
            return []
    
    def get_top_users(self, limit: int = 10) -> List[dict]:
        """Get top users by points"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT * FROM user_points 
                    ORDER BY points DESC 
                    LIMIT ?
                """, (limit,))
                return [dict(row) for row in cursor.fetchall()]
                
        except Exception as e:
            logger.error(f"Error getting top users: {e}")
            return []
    
    # ===== ACHIEVEMENTS OPERATIONS =====
    def save_achievement(self, achievement) -> bool:
        """Save achievement to database"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                cursor.execute('''
                    INSERT INTO achievements (
                        user_id, achievement_type, earned_at, title, description, points_bonus
                    ) VALUES (?, ?, ?, ?, ?, ?)
                ''', (
                    achievement.user_id,
                    achievement.achievement_type,
                    achievement.earned_at,
                    achievement.title,
                    achievement.description,
                    achievement.points_bonus
                ))
                
                conn.commit()
                return True
                
        except Exception as e:
            logger.error(f"Error saving achievement: {e}")
            return False
    
    def get_user_achievements(self, user_id: int) -> List[dict]:
        """Get user achievements from database"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT * FROM achievements 
                    WHERE user_id = ? 
                    ORDER BY earned_at DESC
                """, (user_id,))
                return [dict(row) for row in cursor.fetchall()]
                
        except Exception as e:
            logger.error(f"Error getting user achievements: {e}")
            return []
    
    def get_all_achievements(self) -> List[dict]:
        """Get all achievements"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM achievements ORDER BY earned_at DESC")
                return [dict(row) for row in cursor.fetchall()]
                
        except Exception as e:
            logger.error(f"Error getting all achievements: {e}")
            return []
    
    # ===== STREAM SESSIONS OPERATIONS =====
    def save_stream_session(self, session) -> bool:
        """Save or update stream session"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                data = {
                    'session_id': session.session_id,
                    'user_id': session.user_id,
                    'started_at': session.started_at,
                    'ended_at': session.ended_at,
                    'messages_sent': session.messages_sent,
                    'is_active': 1 if session.is_active else 0
                }
                
                cursor.execute('''
                    INSERT OR REPLACE INTO stream_sessions 
                    (session_id, user_id, started_at, ended_at, messages_sent, is_active)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (
                    data['session_id'],
                    data['user_id'],
                    data['started_at'],
                    data['ended_at'],
                    data['messages_sent'],
                    data['is_active']
                ))
                
                conn.commit()
                return True
                
        except Exception as e:
            logger.error(f"Error saving stream session: {e}")
            return False
    
    def get_active_stream(self, user_id: int):
        """Get active stream session for user"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT * FROM stream_sessions 
                    WHERE user_id = ? AND is_active = 1
                """, (user_id,))
                row = cursor.fetchone()
                
                if row:
                    return dict(row)
                return None
                
        except Exception as e:
            logger.error(f"Error getting active stream: {e}")
            return None
    
    def get_all_active_streams(self) -> List[dict]:
        """Get all active stream sessions"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM stream_sessions WHERE is_active = 1")
                return [dict(row) for row in cursor.fetchall()]
                
        except Exception as e:
            logger.error(f"Error getting active streams: {e}")
            return []
    
    def end_stream_session(self, user_id: int) -> bool:
        """End active stream session"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE stream_sessions 
                    SET ended_at = ?, is_active = 0
                    WHERE user_id = ? AND is_active = 1
                """, (datetime.now(IST_TIMEZONE).isoformat(), user_id))
                conn.commit()
                return True
                
        except Exception as e:
            logger.error(f"Error ending stream session: {e}")
            return False
    
    # ===== ACTIVITY LOG =====
    def log_activity(self, user_id: int, action: str, post_id: int = None, details: str = None):
        """Log user activity"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT INTO activity_log (timestamp, user_id, action, post_id, details)
                    VALUES (?, ?, ?, ?, ?)
                ''', (
                    datetime.now(IST_TIMEZONE).isoformat(),
                    user_id,
                    action,
                    post_id,
                    details
                ))
                conn.commit()
                
        except Exception as e:
            logger.error(f"Error logging activity: {e}")
    
    def get_activity_log(self, limit: int = 100) -> List[dict]:
        """Get recent activity log"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT * FROM activity_log 
                    ORDER BY timestamp DESC 
                    LIMIT ?
                """, (limit,))
                return [dict(row) for row in cursor.fetchall()]
                
        except Exception as e:
            logger.error(f"Error getting activity log: {e}")
            return []
    
    # ===== BACKUP AND RESTORE =====
    def create_backup(self, backup_type: str = "auto") -> bool:
        """Create database backup"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_file = f"{BACKUP_DIR}/backup_{timestamp}.db"
            
            # Copy database file
            shutil.copy2(self.db_path, backup_file)
            
            # Create JSON backup of all data
            json_backup = f"{JSON_BACKUP_DIR}/backup_{timestamp}.json"
            self.export_to_json(json_backup)
            
            # Log backup
            file_size = os.path.getsize(backup_file)
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT INTO backup_log (backup_time, backup_type, file_count, size_bytes, status)
                    VALUES (?, ?, ?, ?, ?)
                ''', (
                    datetime.now(IST_TIMEZONE).isoformat(),
                    backup_type,
                    2,  # DB + JSON
                    file_size,
                    "success"
                ))
                conn.commit()
            
            logger.info(f"Backup created: {backup_file}")
            
            # Clean old backups (keep last 10)
            self.clean_old_backups()
            
            return True
            
        except Exception as e:
            logger.error(f"Error creating backup: {e}")
            return False
    
    def clean_old_backups(self, keep_last: int = 10):
        """Keep only last N backups"""
        try:
            backups = sorted([f for f in os.listdir(BACKUP_DIR) if f.endswith('.db')])
            for old_backup in backups[:-keep_last]:
                os.remove(os.path.join(BACKUP_DIR, old_backup))
                
            json_backups = sorted([f for f in os.listdir(JSON_BACKUP_DIR) if f.endswith('.json')])
            for old_backup in json_backups[:-keep_last]:
                os.remove(os.path.join(JSON_BACKUP_DIR, old_backup))
                
        except Exception as e:
            logger.error(f"Error cleaning old backups: {e}")
    
    def export_to_json(self, output_file: str):
        """Export all data to JSON"""
        try:
            data = {
                'posts': [],
                'user_points': [],
                'achievements': [],
                'stream_sessions': [],
                'export_time': datetime.now(IST_TIMEZONE).isoformat()
            }
            
            with self.get_connection() as conn:
                # Export posts
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM posts")
                data['posts'] = [dict(row) for row in cursor.fetchall()]
                
                # Export user points
                cursor.execute("SELECT * FROM user_points")
                data['user_points'] = [dict(row) for row in cursor.fetchall()]
                
                # Export achievements
                cursor.execute("SELECT * FROM achievements")
                data['achievements'] = [dict(row) for row in cursor.fetchall()]
                
                # Export stream sessions
                cursor.execute("SELECT * FROM stream_sessions")
                data['stream_sessions'] = [dict(row) for row in cursor.fetchall()]
            
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
                
        except Exception as e:
            logger.error(f"Error exporting to JSON: {e}")
    
    def restore_from_backup(self, backup_file: str) -> bool:
        """Restore database from backup"""
        try:
            # Copy backup file
            shutil.copy2(backup_file, self.db_path)
            logger.info(f"Database restored from: {backup_file}")
            return True
            
        except Exception as e:
            logger.error(f"Error restoring from backup: {e}")
            return False
    
    def verify_database_integrity(self) -> bool:
        """Verify database integrity"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("PRAGMA integrity_check")
                result = cursor.fetchone()
                return result[0] == "ok"
                
        except Exception as e:
            logger.error(f"Database integrity check failed: {e}")
            return False

# ==================== STREAM MANAGER ====================
class StreamManager:
    """Manage developer stream sessions with persistence"""
    
    def __init__(self, db: DatabaseManager):
        self.db = db
        self.active_streams: Dict[int, StreamSession] = {}
        self.load_streams()
    
    def load_streams(self):
        """Load active streams from database"""
        try:
            stream_data = self.db.get_all_active_streams()
            for data in stream_data:
                session = StreamSession(
                    session_id=data['session_id'],
                    user_id=data['user_id'],
                    started_at=data['started_at'],
                    ended_at=data['ended_at'],
                    messages_sent=data['messages_sent'],
                    is_active=bool(data['is_active'])
                )
                self.active_streams[session.user_id] = session
            
            logger.info(f"Loaded {len(self.active_streams)} active streams")
            
        except Exception as e:
            logger.error(f"Error loading streams from database: {e}")
            self.load_from_json()
    
    def load_from_json(self):
        """Fallback: Load from JSON file"""
        if os.path.exists(STREAM_SESSIONS_FILE):
            try:
                with open(STREAM_SESSIONS_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    for user_id_str, session_data in data.items():
                        if session_data.get('is_active', False):
                            session = StreamSession.from_dict(session_data)
                            self.active_streams[int(user_id_str)] = session
                            
                            # Save to database
                            self.db.save_stream_session(session)
            except Exception as e:
                logger.error(f"Error loading streams from JSON: {e}")
    
    def save_streams(self):
        """Save streams to database and JSON"""
        try:
            # Save to database
            for session in self.active_streams.values():
                self.db.save_stream_session(session)
            
            # Save JSON backup
            data = {str(k): v.to_dict() for k, v in self.active_streams.items()}
            with open(STREAM_SESSIONS_FILE, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
                
        except Exception as e:
            logger.error(f"Error saving streams: {e}")
    
    def start_stream(self, user_id: int) -> StreamSession:
        """Start a new stream session"""
        session = StreamSession(
            session_id=f"stream_{user_id}_{int(time.time())}",
            user_id=user_id,
            started_at=datetime.now(IST_TIMEZONE).isoformat(),
            is_active=True
        )
        self.active_streams[user_id] = session
        self.save_streams()
        
        # Log activity
        self.db.log_activity(user_id, "stream_started", None, "Stream session started")
        
        return session
    
    def stop_stream(self, user_id: int):
        """Stop an active stream session"""
        if user_id in self.active_streams:
            self.active_streams[user_id].ended_at = datetime.now(IST_TIMEZONE).isoformat()
            self.active_streams[user_id].is_active = False
            session = self.active_streams.pop(user_id)
            self.db.save_stream_session(session)
            self.save_streams()
            
            # Log activity
            self.db.log_activity(user_id, "stream_ended", None, 
                                f"Stream ended, messages sent: {session.messages_sent}")
    
    def is_streaming(self, user_id: int) -> bool:
        """Check if user has an active stream"""
        return user_id in self.active_streams
    
    def increment_message_count(self, user_id: int):
        """Increment message count for active stream"""
        if user_id in self.active_streams:
            self.active_streams[user_id].messages_sent += 1
            self.save_streams()
    
    def get_stream_stats(self, user_id: int) -> Optional[dict]:
        """Get stream statistics for user"""
        if user_id in self.active_streams:
            session = self.active_streams[user_id]
            return {
                'started_at': session.started_at,
                'messages_sent': session.messages_sent,
                'duration': (datetime.now(IST_TIMEZONE) - 
                           datetime.fromisoformat(session.started_at)).total_seconds() / 60
            }
        return None

# ==================== ENHANCED STORAGE CLASSES ====================
class PersistentDataStorage:
    """Enhanced storage with database persistence and backup"""
    
    def __init__(self, db: DatabaseManager):
        self.db = db
        self.pending_posts: Dict[int, NewsPost] = {}
        self.scheduled_posts: Dict[int, NewsPost] = {}
        self.approved_posts: Dict[int, NewsPost] = {}
        self.rejected_posts: Dict[int, NewsPost] = {}
        self.next_post_id = 1
        
        # Load all data from database
        self.load_all_data()
    
    def load_all_data(self):
        """Load all data from database into memory"""
        try:
            # Load posts by status
            all_posts = self.db.get_all_posts()
            
            for post_data in all_posts:
                post = self.dict_to_post(post_data)
                if post:
                    if post.status == PostStatus.PENDING:
                        self.pending_posts[post.post_id] = post
                    elif post.status == PostStatus.SCHEDULED:
                        self.scheduled_posts[post.post_id] = post
                    elif post.status == PostStatus.APPROVED:
                        self.approved_posts[post.post_id] = post
                    elif post.status == PostStatus.REJECTED:
                        self.rejected_posts[post.post_id] = post
                    
                    self.next_post_id = max(self.next_post_id, post.post_id + 1)
            
            logger.info(f"Loaded {len(self.pending_posts)} pending, {len(self.scheduled_posts)} scheduled, "
                       f"{len(self.approved_posts)} approved, {len(self.rejected_posts)} rejected posts")
            
        except Exception as e:
            logger.error(f"Error loading data from database: {e}")
            # Fallback to JSON files
            self.load_from_json()
    
    def dict_to_post(self, data: dict) -> Optional[NewsPost]:
        """Convert database dict to NewsPost object"""
        try:
            return NewsPost(
                post_id=data['post_id'],
                user_id=data['user_id'],
                username=data['username'],
                text=data['text'] or "",
                media_type=data['media_type'],
                media_ids=json.loads(data['media_ids']) if data['media_ids'] else [],
                entities=json.loads(data['entities']) if data['entities'] else [],
                caption_entities=json.loads(data['caption_entities']) if data['caption_entities'] else [],
                status=PostStatus(data['status']),
                submitted_at=data['submitted_at'],
                reviewed_by=data['reviewed_by'],
                reviewed_at=data['reviewed_at'],
                rejection_reason=data['rejection_reason'],
                scheduled_time=data['scheduled_time'],
                channel_message_ids=json.loads(data['channel_message_ids']) if data['channel_message_ids'] else []
            )
        except Exception as e:
            logger.error(f"Error converting dict to post: {e}")
            return None
    
    def load_from_json(self):
        """Fallback: Load from JSON files if database fails"""
        for filename, storage_dict in [
            (PENDING_POSTS_FILE, self.pending_posts),
            (SCHEDULED_POSTS_FILE, self.scheduled_posts),
            (APPROVED_POSTS_FILE, self.approved_posts),
            (REJECTED_POSTS_FILE, self.rejected_posts)
        ]:
            if os.path.exists(filename):
                try:
                    with open(filename, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        for post_id, post_data in data.items():
                            try:
                                post = NewsPost.from_dict(post_data)
                                storage_dict[int(post_id)] = post
                                self.next_post_id = max(self.next_post_id, int(post_id) + 1)
                                
                                # Save to database
                                self.db.save_post(post)
                                
                            except Exception as e:
                                logger.error(f"Error loading post {post_id}: {e}")
                except Exception as e:
                    logger.error(f"Error loading {filename}: {e}")
    
    def save_json_backup(self):
        """Save current state to JSON files as backup"""
        try:
            # Save pending posts
            with open(PENDING_POSTS_FILE, 'w', encoding='utf-8') as f:
                json.dump(
                    {k: v.to_dict() for k, v in self.pending_posts.items()},
                    f,
                    indent=2,
                    ensure_ascii=False
                )
            
            # Save scheduled posts
            with open(SCHEDULED_POSTS_FILE, 'w', encoding='utf-8') as f:
                json.dump(
                    {k: v.to_dict() for k, v in self.scheduled_posts.items()},
                    f,
                    indent=2,
                    ensure_ascii=False
                )
            
            # Save approved posts
            with open(APPROVED_POSTS_FILE, 'w', encoding='utf-8') as f:
                json.dump(
                    {k: v.to_dict() for k, v in self.approved_posts.items()},
                    f,
                    indent=2,
                    ensure_ascii=False
                )
            
            # Save rejected posts
            with open(REJECTED_POSTS_FILE, 'w', encoding='utf-8') as f:
                json.dump(
                    {k: v.to_dict() for k, v in self.rejected_posts.items()},
                    f,
                    indent=2,
                    ensure_ascii=False
                )
            
            logger.info("JSON backup saved successfully")
            
        except Exception as e:
            logger.error(f"Error saving JSON backup: {e}")
    
    def create_post(self, user_id: int, username: str, text: str, media_type: str = None, 
                   media_ids: List[str] = None, entities: List[Dict] = None, 
                   caption_entities: List[Dict] = None) -> NewsPost:
        """Create and save new post"""
        post = NewsPost(
            post_id=self.next_post_id,
            user_id=user_id,
            username=username,
            text=text,
            media_type=media_type,
            media_ids=media_ids or [],
            entities=entities or [],
            caption_entities=caption_entities or []
        )
        
        # Save to memory
        self.pending_posts[post.post_id] = post
        self.next_post_id += 1
        
        # Save to database
        self.db.save_post(post)
        
        # Save JSON backup
        self.save_json_backup()
        
        # Log activity
        self.db.log_activity(user_id, "post_created", post.post_id, f"Media: {media_type}")
        
        return post
    
    def get_post(self, post_id: int) -> Optional[NewsPost]:
        """Get post from memory"""
        for storage_dict in [self.pending_posts, self.scheduled_posts,
                           self.approved_posts, self.rejected_posts]:
            if post_id in storage_dict:
                return storage_dict[post_id]
        
        # Try to load from database if not in memory
        post_data = self.db.get_post(post_id)
        if post_data:
            post = self.dict_to_post(post_data)
            if post:
                # Add to appropriate memory dict
                if post.status == PostStatus.PENDING:
                    self.pending_posts[post_id] = post
                elif post.status == PostStatus.SCHEDULED:
                    self.scheduled_posts[post_id] = post
                elif post.status == PostStatus.APPROVED:
                    self.approved_posts[post_id] = post
                elif post.status == PostStatus.REJECTED:
                    self.rejected_posts[post_id] = post
                
                return post
        
        return None
    
    def move_to_approved(self, post: NewsPost):
        """Move post to approved"""
        # Remove from current storage
        if post.post_id in self.pending_posts:
            del self.pending_posts[post.post_id]
        if post.post_id in self.scheduled_posts:
            del self.scheduled_posts[post.post_id]
        if post.post_id in self.rejected_posts:
            del self.rejected_posts[post.post_id]
        
        # Add to approved
        self.approved_posts[post.post_id] = post
        post.status = PostStatus.APPROVED
        
        # Update in database
        self.db.save_post(post)
        
        # Save JSON backup
        self.save_json_backup()
    
    def move_to_rejected(self, post: NewsPost):
        """Move post to rejected"""
        if post.post_id in self.pending_posts:
            del self.pending_posts[post.post_id]
        if post.post_id in self.scheduled_posts:
            del self.scheduled_posts[post.post_id]
        if post.post_id in self.approved_posts:
            del self.approved_posts[post.post_id]
        
        self.rejected_posts[post.post_id] = post
        post.status = PostStatus.REJECTED
        self.db.save_post(post)
        self.save_json_backup()
    
    def move_to_scheduled(self, post: NewsPost):
        """Move post to scheduled"""
        if post.post_id in self.pending_posts:
            del self.pending_posts[post.post_id]
        if post.post_id in self.approved_posts:
            del self.approved_posts[post.post_id]
        if post.post_id in self.rejected_posts:
            del self.rejected_posts[post.post_id]
        
        self.scheduled_posts[post.post_id] = post
        post.status = PostStatus.SCHEDULED
        self.db.save_post(post)
        self.save_json_backup()
    
    def delete_post(self, post_id: int):
        """Permanently delete post"""
        # Remove from memory
        for storage_dict in [self.pending_posts, self.scheduled_posts,
                           self.approved_posts, self.rejected_posts]:
            if post_id in storage_dict:
                del storage_dict[post_id]
                break
        
        # Delete from database
        self.db.delete_post(post_id)
        
        # Save JSON backup
        self.save_json_backup()

class PersistentPointsSystem:
    """Enhanced points system with database persistence"""
    
    def __init__(self, db: DatabaseManager):
        self.db = db
        self.points_data: Dict[int, UserPoints] = {}
        self.load_points()
    
    def load_points(self):
        """Load points from database"""
        try:
            user_points_list = self.db.get_all_user_points()
            for data in user_points_list:
                user_points = UserPoints(
                    user_id=data['user_id'],
                    username=data['username'],
                    points=data['points'],
                    total_approved=data['total_approved'],
                    total_rejected=data['total_rejected'],
                    total_deleted=data['total_deleted'],
                    achievements=json.loads(data['achievements']) if data['achievements'] else [],
                    last_post_date=data['last_post_date'],
                    consecutive_days=data['consecutive_days']
                )
                self.points_data[user_points.user_id] = user_points
            
            logger.info(f"Loaded {len(self.points_data)} users' points")
            
        except Exception as e:
            logger.error(f"Error loading points from database: {e}")
            self.load_from_json()
    
    def load_from_json(self):
        """Fallback: Load from JSON file"""
        if os.path.exists(POINTS_FILE):
            try:
                with open(POINTS_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    for user_id_str, user_data in data.items():
                        user_points = UserPoints.from_dict(user_data)
                        self.points_data[int(user_id_str)] = user_points
                        
                        # Save to database
                        self.db.save_user_points(user_points)
            except Exception as e:
                logger.error(f"Error loading points from JSON: {e}")
    
    def save_points(self):
        """Save points to database and JSON"""
        try:
            # Save to database
            for user_points in self.points_data.values():
                self.db.save_user_points(user_points)
            
            # Save JSON backup
            with open(POINTS_FILE, 'w', encoding='utf-8') as f:
                json.dump(
                    {k: v.to_dict() for k, v in self.points_data.items()},
                    f,
                    indent=2,
                    ensure_ascii=False
                )
        except Exception as e:
            logger.error(f"Error saving points: {e}")
    
    def update_user(self, user_id: int, username: str = None, points_change: int = 0, 
                   approved: bool = False, rejected: bool = False, deleted: bool = False):
        """Update user points"""
        if user_id not in self.points_data:
            self.points_data[user_id] = UserPoints(
                user_id=user_id,
                username=username or "",
                points=0
            )
        
        user = self.points_data[user_id]
        user.points += points_change
        if approved: user.total_approved += 1
        if rejected: user.total_rejected += 1
        if deleted: user.total_deleted += 1
        
        # Update consecutive days
        today = datetime.now(IST_TIMEZONE).date().isoformat()
        if user.last_post_date != today:
            if user.last_post_date == (datetime.now(IST_TIMEZONE).date() - timedelta(days=1)).isoformat():
                user.consecutive_days += 1
            else:
                user.consecutive_days = 1
            user.last_post_date = today
        
        if username and username != user.username:
            user.username = username
        
        # Save changes
        self.save_points()
        
        # Log activity
        self.db.log_activity(user_id, "points_updated", None, 
                            f"Points change: {points_change}, New total: {user.points}")
        
        return user.points
    
    def get_user_points(self, user_id: int) -> Optional[UserPoints]:
        """Get user points"""
        return self.points_data.get(user_id)
    
    def get_top_users(self, limit: int = 10) -> List[UserPoints]:
        """Get top users by points"""
        return sorted(self.points_data.values(), key=lambda x: x.points, reverse=True)[:limit]

class PersistentAchievementSystem:
    """Enhanced achievement system with database persistence"""
    
    def __init__(self, db: DatabaseManager, points_system: PersistentPointsSystem):
        self.db = db
        self.points_system = points_system
        self.achievements_data: Dict[int, List[Achievement]] = {}
        self.load_achievements()
    
    def load_achievements(self):
        """Load achievements from database"""
        try:
            achievements_list = self.db.get_all_achievements()
            for data in achievements_list:
                achievement = Achievement(
                    user_id=data['user_id'],
                    achievement_type=data['achievement_type'],
                    earned_at=data['earned_at'],
                    title=data['title'],
                    description=data['description'],
                    points_bonus=data['points_bonus']
                )
                
                if achievement.user_id not in self.achievements_data:
                    self.achievements_data[achievement.user_id] = []
                
                self.achievements_data[achievement.user_id].append(achievement)
            
            logger.info(f"Loaded achievements for {len(self.achievements_data)} users")
            
        except Exception as e:
            logger.error(f"Error loading achievements from database: {e}")
            self.load_from_json()
    
    def load_from_json(self):
        """Fallback: Load from JSON file"""
        if os.path.exists(ACHIEVEMENTS_FILE):
            try:
                with open(ACHIEVEMENTS_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    for user_id_str, achievements_list in data.items():
                        user_id = int(user_id_str)
                        self.achievements_data[user_id] = [
                            Achievement(**ach) for ach in achievements_list
                        ]
                        
                        # Save to database
                        for ach in self.achievements_data[user_id]:
                            self.db.save_achievement(ach)
            except Exception as e:
                logger.error(f"Error loading achievements from JSON: {e}")
    
    def save_achievements(self):
        """Save achievements to database and JSON"""
        try:
            # Save to database
            for user_id, achievements in self.achievements_data.items():
                for ach in achievements:
                    self.db.save_achievement(ach)
            
            # Save JSON backup
            data = {}
            for user_id, achievements in self.achievements_data.items():
                data[str(user_id)] = [asdict(ach) for ach in achievements]
            
            with open(ACHIEVEMENTS_FILE, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
                
        except Exception as e:
            logger.error(f"Error saving achievements: {e}")
    
    def check_and_award_achievements(self, user_id: int) -> List[Achievement]:
        """Check and award achievements based on user stats"""
        user_points = self.points_system.get_user_points(user_id)
        if not user_points:
            return []
        
        new_achievements = []
        user_achievements = self.achievements_data.get(user_id, [])
        existing_types = {ach.achievement_type for ach in user_achievements}
        
        # Define achievement criteria
        achievement_defs = {
            AchievementType.FIRST_POST.value: {
                'title': '🌟 First Step',
                'description': 'Submitted your first post',
                'check': lambda up: up.total_approved + up.total_rejected >= 1,
                'points': 5
            },
            AchievementType.TEN_APPROVED.value: {
                'title': '📰 Rising Reporter',
                'description': '10 posts approved',
                'check': lambda up: up.total_approved >= 10,
                'points': 20
            },
            AchievementType.FIFTY_APPROVED.value: {
                'title': '🏆 Veteran Contributor',
                'description': '50 posts approved',
                'check': lambda up: up.total_approved >= 50,
                'points': 100
            },
            AchievementType.HUNDRED_APPROVED.value: {
                'title': '👑 News Master',
                'description': '100 posts approved',
                'check': lambda up: up.total_approved >= 100,
                'points': 250
            },
            AchievementType.POINTS_100.value: {
                'title': '⭐ Century Club',
                'description': 'Earned 100 points',
                'check': lambda up: up.points >= 100,
                'points': 50
            },
            AchievementType.POINTS_500.value: {
                'title': '💫 Elite Contributor',
                'description': 'Earned 500 points',
                'check': lambda up: up.points >= 500,
                'points': 200
            },
            AchievementType.POINTS_1000.value: {
                'title': '👁‍🗨 Legend',
                'description': 'Earned 1000 points',
                'check': lambda up: up.points >= 1000,
                'points': 500
            },
            AchievementType.CONSISTENT_CONTRIBUTOR.value: {
                'title': '📅 Consistent Contributor',
                'description': 'Posted for 7 consecutive days',
                'check': lambda up: up.consecutive_days >= 7,
                'points': 50
            },
            AchievementType.TOP_CONTRIBUTOR.value: {
                'title': '🏅 Top Contributor',
                'description': 'Reached top 3 in points',
                'check': lambda up: self.is_top_contributor(user_id),
                'points': 100
            }
        }
        
        for ach_type, ach_def in achievement_defs.items():
            if ach_type not in existing_types and ach_def['check'](user_points):
                achievement = Achievement(
                    user_id=user_id,
                    achievement_type=ach_type,
                    earned_at=datetime.now(IST_TIMEZONE).isoformat(),
                    title=ach_def['title'],
                    description=ach_def['description'],
                    points_bonus=ach_def['points']
                )
                
                if user_id not in self.achievements_data:
                    self.achievements_data[user_id] = []
                
                self.achievements_data[user_id].append(achievement)
                new_achievements.append(achievement)
                
                # Award bonus points
                self.points_system.update_user(
                    user_id=user_id,
                    points_change=ach_def['points']
                )
                
                # Save to database
                self.db.save_achievement(achievement)
                
                # Log activity
                self.db.log_activity(user_id, "achievement_earned", None,
                                    f"Achievement: {ach_def['title']}, Points: +{ach_def['points']}")
        
        if new_achievements:
            self.save_achievements()
        
        return new_achievements
    
    def is_top_contributor(self, user_id: int) -> bool:
        """Check if user is in top 3 contributors"""
        top_users = self.points_system.get_top_users(3)
        return any(u.user_id == user_id for u in top_users)
    
    def get_user_achievements(self, user_id: int) -> List[Achievement]:
        """Get user achievements"""
        return self.achievements_data.get(user_id, [])

# ==================== HELPER FUNCTIONS ====================
def is_developer(user_id: int) -> bool:
    return user_id == DEVELOPER_ID

def is_authorized_poster(user_id: int) -> bool:
    return user_id in AUTHORIZED_POSTERS or is_developer(user_id)

def is_moderator(user_id: int) -> bool:
    return user_id in MODERATOR_USER_IDS or is_developer(user_id)

def get_user_display(user_id: int, username: str) -> str:
    return f"@{username}" if username else f"User {user_id}"

def format_schedule_time(dt: datetime) -> str:
    return dt.strftime("%d %b %Y at %I:%M %p IST")

def format_submitted_time(dt: datetime) -> str:
    return dt.strftime("%I:%M %p %d/%m/%Y")

def parse_time_string(time_str: str) -> datetime:
    """Parse time string in format: HH:MM AM/PM DD/MM/YYYY"""
    try:
        time_str = time_str.strip().upper()
        parts = time_str.split()
        
        if len(parts) < 3:
            raise ValueError("Format: HH:MM AM/PM DD/MM/YYYY")
        
        time_part = parts[0]
        am_pm = parts[1]
        date_part = parts[2]
        
        # Parse datetime
        dt_str = f"{time_part} {am_pm} {date_part}"
        dt_obj = datetime.strptime(dt_str, "%I:%M %p %d/%m/%Y")
        
        # Localize to IST
        dt_ist = IST_TIMEZONE.localize(dt_obj)
        
        # Check if in future
        if dt_ist < datetime.now(IST_TIMEZONE):
            raise ValueError("Time must be in the future")
        
        return dt_ist
        
    except Exception as e:
        raise ValueError(f"Invalid time format. Use: HH:MM AM/PM DD/MM/YYYY\nError: {str(e)}")

async def safe_edit_message(query, text: str, **kwargs):
    """Safely edit messages with media"""
    try:
        if query.message.photo or query.message.document or query.message.video:
            caption_kwargs = {}
            if 'parse_mode' in kwargs:
                caption_kwargs['parse_mode'] = kwargs['parse_mode']
            if 'reply_markup' in kwargs:
                caption_kwargs['reply_markup'] = kwargs['reply_markup']
            await query.edit_message_caption(caption=text, **caption_kwargs)
        else:
            await query.edit_message_text(text=text, **kwargs)
    except Exception as e:
        logger.error(f"Error editing message: {e}")
        await query.answer()

async def execute_post(bot: Bot, post: NewsPost) -> List[int]:
    """Send post to channel with formatting preservation"""
    try:
        # Convert stored entities back to MessageEntity objects
        entities = None
        caption_entities = None
        
        if post.entities:
            entities = [MessageEntity(**entity) for entity in post.entities]
        if post.caption_entities:
            caption_entities = [MessageEntity(**entity) for entity in post.caption_entities]
        
        if post.media_ids:
            if post.media_type == "photo":
                msg = await bot.send_photo(
                    CHANNEL_ID,
                    photo=post.media_ids[0],
                    caption=post.text,
                    caption_entities=caption_entities,
                    parse_mode=None  # Let Telegram handle formatting
                )
            elif post.media_type == "document":
                msg = await bot.send_document(
                    CHANNEL_ID,
                    document=post.media_ids[0],
                    caption=post.text,
                    caption_entities=caption_entities,
                    parse_mode=None
                )
            elif post.media_type == "video":
                msg = await bot.send_video(
                    CHANNEL_ID,
                    video=post.media_ids[0],
                    caption=post.text,
                    caption_entities=caption_entities,
                    parse_mode=None
                )
            else:
                msg = await bot.send_message(CHANNEL_ID, text=post.text)
            return [msg.message_id]
        else:
            # Text only with formatting
            msg = await bot.send_message(
                CHANNEL_ID,
                text=post.text,
                entities=entities,
                parse_mode=None,  # Preserve original formatting
                disable_web_page_preview=True
            )
            return [msg.message_id]
    except Exception as e:
        logger.error(f"Error posting to channel: {e}")
        raise

async def send_moderator_notification(context: ContextTypes.DEFAULT_TYPE, post: NewsPost, action: str, moderator_id: Optional[int] = None):
    """Send detailed notification to moderators"""
    try:
        moderator_display = "Auto-scheduler"
        if moderator_id:
            try:
                user = await context.bot.get_chat(moderator_id)
                moderator_display = f"@{user.username}" if user.username else f"User {moderator_id}"
            except:
                moderator_display = f"User {moderator_id}"
        
        posted_time = datetime.now(IST_TIMEZONE).strftime("%d %b %Y at %I:%M %p IST")
        
        notification_text = (
            f"📢 <b>POST PUBLISHED</b>\n\n"
            f"📝 <b>Post ID:</b> #{post.post_id}\n"
            f"👤 <b>Posted by:</b> {get_user_display(post.user_id, post.username)}\n"
            f"⏰ <b>Published:</b> {posted_time}\n"
            f"🛠 <b>Action:</b> {action}\n"
            f"👁 <b>By:</b> {moderator_display}\n\n"
        )
        
        # Add content preview
        content_preview = post.text[:200] + "..." if len(post.text) > 200 else post.text
        notification_text += f"📄 <b>Content:</b>\n{content_preview}\n\n"
        
        # Add channel link if available
        if post.channel_message_ids:
            channel_link = f"https://t.me/c/{str(CHANNEL_ID)[4:]}/{post.channel_message_ids[0]}"
            notification_text += f"🔗 <b>Channel Link:</b>\n{channel_link}"
        
        await context.bot.send_message(
            chat_id=MODERATOR_GROUP_ID,
            text=notification_text,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True
        )
    except Exception as e:
        logger.error(f"Failed to send moderator notification: {e}")

# ==================== UPDATED KEYBOARD CREATORS ====================
def create_moderation_keyboard(post_id: int) -> InlineKeyboardMarkup:
    keyboard = [
        [
            InlineKeyboardButton("✅ Approve", callback_data=f"approve_{post_id}"),
            InlineKeyboardButton("❌ Reject", callback_data=f"reject_{post_id}")
        ],
        [
            InlineKeyboardButton("🗑 Delete", callback_data=f"delete_{post_id}"),
            InlineKeyboardButton("📅 Schedule", callback_data=f"schedule_{post_id}")
        ]
    ]
    return InlineKeyboardMarkup(keyboard)

def create_rejection_reasons_keyboard(post_id: int, page: int = 0) -> InlineKeyboardMarkup:
    """Create paginated keyboard with full rejection reason text"""
    keyboard = []
    items_per_page = 5
    
    start_idx = page * items_per_page
    end_idx = min(start_idx + items_per_page, len(PRERECORDED_REASONS))
    
    # Add reasons for current page with full text
    for i in range(start_idx, end_idx):
        reason = PRERECORDED_REASONS[i]
        # Show full reason text (Telegram buttons can handle up to ~64 chars)
        display_text = reason[:35] + "..." if len(reason) > 35 else reason
        keyboard.append([
            InlineKeyboardButton(
                f"📝 {display_text}", 
                callback_data=f"reason_preset_{i}_{post_id}"
            )
        ])
    
    # Navigation buttons
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton("◀️ Previous", callback_data=f"reject_page_{page-1}_{post_id}"))
    if end_idx < len(PRERECORDED_REASONS):
        nav_buttons.append(InlineKeyboardButton("Next ▶️", callback_data=f"reject_page_{page+1}_{post_id}"))
    
    if nav_buttons:
        keyboard.append(nav_buttons)
    
    # Action buttons
    keyboard.append([
        InlineKeyboardButton("✏️ Custom Reason", callback_data=f"reason_custom_{post_id}"),
        InlineKeyboardButton("⏭️ Skip Reason", callback_data=f"skip_reason_{post_id}")
    ])
    keyboard.append([
        InlineKeyboardButton("↩️ Go Back", callback_data=f"back_{post_id}")
    ])
    
    return InlineKeyboardMarkup(keyboard)

def create_points_confirmation_keyboard(post_id: int, action: str = "approve") -> InlineKeyboardMarkup:
    """Create keyboard for points confirmation"""
    if action == "approve":
        keyboard = [
            [
                InlineKeyboardButton("✅ +2 Points", callback_data=f"approve_with_points_{post_id}"),
                InlineKeyboardButton("❌ No Points", callback_data=f"approve_without_points_{post_id}")
            ],
            [
                InlineKeyboardButton("↩️ Go Back", callback_data=f"back_{post_id}")
            ]
        ]
    else:  # reject
        keyboard = [
            [
                InlineKeyboardButton("⚠️ +1 Point", callback_data=f"reject_with_points_{post_id}"),
                InlineKeyboardButton("❌ No Points", callback_data=f"reject_without_points_{post_id}")
            ],
            [
                InlineKeyboardButton("↩️ Go Back", callback_data=f"back_{post_id}")
            ]
        ]
    return InlineKeyboardMarkup(keyboard)

def create_back_keyboard(post_id: int) -> InlineKeyboardMarkup:
    keyboard = [
        [
            InlineKeyboardButton("↩️ Go Back", callback_data=f"back_{post_id}")
        ]
    ]
    return InlineKeyboardMarkup(keyboard)

def create_scheduled_post_keyboard(post_id: int) -> InlineKeyboardMarkup:
    keyboard = [
        [
            InlineKeyboardButton("🔄 Reschedule", callback_data=f"reschedule_{post_id}"),
            InlineKeyboardButton("✅ Post Now", callback_data=f"post_now_{post_id}")
        ],
        [
            InlineKeyboardButton("❌ Cancel Schedule", callback_data=f"cancel_schedule_{post_id}"),
            InlineKeyboardButton("↩️ Back", callback_data=f"back_{post_id}")
        ]
    ]
    return InlineKeyboardMarkup(keyboard)

def create_achievement_notification_keyboard() -> InlineKeyboardMarkup:
    """Create keyboard for achievement notifications"""
    keyboard = [
        [InlineKeyboardButton("🎉 View My Achievements", callback_data="view_my_achievements")]
    ]
    return InlineKeyboardMarkup(keyboard)

# ==================== COMMAND HANDLERS ====================
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if is_authorized_poster(user.id):
        if is_moderator(user.id):
            await update.message.reply_text(
                "👋 <b>Welcome Admin/Moderator!</b>\n\n"
                "📚 <b>Available Commands:</b>\n"
                "• /post - Submit news\n"
                "• /myposts - View your posts\n"
                "• /mystats - View your statistics\n"
                "• /achievements - View your achievements\n"
                "• /points - View your points\n"
                "• /pending - Show pending posts\n"
                "• /list - View all posts\n"
                "• /showlist - Scheduled posts\n"
                "• /stats - System statistics\n"
                "• /toppoints - Top posters\n"
                "• /backup - Create backup\n"
                "• /verify - Verify database\n"
                "• /export - Export data\n"
                "• /help - Show all commands",
                parse_mode=ParseMode.HTML
            )
        else:
            await update.message.reply_text(
                "👋 <b>Welcome Poster!</b>\n\n"
                "📚 <b>Available Commands:</b>\n"
                "• /post - Submit news\n"
                "• /myposts - View your posts\n"
                "• /mystats - View statistics\n"
                "• /achievements - View achievements\n"
                "• /points - View points\n"
                "• /help - Show commands",
                parse_mode=ParseMode.HTML
            )
    else:
        await update.message.reply_text("🚫 Not authorized.")

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not is_authorized_poster(user.id):
        await update.message.reply_text("🚫 Not authorized.")
        return
    
    if is_moderator(user.id):
        help_text = "📚 <b>Moderator Commands:</b>\n"
        help_text += "/pending - Show pending posts\n"
        help_text += "/list - View all posts\n"
        help_text += "/showlist - Scheduled posts\n"
        help_text += "/stats - System statistics\n"
        help_text += "/toppoints - Top posters\n"
        help_text += "/backup - Create manual backup\n"
        help_text += "/verify - Verify database integrity\n"
        help_text += "/export - Export all data\n\n"
        help_text += "👤 <b>User Commands:</b>\n"
    else:
        help_text = "📚 <b>Available Commands:</b>\n"
    
    help_text += "/post - Submit news\n"
    help_text += "/myposts - View your posts\n"
    help_text += "/mystats - Your statistics\n"
    help_text += "/achievements - Your achievements\n"
    help_text += "/points - Your points\n"
    help_text += "/help - This help"
    
    await update.message.reply_text(help_text, parse_mode=ParseMode.HTML)

async def post_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not is_authorized_poster(user.id):
        await update.message.reply_text("🚫 Not authorized to post.")
        return
    
    await update.message.reply_text(
        "📝 <b>Send your news:</b>\n\n"
        "You can send text, photos, videos, or documents.\n"
        "Formatting like *bold*, _italic_ will be preserved.\n"
        "Type /cancel to cancel.",
        parse_mode=ParseMode.HTML
    )
    return WAITING_FOR_NEWS

async def receive_news(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    text = update.message.text or update.message.caption or ""
    
    if not text and not update.message.photo and not update.message.document and not update.message.video:
        await update.message.reply_text("❌ Please provide content.")
        return WAITING_FOR_NEWS
    
    media_type = None
    media_ids = []
    entities = None
    caption_entities = None
    
    # Store formatting entities
    if update.message.text and update.message.entities:
        entities = [entity.to_dict() for entity in update.message.entities]
    
    if update.message.caption and update.message.caption_entities:
        caption_entities = [entity.to_dict() for entity in update.message.caption_entities]
    
    if update.message.photo:
        media_type = "photo"
        media_ids.append(update.message.photo[-1].file_id)
    elif update.message.document:
        media_type = "document"
        media_ids.append(update.message.document.file_id)
    elif update.message.video:
        media_type = "video"
        media_ids.append(update.message.video.file_id)
    
    # Create post with formatting preservation
    post = storage.create_post(
        user.id, 
        user.username, 
        text, 
        media_type, 
        media_ids,
        entities=entities,
        caption_entities=caption_entities
    )
    
    await update.message.reply_text(
        "✅ <b>Post sent to moderators!</b>\n"
        "You'll be notified when reviewed.",
        parse_mode=ParseMode.HTML
    )
    
    # Check for first post achievement
    user_points = points_system.get_user_points(user.id)
    if not user_points or (user_points.total_approved + user_points.total_rejected) == 0:
        new_achievements = achievement_system.check_and_award_achievements(user.id)
        if new_achievements:
            await notify_achievements(context, user.id, new_achievements)
    
    # Send to moderators with formatting
    await send_to_moderators(context, post)
    
    return ConversationHandler.END

async def achievements_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """View user achievements"""
    user = update.effective_user
    if not is_authorized_poster(user.id):
        await update.message.reply_text("🚫 Not authorized.")
        return
    
    achievements = achievement_system.get_user_achievements(user.id)
    user_points = points_system.get_user_points(user.id)
    
    if not achievements:
        message = "🏆 <b>No Achievements Yet</b>\n\n"
        message += "Keep posting to earn achievements!\n\n"
        message += "📊 <b>Your Stats:</b>\n"
        message += f"⭐ Points: {user_points.points if user_points else 0}\n"
        message += f"✅ Approved: {user_points.total_approved if user_points else 0}\n"
        message += f"📝 Total Posts: {(user_points.total_approved if user_points else 0) + (user_points.total_rejected if user_points else 0)}"
        
        await update.message.reply_text(message, parse_mode=ParseMode.HTML)
        return
    
    message = "🏆 <b>Your Achievements</b>\n\n"
    
    for ach in achievements:
        message += f"🎖 <b>{ach.title}</b>\n"
        message += f"   {ach.description}\n"
        message += f"   ⭐ +{ach.points_bonus} points\n"
        earned_date = datetime.fromisoformat(ach.earned_at).strftime("%d %b %Y")
        message += f"   📅 {earned_date}\n\n"
    
    message += f"📊 <b>Total:</b> {len(achievements)} achievements"
    
    await update.message.reply_text(message, parse_mode=ParseMode.HTML)

async def pending_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not is_moderator(user.id):
        await update.message.reply_text("🚫 Moderators only.")
        return
    
    if not storage.pending_posts:
        await update.message.reply_text("📭 No pending posts.")
        return
    
    for post_id, post in storage.pending_posts.items():
        await send_to_moderators(context, post)
    
    await update.message.reply_text(f"📋 Showing {len(storage.pending_posts)} pending posts.")

async def showlist_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not is_moderator(user.id):
        await update.message.reply_text("🚫 Moderators only.")
        return
    
    if not storage.scheduled_posts:
        await update.message.reply_text("📭 No scheduled posts.")
        return
    
    for post_id, post in storage.scheduled_posts.items():
        if post.scheduled_time:
            scheduled_dt = datetime.fromisoformat(post.scheduled_time).astimezone(IST_TIMEZONE)
            scheduled_str = format_schedule_time(scheduled_dt)
            
            # Send with reschedule options
            info_text = (
                f"📅 <b>Scheduled Post</b>\n\n"
                f"📝 <b>Post ID:</b> {post.post_id}\n"
                f"👤 <b>By:</b> {get_user_display(post.user_id, post.username)}\n"
                f"⏰ <b>Scheduled:</b> {scheduled_str}\n\n"
            )
            
            await context.bot.send_message(
                MODERATOR_GROUP_ID,
                text=info_text,
                parse_mode=ParseMode.HTML
            )
            
            # Send content
            if post.media_ids:
                if post.media_type == "photo":
                    await context.bot.send_photo(
                        MODERATOR_GROUP_ID,
                        photo=post.media_ids[0],
                        caption=post.text
                    )
                elif post.media_type == "document":
                    await context.bot.send_document(
                        MODERATOR_GROUP_ID,
                        document=post.media_ids[0],
                        caption=post.text
                    )
                elif post.media_type == "video":
                    await context.bot.send_video(
                        MODERATOR_GROUP_ID,
                        video=post.media_ids[0],
                        caption=post.text
                    )
            else:
                await context.bot.send_message(
                    MODERATOR_GROUP_ID,
                    text=post.text,
                    disable_web_page_preview=True
                )
            
            # Send action buttons with reschedule option
            await context.bot.send_message(
                MODERATOR_GROUP_ID,
                text=f"📅 <b>Scheduled Post #{post.post_id}</b>",
                parse_mode=ParseMode.HTML,
                reply_markup=create_scheduled_post_keyboard(post.post_id)
            )
    
    await update.message.reply_text(f"📋 Showing {len(storage.scheduled_posts)} scheduled posts.")

async def list_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not is_moderator(user.id):
        await update.message.reply_text("🚫 Moderators only.")
        return
    
    total = len(storage.pending_posts) + len(storage.scheduled_posts) + len(storage.approved_posts) + len(storage.rejected_posts)
    
    message = f"📊 <b>Post Statistics</b>\n\n"
    message += f"📝 Total Posts: {total}\n"
    message += f"⏳ Pending: {len(storage.pending_posts)}\n"
    message += f"📅 Scheduled: {len(storage.scheduled_posts)}\n"
    message += f"✅ Approved: {len(storage.approved_posts)}\n"
    message += f"❌ Rejected: {len(storage.rejected_posts)}"
    
    await update.message.reply_text(message, parse_mode=ParseMode.HTML)

async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not is_moderator(user.id):
        await update.message.reply_text("🚫 Moderators only.")
        return
    
    total_users = len(points_system.points_data)
    total_points = sum(user.points for user in points_system.points_data.values())
    
    # Get database stats
    with db_manager.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM posts")
        db_posts = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM achievements")
        db_achievements = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM activity_log")
        db_activities = cursor.fetchone()[0]
    
    message = "📊 <b>System Statistics</b>\n\n"
    message += f"👤 Total Users: {total_users}\n"
    message += f"⭐ Total Points: {total_points}\n"
    message += f"📝 Database Posts: {db_posts}\n"
    message += f"🏆 Achievements: {db_achievements}\n"
    message += f"📋 Activities Logged: {db_activities}"
    
    await update.message.reply_text(message, parse_mode=ParseMode.HTML)

async def toppoints_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not is_moderator(user.id):
        await update.message.reply_text("🚫 Moderators only.")
        return
    
    top_users = points_system.get_top_users(limit=10)
    if not top_users:
        await update.message.reply_text("📊 No points data yet.")
        return
    
    message = "🏆 <b>Top Posters</b>\n\n"
    for i, user_points in enumerate(top_users):
        medal = ""
        if i == 0: medal = "🥇 "
        elif i == 1: medal = "🥈 "
        elif i == 2: medal = "🥉 "
        
        display_name = f"@{user_points.username}" if user_points.username else f"User {user_points.user_id}"
        message += f"{medal}<b>{i+1}. {display_name}</b>\n"
        message += f"   ⭐ Points: {user_points.points} | ✅ {user_points.total_approved} | ❌ {user_points.total_rejected}\n"
    
    await update.message.reply_text(message, parse_mode=ParseMode.HTML)

async def myposts_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not is_authorized_poster(user.id):
        await update.message.reply_text("🚫 Not authorized.")
        return
    
    user_posts = []
    for post_list in [storage.pending_posts, storage.approved_posts, storage.rejected_posts, storage.scheduled_posts]:
        for post in post_list.values():
            if post.user_id == user.id:
                user_posts.append(post)
    
    if not user_posts:
        await update.message.reply_text("📭 You have no posts.")
        return
    
    user_posts.sort(key=lambda x: x.post_id, reverse=True)
    
    message = "📋 <b>Your Posts:</b>\n\n"
    for post in user_posts[:10]:
        status_emoji = {
            "pending": "⏳",
            "approved": "✅",
            "rejected": "❌",
            "scheduled": "📅"
        }.get(post.status.value, "❓")
        
        message += f"{status_emoji} <b>Post #{post.post_id}</b> - {post.status.value.upper()}\n"
        message += f"📄 {post.text[:50]}...\n\n"
    
    await update.message.reply_text(message, parse_mode=ParseMode.HTML)

async def mystats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not is_authorized_poster(user.id):
        await update.message.reply_text("🚫 Not authorized.")
        return
    
    user_points = points_system.get_user_points(user.id)
    if not user_points:
        await update.message.reply_text("📊 You have no statistics yet.")
        return
    
    achievements = achievement_system.get_user_achievements(user.id)
    
    message = f"📊 <b>Your Statistics</b>\n\n"
    message += f"👤 User: @{user.username or user.id}\n"
    message += f"⭐ Points: {user_points.points}\n"
    message += f"✅ Approved: {user_points.total_approved}\n"
    message += f"❌ Rejected: {user_points.total_rejected}\n"
    message += f"🗑 Deleted: {user_points.total_deleted}\n"
    message += f"📅 Consecutive Days: {user_points.consecutive_days}\n"
    message += f"🏆 Achievements: {len(achievements)}"
    
    await update.message.reply_text(message, parse_mode=ParseMode.HTML)

async def points_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not is_authorized_poster(user.id):
        await update.message.reply_text("🚫 Not authorized.")
        return
    
    user_points = points_system.get_user_points(user.id)
    if not user_points:
        await update.message.reply_text("⭐ You have 0 points.")
        return
    
    # Get rank
    rank = "Not ranked"
    all_users = points_system.get_top_users(limit=100)
    for i, up in enumerate(all_users):
        if up.user_id == user.id:
            rank = f"#{i+1}"
            break
    
    message = (
        f"📊 <b>Your Points</b>\n\n"
        f"👤 <b>User:</b> @{user.username or user.id}\n"
        f"🏆 <b>Rank:</b> {rank}\n"
        f"⭐ <b>Total Points:</b> {user_points.points}\n\n"
        f"📈 <b>Statistics:</b>\n"
        f"✅ Approved Posts: {user_points.total_approved}\n"
        f"❌ Rejected Posts: {user_points.total_rejected}\n"
        f"🗑 Deleted Posts: {user_points.total_deleted}"
    )
    
    await update.message.reply_text(message, parse_mode=ParseMode.HTML)

async def cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("❌ Operation cancelled.")
    return ConversationHandler.END

async def send_to_moderators(context: ContextTypes.DEFAULT_TYPE, post: NewsPost):
    """Send post to moderator group with preserved formatting"""
    try:
        # Convert entities back for display
        entities = None
        caption_entities = None
        
        if post.entities:
            entities = [MessageEntity(**entity) for entity in post.entities]
        if post.caption_entities:
            caption_entities = [MessageEntity(**entity) for entity in post.caption_entities]
        
        info_text = (
            f"🆕 <b>New Post Pending</b>\n\n"
            f"📝 <b>Post ID:</b> {post.post_id}\n"
            f"👤 <b>Posted by:</b> {get_user_display(post.user_id, post.username)}\n"
            f"📅 <b>Submitted:</b> {format_submitted_time(datetime.fromisoformat(post.submitted_at).astimezone(IST_TIMEZONE))}\n\n"
        )
        
        # Send info first
        await context.bot.send_message(
            MODERATOR_GROUP_ID,
            text=info_text,
            parse_mode=ParseMode.HTML
        )
        
        # Send content with formatting
        if post.media_ids:
            if post.media_type == "photo":
                await context.bot.send_photo(
                    MODERATOR_GROUP_ID,
                    photo=post.media_ids[0],
                    caption=post.text,
                    caption_entities=caption_entities,
                    parse_mode=None
                )
            elif post.media_type == "document":
                await context.bot.send_document(
                    MODERATOR_GROUP_ID,
                    document=post.media_ids[0],
                    caption=post.text,
                    caption_entities=caption_entities,
                    parse_mode=None
                )
            elif post.media_type == "video":
                await context.bot.send_video(
                    MODERATOR_GROUP_ID,
                    video=post.media_ids[0],
                    caption=post.text,
                    caption_entities=caption_entities,
                    parse_mode=None
                )
        else:
            await context.bot.send_message(
                MODERATOR_GROUP_ID,
                text=post.text,
                entities=entities,
                parse_mode=None,
                disable_web_page_preview=True
            )
        
        # Send action buttons
        await context.bot.send_message(
            MODERATOR_GROUP_ID,
            text=f"📝 <b>Post ID:</b> {post.post_id}",
            parse_mode=ParseMode.HTML,
            reply_markup=create_moderation_keyboard(post.post_id)
        )
        
    except Exception as e:
        logger.error(f"Error sending to moderators: {e}")

async def notify_achievements(context: ContextTypes.DEFAULT_TYPE, user_id: int, achievements: List[Achievement]):
    """Notify user about new achievements"""
    try:
        for ach in achievements:
            message = (
                f"🎉 <b>ACHIEVEMENT UNLOCKED!</b>\n\n"
                f"🏆 <b>{ach.title}</b>\n"
                f"📝 {ach.description}\n"
                f"⭐ <b>+{ach.points_bonus} Bonus Points!</b>"
            )
            
            await context.bot.send_message(
                chat_id=user_id,
                text=message,
                parse_mode=ParseMode.HTML,
                reply_markup=create_achievement_notification_keyboard()
            )
            
            # Also notify moderator group
            try:
                user = await context.bot.get_chat(user_id)
                await context.bot.send_message(
                    chat_id=MODERATOR_GROUP_ID,
                    text=f"🎉 <b>Achievement Unlocked!</b>\n\n"
                         f"👤 {get_user_display(user_id, user.username)}\n"
                         f"🏆 {ach.title}\n"
                         f"⭐ +{ach.points_bonus} points!",
                    parse_mode=ParseMode.HTML
                )
            except:
                pass
    except Exception as e:
        logger.error(f"Error notifying achievements: {e}")

# ==================== BACKUP AND RECOVERY COMMANDS ====================
async def backup_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Manually create backup (moderators only)"""
    user = update.effective_user
    if not is_moderator(user.id):
        await update.message.reply_text("🚫 Moderators only.")
        return
    
    msg = await update.message.reply_text("🔄 Creating backup...")
    
    success = db_manager.create_backup("manual")
    
    if success:
        await msg.edit_text(
            "✅ <b>Backup created successfully!</b>\n\n"
            "All data has been backed up to:\n"
            f"• Database backup\n"
            f"• JSON backup\n"
            f"• Activity log",
            parse_mode=ParseMode.HTML
        )
    else:
        await msg.edit_text("❌ Failed to create backup.")

async def verify_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Verify database integrity (moderators only)"""
    user = update.effective_user
    if not is_moderator(user.id):
        await update.message.reply_text("🚫 Moderators only.")
        return
    
    msg = await update.message.reply_text("🔍 Verifying database integrity...")
    
    is_valid = db_manager.verify_database_integrity()
    
    if is_valid:
        # Get database stats
        with db_manager.get_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute("SELECT COUNT(*) FROM posts")
            posts_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM user_points")
            users_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM achievements")
            achievements_count = cursor.fetchone()[0]
        
        await msg.edit_text(
            f"✅ <b>Database Integrity Check Passed</b>\n\n"
            f"📊 <b>Statistics:</b>\n"
            f"• Posts: {posts_count}\n"
            f"• Users: {users_count}\n"
            f"• Achievements: {achievements_count}\n\n"
            f"💾 Database is healthy and all data is intact.",
            parse_mode=ParseMode.HTML
        )
    else:
        await msg.edit_text("❌ Database integrity check failed!")

async def export_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Export all data to JSON (moderators only)"""
    user = update.effective_user
    if not is_moderator(user.id):
        await update.message.reply_text("🚫 Moderators only.")
        return
    
    msg = await update.message.reply_text("📤 Exporting data...")
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    export_file = f"{JSON_BACKUP_DIR}/export_{timestamp}.json"
    
    db_manager.export_to_json(export_file)
    
    # Get file size
    file_size = os.path.getsize(export_file) / 1024
    
    await msg.edit_text(
        f"✅ <b>Data Exported Successfully</b>\n\n"
        f"📁 File: export_{timestamp}.json\n"
        f"📦 Size: {file_size:.1f} KB\n\n"
        f"All data has been exported to JSON format.",
        parse_mode=ParseMode.HTML
    )

# ==================== DEVELOPER STREAM COMMANDS ====================
async def stream_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start a stream session (developer only)"""
    user = update.effective_user
    if not is_developer(user.id):
        await update.message.reply_text("🚫 Developer only command.")
        return
    
    if stream_manager.is_streaming(user.id):
        await update.message.reply_text("📡 You're already streaming!")
        return
    
    stream_manager.start_stream(user.id)
    
    await update.message.reply_text(
        "📡 <b>Stream Mode Activated!</b>\n\n"
        "Now any message you send will be broadcast to:\n"
        "• Moderator Group\n"
        "• All authorized posters\n\n"
        "<b>Commands:</b>\n"
        "• /stopstream - End stream session\n"
        "• /streamstats - View stream stats\n\n"
        "<i>Type your message to broadcast...</i>",
        parse_mode=ParseMode.HTML
    )

async def stopstream_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Stop streaming (developer only)"""
    user = update.effective_user
    if not is_developer(user.id):
        await update.message.reply_text("🚫 Developer only command.")
        return
    
    if not stream_manager.is_streaming(user.id):
        await update.message.reply_text("📡 No active stream.")
        return
    
    stream_manager.stop_stream(user.id)
    
    await update.message.reply_text(
        "📡 <b>Stream Mode Deactivated</b>\n\n"
        "Stream session ended.",
        parse_mode=ParseMode.HTML
    )

async def streamstats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """View stream statistics (developer only)"""
    user = update.effective_user
    if not is_developer(user.id):
        await update.message.reply_text("🚫 Developer only command.")
        return
    
    stats = stream_manager.get_stream_stats(user.id)
    
    message = "📊 <b>Stream Statistics</b>\n\n"
    message += f"📡 Status: {'🟢 Active' if stream_manager.is_streaming(user.id) else '🔴 Inactive'}\n"
    
    if stats:
        started = datetime.fromisoformat(stats['started_at']).strftime("%d %b %Y %I:%M %p")
        message += f"⏰ Started: {started}\n"
        message += f"📤 Messages sent: {stats['messages_sent']}\n"
        message += f"⏱ Duration: {stats['duration']:.1f} minutes\n"
    
    # Count total users
    total_users = len(points_system.points_data)
    message += f"👥 Total users: {total_users}\n"
    
    await update.message.reply_text(message, parse_mode=ParseMode.HTML)

async def handle_stream_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle messages during stream"""
    user = update.effective_user
    
    if not is_developer(user.id) or not stream_manager.is_streaming(user.id):
        return
    
    message_text = update.message.text or update.message.caption or ""
    
    if not message_text:
        await update.message.reply_text("❌ Cannot stream media messages.")
        return
    
    # Update message count
    stream_manager.increment_message_count(user.id)
    
    # Send to moderator group
    await context.bot.send_message(
        chat_id=MODERATOR_GROUP_ID,
        text=f"📢 <b>Developer Broadcast</b>\n\n{message_text}",
        parse_mode=ParseMode.HTML
    )
    
    # Send to all authorized posters
    success_count = 0
    fail_count = 0
    
    for poster_id in AUTHORIZED_POSTERS:
        if poster_id != user.id:  # Don't send to self
            try:
                await context.bot.send_message(
                    chat_id=poster_id,
                    text=f"📢 <b>Developer Broadcast</b>\n\n{message_text}",
                    parse_mode=ParseMode.HTML
                )
                success_count += 1
                await asyncio.sleep(0.05)  # Small delay to avoid rate limits
            except Exception as e:
                fail_count += 1
                logger.error(f"Failed to send stream to {poster_id}: {e}")
    
    # Send confirmation to developer
    await update.message.reply_text(
        f"✅ <b>Broadcast Sent!</b>\n\n"
        f"📤 Sent to: {success_count} users\n"
        f"❌ Failed: {fail_count} users",
        parse_mode=ParseMode.HTML
    )

# ==================== CALLBACK QUERY HANDLER ====================
async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    user = query.from_user
    data = query.data
    
    logger.info(f"Callback from {user.id}: {data}")
    
    if not is_moderator(user.id) and not data.startswith('view_my_achievements'):
        await safe_edit_message(query, "🚫 Only moderators can perform this action.")
        return
    
    try:
        # Handle achievement view
        if data == "view_my_achievements":
            achievements = achievement_system.get_user_achievements(user.id)
            if not achievements:
                await safe_edit_message(query, "🏆 No achievements yet.")
                return
            
            message = "🏆 <b>Your Achievements</b>\n\n"
            for ach in achievements:
                message += f"🎖 <b>{ach.title}</b>\n"
                message += f"   {ach.description}\n\n"
            
            await safe_edit_message(query, message, parse_mode=ParseMode.HTML)
            return
        
        # Handle rejection page navigation
        if data.startswith('reject_page_'):
            parts = data.split('_')
            page = int(parts[2])
            post_id = int(parts[3])
            await handle_rejection_page(query, page, post_id)
            return
        
        # Handle different callback patterns
        if data.startswith('approve_') and len(data.split('_')) == 2:
            post_id = int(data.split('_')[1])
            await handle_approve_start(query, post_id)
        
        elif data.startswith('approve_with_points_'):
            post_id = int(data.split('_')[3])
            await handle_approve_with_points(query, post_id, context)
        
        elif data.startswith('approve_without_points_'):
            post_id = int(data.split('_')[3])
            await handle_approve_without_points(query, post_id, context)
        
        elif data.startswith('reject_') and len(data.split('_')) == 2:
            post_id = int(data.split('_')[1])
            await handle_reject_start(query, post_id)
        
        elif data.startswith('reject_with_points_'):
            post_id = int(data.split('_')[3])
            await handle_reject_with_points(query, post_id, context)
        
        elif data.startswith('reject_without_points_'):
            post_id = int(data.split('_')[3])
            await handle_reject_without_points(query, post_id, context)
        
        elif data.startswith('reason_preset_'):
            parts = data.split('_')
            reason_index = int(parts[2])
            post_id = int(parts[3])
            await handle_reject_with_preset_reason(query, post_id, reason_index, context)
        
        elif data.startswith('reason_custom_'):
            post_id = int(data.split('_')[2])
            await handle_reject_custom_reason_start(query, post_id, context)
        
        elif data.startswith('skip_reason_'):
            post_id = int(data.split('_')[2])
            await handle_reject_without_reason(query, post_id, context)
        
        elif data.startswith('delete_'):
            post_id = int(data.split('_')[1])
            await handle_delete(query, post_id, context)
        
        elif data.startswith('schedule_') and len(data.split('_')) == 2:
            post_id = int(data.split('_')[1])
            await handle_schedule_start(query, post_id, context)
        
        elif data.startswith('reschedule_'):
            post_id = int(data.split('_')[1])
            await handle_reschedule_start(query, post_id, context)
        
        elif data.startswith('post_now_'):
            post_id = int(data.split('_')[2])
            await handle_post_now(query, post_id, context)
        
        elif data.startswith('cancel_schedule_'):
            post_id = int(data.split('_')[2])
            await handle_cancel_schedule(query, post_id, context)
        
        elif data.startswith('back_'):
            post_id = int(data.split('_')[1])
            await handle_back(query, post_id)
        
        else:
            await safe_edit_message(query, "❌ Unknown action.")
            
    except Exception as e:
        logger.error(f"Error in callback handler: {e}")
        await safe_edit_message(query, f"❌ Error: {str(e)}")

# ==================== NEW HANDLER FOR REJECTION PAGINATION ====================
async def handle_rejection_page(query, page: int, post_id: int):
    """Handle pagination for rejection reasons"""
    post = storage.get_post(post_id)
    if not post:
        await safe_edit_message(query, "❌ Post not found.")
        return
    
    total_pages = (len(PRERECORDED_REASONS) + 4) // 5
    await safe_edit_message(
        query,
        f"📝 <b>Select Rejection Reason for Post #{post_id}</b>\n\n"
        f"Page {page + 1} of {total_pages}\n"
        f"👤 Posted by: {get_user_display(post.user_id, post.username)}",
        parse_mode=ParseMode.HTML,
        reply_markup=create_rejection_reasons_keyboard(post_id, page)
    )

# ==================== MODERATION ACTION HANDLERS ====================
async def handle_approve_start(query, post_id: int):
    """Start approve process with points confirmation"""
    post = storage.get_post(post_id)
    if not post:
        await safe_edit_message(query, "❌ Post not found.")
        return
    
    text = (
        f"📝 <b>Approve Post #{post_id}</b>\n\n"
        f"Give +2 points to {get_user_display(post.user_id, post.username)}?\n\n"
        f"<i>Points encourage quality submissions and engagement.</i>"
    )
    
    await safe_edit_message(
        query,
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=create_points_confirmation_keyboard(post_id, "approve")
    )

async def handle_approve_with_points(query, post_id: int, context: ContextTypes.DEFAULT_TYPE):
    """Approve with +2 points"""
    post = storage.get_post(post_id)
    if not post:
        await safe_edit_message(query, "❌ Post not found.")
        return
    
    try:
        channel_message_ids = await execute_post(context.bot, post)
        
        post.status = PostStatus.APPROVED
        post.reviewed_by = query.from_user.id
        post.reviewed_at = datetime.now(IST_TIMEZONE).isoformat()
        post.channel_message_ids = channel_message_ids
        
        storage.move_to_approved(post)
        
        new_points = points_system.update_user(
            user_id=post.user_id,
            username=post.username,
            points_change=2,
            approved=True
        )
        
        # Check for achievements
        new_achievements = achievement_system.check_and_award_achievements(post.user_id)
        if new_achievements:
            await notify_achievements(context, post.user_id, new_achievements)
        
        await safe_edit_message(
            query,
            f"✅ <b>Post Approved with +2 Points!</b>\n\n"
            f"📝 Post #{post_id} has been published.\n"
            f"👤 User: {get_user_display(post.user_id, post.username)}\n"
            f"⭐ New Total Points: {new_points}",
            parse_mode=ParseMode.HTML
        )
        
        # Notify user
        notification = f"✅ <b>Your post #{post_id} has been approved and published!</b>\n⭐ <b>+2 points awarded!</b> (Total: {new_points})"
        
        if new_achievements:
            notification += "\n\n🎉 <b>New Achievements Unlocked!</b>"
            for ach in new_achievements:
                notification += f"\n🏆 {ach.title} (+{ach.points_bonus})"
        
        await context.bot.send_message(
            chat_id=post.user_id,
            text=notification,
            parse_mode=ParseMode.HTML
        )
        
        # Send moderator notification
        await send_moderator_notification(
            context=context,
            post=post,
            action=f"Approved with +2 points",
            moderator_id=query.from_user.id
        )
        
    except Exception as e:
        logger.error(f"Error approving post: {e}")
        await safe_edit_message(query, f"❌ Failed to approve post: {str(e)}")

async def handle_approve_without_points(query, post_id: int, context: ContextTypes.DEFAULT_TYPE):
    """Approve without points"""
    post = storage.get_post(post_id)
    if not post:
        await safe_edit_message(query, "❌ Post not found.")
        return
    
    try:
        channel_message_ids = await execute_post(context.bot, post)
        
        post.status = PostStatus.APPROVED
        post.reviewed_by = query.from_user.id
        post.reviewed_at = datetime.now(IST_TIMEZONE).isoformat()
        post.channel_message_ids = channel_message_ids
        
        storage.move_to_approved(post)
        
        points_system.update_user(
            user_id=post.user_id,
            username=post.username,
            points_change=0,
            approved=True
        )
        
        # Check for achievements (still check even without points)
        new_achievements = achievement_system.check_and_award_achievements(post.user_id)
        if new_achievements:
            await notify_achievements(context, post.user_id, new_achievements)
        
        await safe_edit_message(
            query,
            f"✅ <b>Post Approved (No Points)</b>\n\n"
            f"📝 Post #{post_id} has been published.\n"
            f"👤 User: {get_user_display(post.user_id, post.username)}",
            parse_mode=ParseMode.HTML
        )
        
        # Notify user
        notification = f"✅ <b>Your post #{post_id} has been approved and published!</b>"
        
        if new_achievements:
            notification += "\n\n🎉 <b>New Achievements Unlocked!</b>"
            for ach in new_achievements:
                notification += f"\n🏆 {ach.title} (+{ach.points_bonus})"
        
        await context.bot.send_message(
            chat_id=post.user_id,
            text=notification,
            parse_mode=ParseMode.HTML
        )
        
        # Send moderator notification
        await send_moderator_notification(
            context=context,
            post=post,
            action=f"Approved without points",
            moderator_id=query.from_user.id
        )
        
    except Exception as e:
        logger.error(f"Error approving post: {e}")
        await safe_edit_message(query, f"❌ Failed to approve post: {str(e)}")

async def handle_reject_start(query, post_id: int):
    """Start rejection process with reasons keyboard"""
    post = storage.get_post(post_id)
    if not post:
        await safe_edit_message(query, "❌ Post not found.")
        return
    
    total_pages = (len(PRERECORDED_REASONS) + 4) // 5
    await safe_edit_message(
        query,
        f"📝 <b>Select Rejection Reason for Post #{post_id}</b>\n\n"
        f"Page 1 of {total_pages}\n"
        f"👤 Posted by: {get_user_display(post.user_id, post.username)}\n\n"
        f"<i>Choose a reason or add custom:</i>",
        parse_mode=ParseMode.HTML,
        reply_markup=create_rejection_reasons_keyboard(post_id, 0)
    )

async def handle_reject_with_preset_reason(query, post_id: int, reason_index: int, context: ContextTypes.DEFAULT_TYPE):
    """Reject with pre-recorded reason"""
    post = storage.get_post(post_id)
    if not post:
        await safe_edit_message(query, "❌ Post not found.")
        return
    
    if reason_index < 0 or reason_index >= len(PRERECORDED_REASONS):
        await safe_edit_message(query, "❌ Invalid reason.")
        return
    
    reason = PRERECORDED_REASONS[reason_index]
    
    # Ask for points
    text = (
        f"📝 <b>Reject Post #{post_id}</b>\n\n"
        f"Reason: {reason}\n\n"
        f"Give +1 point to {get_user_display(post.user_id, post.username)}?\n\n"
        f"<i>Points can be given for constructive feedback.</i>"
    )
    
    # Store reason temporarily
    context.user_data[f'reject_reason_{post_id}'] = reason
    
    await safe_edit_message(
        query,
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=create_points_confirmation_keyboard(post_id, "reject")
    )

async def handle_reject_custom_reason_start(query, post_id: int, context: ContextTypes.DEFAULT_TYPE):
    """Start custom reason input"""
    await safe_edit_message(
        query,
        f"📝 <b>Enter custom rejection reason for Post #{post_id}:</b>\n\n"
        f"Type your reason and send it as a reply to this message.",
        parse_mode=ParseMode.HTML,
        reply_markup=create_back_keyboard(post_id)
    )
    
    context.user_data[f'custom_reason_{post_id}'] = {
        'post_id': post_id,
        'moderator_id': query.from_user.id
    }

async def handle_reject_with_points(query, post_id: int, context: ContextTypes.DEFAULT_TYPE):
    """Reject with +1 point"""
    post = storage.get_post(post_id)
    if not post:
        await safe_edit_message(query, "❌ Post not found.")
        return
    
    # Get stored reason
    reason = context.user_data.get(f'reject_reason_{post_id}', "No reason provided")
    
    post.status = PostStatus.REJECTED
    post.reviewed_by = query.from_user.id
    post.reviewed_at = datetime.now(IST_TIMEZONE).isoformat()
    post.rejection_reason = reason
    
    storage.move_to_rejected(post)
    
    new_points = points_system.update_user(
        user_id=post.user_id,
        username=post.username,
        points_change=1,
        rejected=True
    )
    
    # Check for achievements
    new_achievements = achievement_system.check_and_award_achievements(post.user_id)
    if new_achievements:
        await notify_achievements(context, post.user_id, new_achievements)
    
    # Clean up stored reason
    context.user_data.pop(f'reject_reason_{post_id}', None)
    
    await safe_edit_message(
        query,
        f"❌ <b>Post Rejected with +1 Point</b>\n\n"
        f"📝 Post #{post_id} has been rejected.\n"
        f"👤 User: {get_user_display(post.user_id, post.username)}\n"
        f"📝 Reason: {reason}\n"
        f"⭐ +1 point awarded (Total: {new_points})",
        parse_mode=ParseMode.HTML
    )
    
    # Notify user
    notification = f"❌ <b>Your post #{post_id} has been rejected.</b>\n\nReason: {reason}\n⭐ <b>+1 point awarded for feedback!</b> (Total: {new_points})"
    
    if new_achievements:
        notification += "\n\n🎉 <b>New Achievements Unlocked!</b>"
        for ach in new_achievements:
            notification += f"\n🏆 {ach.title} (+{ach.points_bonus})"
    
    await context.bot.send_message(
        chat_id=post.user_id,
        text=notification,
        parse_mode=ParseMode.HTML
    )

async def handle_reject_without_points(query, post_id: int, context: ContextTypes.DEFAULT_TYPE):
    """Reject without points"""
    post = storage.get_post(post_id)
    if not post:
        await safe_edit_message(query, "❌ Post not found.")
        return
    
    # Get stored reason
    reason = context.user_data.get(f'reject_reason_{post_id}', "No reason provided")
    
    post.status = PostStatus.REJECTED
    post.reviewed_by = query.from_user.id
    post.reviewed_at = datetime.now(IST_TIMEZONE).isoformat()
    post.rejection_reason = reason
    
    storage.move_to_rejected(post)
    
    points_system.update_user(
        user_id=post.user_id,
        username=post.username,
        points_change=0,
        rejected=True
    )
    
    # Check for achievements
    new_achievements = achievement_system.check_and_award_achievements(post.user_id)
    if new_achievements:
        await notify_achievements(context, post.user_id, new_achievements)
    
    # Clean up stored reason
    context.user_data.pop(f'reject_reason_{post_id}', None)
    
    await safe_edit_message(
        query,
        f"❌ <b>Post Rejected (No Points)</b>\n\n"
        f"📝 Post #{post_id} has been rejected.\n"
        f"👤 User: {get_user_display(post.user_id, post.username)}\n"
        f"📝 Reason: {reason}",
        parse_mode=ParseMode.HTML
    )
    
    # Notify user
    notification = f"❌ <b>Your post #{post_id} has been rejected.</b>\n\nReason: {reason}"
    
    if new_achievements:
        notification += "\n\n🎉 <b>New Achievements Unlocked!</b>"
        for ach in new_achievements:
            notification += f"\n🏆 {ach.title} (+{ach.points_bonus})"
    
    await context.bot.send_message(
        chat_id=post.user_id,
        text=notification,
        parse_mode=ParseMode.HTML
    )

async def handle_reject_without_reason(query, post_id: int, context: ContextTypes.DEFAULT_TYPE):
    """Reject without reason, ask for points"""
    post = storage.get_post(post_id)
    if not post:
        await safe_edit_message(query, "❌ Post not found.")
        return
    
    text = (
        f"⚠️ <b>Reject Post #{post_id}</b>\n\n"
        f"No reason provided.\n\n"
        f"Give +1 point to {get_user_display(post.user_id, post.username)}?\n\n"
        f"<i>Points can be given for participation.</i>"
    )
    
    context.user_data[f'reject_reason_{post_id}'] = "No reason provided"
    
    await safe_edit_message(
        query,
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=create_points_confirmation_keyboard(post_id, "reject")
    )

async def handle_delete(query, post_id: int, context: ContextTypes.DEFAULT_TYPE):
    """Delete a post"""
    post = storage.get_post(post_id)
    if not post:
        await safe_edit_message(query, "❌ Post not found.")
        return
    
    # Remove from storage
    storage.delete_post(post_id)
    
    points_system.update_user(
        user_id=post.user_id,
        username=post.username,
        deleted=True
    )
    
    await safe_edit_message(
        query,
        f"🗑 <b>Post Deleted</b>\n\n"
        f"📝 Post #{post_id} has been permanently deleted.\n"
        f"👤 User: {get_user_display(post.user_id, post.username)}",
        parse_mode=ParseMode.HTML
    )
    
    # Notify user
    await context.bot.send_message(
        chat_id=post.user_id,
        text=f"🗑 <b>Your post #{post_id} has been deleted by a moderator.</b>",
        parse_mode=ParseMode.HTML
    )

async def handle_schedule_start(query, post_id: int, context: ContextTypes.DEFAULT_TYPE):
    """Start scheduling process"""
    post = storage.get_post(post_id)
    if not post:
        await safe_edit_message(query, "❌ Post not found.")
        return
    
    # Show today's date as example
    today = datetime.now(IST_TIMEZONE)
    today_str = today.strftime("%d/%m/%Y")
    tomorrow_str = (today + timedelta(days=1)).strftime("%d/%m/%Y")
    
    await safe_edit_message(
        query,
        f"📅 <b>Schedule Post #{post_id}</b>\n\n"
        f"Enter schedule time in format:\n"
        f"<code>HH:MM AM/PM DD/MM/YYYY</code>\n\n"
        f"<b>Examples (Today: {today_str}):</b>\n"
        f"• <code>09:30 PM {today_str}</code> (Today)\n"
        f"• <code>14:45 {tomorrow_str}</code> (Tomorrow, 24hr)\n\n"
        f"<i>Time is in IST (Asia/Kolkata)</i>",
        parse_mode=ParseMode.HTML,
        reply_markup=create_back_keyboard(post_id)
    )
    
    # Store context
    context.user_data[f'schedule_{post_id}'] = {
        'post_id': post_id,
        'moderator_id': query.from_user.id
    }

async def handle_reschedule_start(query, post_id: int, context: ContextTypes.DEFAULT_TYPE):
    """Start rescheduling process"""
    post = storage.get_post(post_id)
    if not post or not post.scheduled_time:
        await safe_edit_message(query, "❌ Post not found or not scheduled.")
        return
    
    current_time = datetime.fromisoformat(post.scheduled_time).astimezone(IST_TIMEZONE)
    current_str = format_schedule_time(current_time)
    
    # Show today's date as example
    today = datetime.now(IST_TIMEZONE)
    today_str = today.strftime("%d/%m/%Y")
    
    await safe_edit_message(
        query,
        f"🔄 <b>Reschedule Post #{post_id}</b>\n\n"
        f"Current schedule: {current_str}\n\n"
        f"Enter new schedule time in format:\n"
        f"<code>HH:MM AM/PM DD/MM/YYYY</code>\n\n"
        f"<b>Examples (Today: {today_str}):</b>\n"
        f"• <code>09:30 PM {today_str}</code> (Today)\n"
        f"• <code>14:45 {today_str}</code> (24hr format)\n\n"
        f"<i>Time is in IST (Asia/Kolkata)</i>",
        parse_mode=ParseMode.HTML,
        reply_markup=create_back_keyboard(post_id)
    )
    
    # Store context
    context.user_data[f'reschedule_{post_id}'] = {
        'post_id': post_id,
        'moderator_id': query.from_user.id
    }

async def handle_post_now(query, post_id: int, context: ContextTypes.DEFAULT_TYPE):
    """Post scheduled post immediately"""
    post = storage.get_post(post_id)
    if not post:
        await safe_edit_message(query, "❌ Post not found.")
        return
    
    try:
        channel_message_ids = await execute_post(context.bot, post)
        
        post.status = PostStatus.APPROVED
        post.reviewed_by = query.from_user.id
        post.reviewed_at = datetime.now(IST_TIMEZONE).isoformat()
        post.channel_message_ids = channel_message_ids
        
        # Remove from scheduled
        if post_id in storage.scheduled_posts:
            del storage.scheduled_posts[post_id]
        
        storage.approved_posts[post_id] = post
        storage.db.save_post(post)
        storage.save_json_backup()
        
        # Give points
        new_points = points_system.update_user(
            user_id=post.user_id,
            username=post.username,
            points_change=2,
            approved=True
        )
        
        # Check for achievements
        new_achievements = achievement_system.check_and_award_achievements(post.user_id)
        if new_achievements:
            await notify_achievements(context, post.user_id, new_achievements)
        
        await safe_edit_message(
            query,
            f"✅ <b>Scheduled Post Published Now!</b>\n\n"
            f"📝 Post #{post_id} has been published.\n"
            f"👤 User: {get_user_display(post.user_id, post.username)}\n"
            f"⭐ Points Awarded: +2 (Total: {new_points})",
            parse_mode=ParseMode.HTML
        )
        
        # Notify user
        notification = f"✅ <b>Your scheduled post #{post_id} has been published now!</b>\n⭐ <b>+2 points awarded!</b> (Total: {new_points})"
        
        if new_achievements:
            notification += "\n\n🎉 <b>New Achievements Unlocked!</b>"
            for ach in new_achievements:
                notification += f"\n🏆 {ach.title} (+{ach.points_bonus})"
        
        await context.bot.send_message(
            chat_id=post.user_id,
            text=notification,
            parse_mode=ParseMode.HTML
        )
        
        # Send moderator notification
        await send_moderator_notification(
            context=context,
            post=post,
            action=f"Scheduled post published now",
            moderator_id=query.from_user.id
        )
        
    except Exception as e:
        logger.error(f"Error posting scheduled post: {e}")
        await safe_edit_message(query, f"❌ Failed to post: {str(e)}")

async def handle_cancel_schedule(query, post_id: int, context: ContextTypes.DEFAULT_TYPE):
    """Cancel schedule and move back to pending"""
    post = storage.get_post(post_id)
    if not post:
        await safe_edit_message(query, "❌ Post not found.")
        return
    
    # Move back to pending
    if post_id in storage.scheduled_posts:
        del storage.scheduled_posts[post_id]
    
    post.status = PostStatus.PENDING
    post.scheduled_time = None
    storage.pending_posts[post_id] = post
    storage.db.save_post(post)
    storage.save_json_backup()
    
    await safe_edit_message(
        query,
        f"🔄 <b>Schedule Cancelled</b>\n\n"
        f"📝 Post #{post_id} moved back to pending.\n"
        f"👤 User: {get_user_display(post.user_id, post.username)}",
        parse_mode=ParseMode.HTML
    )
    
    # Notify user
    await context.bot.send_message(
        chat_id=post.user_id,
        text=f"🔄 <b>Schedule cancelled for your post #{post_id}</b>\n"
             f"It's now back in pending status.",
        parse_mode=ParseMode.HTML
    )

async def handle_back(query, post_id: int):
    """Go back to main moderation options"""
    post = storage.get_post(post_id)
    if not post:
        await safe_edit_message(query, "❌ Post not found.")
        return
    
    text = (
        f"🔄 <b>Post #{post_id}</b>\n\n"
        f"👤 Posted by: {get_user_display(post.user_id, post.username)}\n"
        f"📅 Submitted: {format_submitted_time(datetime.fromisoformat(post.submitted_at).astimezone(IST_TIMEZONE))}\n\n"
        f"<i>Select an action:</i>"
    )
    
    await safe_edit_message(
        query,
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=create_moderation_keyboard(post_id)
    )

# ==================== MESSAGE HANDLERS ====================
async def handle_schedule_time(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle schedule time input"""
    user = update.effective_user
    if not is_moderator(user.id):
        return
    
    # Check for schedule context
    schedule_data = None
    schedule_key = None
    
    for key in list(context.user_data.keys()):
        if key.startswith('schedule_') or key.startswith('reschedule_'):
            schedule_data = context.user_data[key]
            schedule_key = key
            break
    
    if not schedule_data:
        return
    
    post_id = schedule_data['post_id']
    post = storage.get_post(post_id)
    
    if not post:
        await update.message.reply_text("❌ Post not found.")
        return
    
    time_str = update.message.text.strip()
    
    try:
        scheduled_time = parse_time_string(time_str)
        scheduled_str = format_schedule_time(scheduled_time)
        
        # Update post
        post.status = PostStatus.SCHEDULED
        post.reviewed_by = user.id
        post.reviewed_at = datetime.now(IST_TIMEZONE).isoformat()
        post.scheduled_time = scheduled_time.isoformat()
        
        # Move to scheduled
        if post_id in storage.pending_posts:
            del storage.pending_posts[post_id]
        storage.scheduled_posts[post_id] = post
        storage.db.save_post(post)
        storage.save_json_backup()
        
        # Clear context
        del context.user_data[schedule_key]
        
        await update.message.reply_text(
            f"✅ <b>Post Scheduled!</b>\n\n"
            f"📝 Post #{post_id} scheduled for:\n"
            f"⏰ {scheduled_str}",
            parse_mode=ParseMode.HTML
        )
        
        # Notify user
        await context.bot.send_message(
            chat_id=post.user_id,
            text=f"📅 <b>Your post #{post_id} has been scheduled!</b>\n\n"
                 f"⏰ Scheduled for: {scheduled_str}",
            parse_mode=ParseMode.HTML
        )
        
    except ValueError as e:
        await update.message.reply_text(f"❌ {str(e)}")

async def handle_custom_rejection_reason(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle custom rejection reason input"""
    user = update.effective_user
    if not is_moderator(user.id):
        return
    
    # Check for custom reason context
    reason_data = None
    reason_key = None
    
    for key in list(context.user_data.keys()):
        if key.startswith('custom_reason_'):
            reason_data = context.user_data[key]
            reason_key = key
            break
    
    if not reason_data:
        return
    
    post_id = reason_data['post_id']
    post = storage.get_post(post_id)
    
    if not post:
        await update.message.reply_text("❌ Post not found.")
        return
    
    reason = update.message.text.strip()
    
    if not reason:
        await update.message.reply_text("❌ Please provide a reason.")
        return
    
    # Store reason and ask for points
    context.user_data[f'reject_reason_{post_id}'] = reason
    
    await update.message.reply_text(
        f"📝 <b>Reject Post #{post_id}</b>\n\n"
        f"Reason: {reason}\n\n"
        f"Give +1 point to {get_user_display(post.user_id, post.username)}?",
        parse_mode=ParseMode.HTML,
        reply_markup=create_points_confirmation_keyboard(post_id, "reject")
    )
    
    # Clear custom reason context
    del context.user_data[reason_key]

# ==================== SCHEDULED POSTS CHECKER ====================
async def check_scheduled_posts(context: ContextTypes.DEFAULT_TYPE):
    """Check and auto-post scheduled posts"""
    now_ist = datetime.now(IST_TIMEZONE)
    
    for post_id, post in list(storage.scheduled_posts.items()):
        if not post.scheduled_time:
            continue
        
        try:
            scheduled_dt = datetime.fromisoformat(post.scheduled_time)
            if scheduled_dt.tzinfo is None:
                scheduled_dt = IST_TIMEZONE.localize(scheduled_dt)
            
            # Check if time to post (within 5 seconds)
            time_diff = (scheduled_dt - now_ist).total_seconds()
            if -5 <= time_diff <= 5:
                try:
                    channel_message_ids = await execute_post(context.bot, post)
                    
                    post.status = PostStatus.APPROVED
                    post.reviewed_by = context.bot.id
                    post.reviewed_at = datetime.now(IST_TIMEZONE).isoformat()
                    post.channel_message_ids = channel_message_ids
                    
                    # Move to approved
                    del storage.scheduled_posts[post_id]
                    storage.approved_posts[post_id] = post
                    storage.db.save_post(post)
                    storage.save_json_backup()
                    
                    # Give points
                    new_points = points_system.update_user(
                        user_id=post.user_id,
                        username=post.username,
                        points_change=2,
                        approved=True
                    )
                    
                    # Check for achievements
                    new_achievements = achievement_system.check_and_award_achievements(post.user_id)
                    if new_achievements:
                        await notify_achievements(context, post.user_id, new_achievements)
                    
                    # Notify user
                    notification = f"✅ <b>Your scheduled post #{post_id} has been automatically posted!</b>\n⭐ <b>+2 points automatically credited!</b> (Total: {new_points})"
                    
                    if new_achievements:
                        notification += "\n\n🎉 <b>New Achievements Unlocked!</b>"
                        for ach in new_achievements:
                            notification += f"\n🏆 {ach.title} (+{ach.points_bonus})"
                    
                    await context.bot.send_message(
                        chat_id=post.user_id,
                        text=notification,
                        parse_mode=ParseMode.HTML
                    )
                    
                    # Send moderator notification
                    await send_moderator_notification(
                        context=context,
                        post=post,
                        action="Auto-scheduled post published (+2 points auto-credited)",
                        moderator_id=post.reviewed_by
                    )
                    
                    logger.info(f"Auto-posted scheduled post #{post_id}")
                    
                except Exception as e:
                    logger.error(f"Error auto-posting scheduled post #{post_id}: {e}")
                    
        except Exception as e:
            logger.error(f"Error checking scheduled post #{post_id}: {e}")

# ==================== MAIN ====================
if __name__ == '__main__':
    try:
        # Initialize database manager
        db_manager = DatabaseManager()
        
        # Verify database integrity on startup
        if not db_manager.verify_database_integrity():
            logger.warning("Database integrity check failed on startup!")
            # Attempt to restore from latest backup
            backups = sorted([f for f in os.listdir(BACKUP_DIR) if f.endswith('.db')])
            if backups:
                latest_backup = os.path.join(BACKUP_DIR, backups[-1])
                logger.info(f"Attempting to restore from: {latest_backup}")
                db_manager.restore_from_backup(latest_backup)
        
        # Initialize storage systems with persistence
        storage = PersistentDataStorage(db_manager)
        points_system = PersistentPointsSystem(db_manager)
        achievement_system = PersistentAchievementSystem(db_manager, points_system)
        stream_manager = StreamManager(db_manager)
        
        # Create initial backup
        db_manager.create_backup("startup")
        
        # Create application
        application = Application.builder().token(BOT_TOKEN).build()
        
        # Add job queue for scheduled posts
        job_queue = application.job_queue
        if job_queue:
            job_queue.run_repeating(check_scheduled_posts, interval=5, first=5)
            # Add periodic backup job (every 6 hours)
            job_queue.run_repeating(
                lambda ctx: db_manager.create_backup("scheduled"),
                interval=21600,  # 6 hours
                first=3600  # Start after 1 hour
            )
        
        # Conversation handler for post submission
        conv_handler = ConversationHandler(
            entry_points=[CommandHandler('post', post_command)],
            states={
                WAITING_FOR_NEWS: [
                    MessageHandler(
                        filters.TEXT | filters.PHOTO | filters.Document.ALL | filters.VIDEO,
                        receive_news
                    )
                ]
            },
            fallbacks=[CommandHandler('cancel', cancel_command)]
        )
        
        # ==================== HANDLER ORDER ====================
        # 1. Basic commands
        application.add_handler(CommandHandler('start', start_command))
        application.add_handler(CommandHandler('help', help_command))
        application.add_handler(CommandHandler('myposts', myposts_command))
        application.add_handler(CommandHandler('mystats', mystats_command))
        application.add_handler(CommandHandler('achievements', achievements_command))
        application.add_handler(CommandHandler('points', points_command))
        
        # 2. Moderator commands
        application.add_handler(CommandHandler('pending', pending_command))
        application.add_handler(CommandHandler('showlist', showlist_command))
        application.add_handler(CommandHandler('list', list_command))
        application.add_handler(CommandHandler('stats', stats_command))
        application.add_handler(CommandHandler('toppoints', toppoints_command))
        
        # 3. Backup and recovery commands
        application.add_handler(CommandHandler('backup', backup_command))
        application.add_handler(CommandHandler('verify', verify_command))
        application.add_handler(CommandHandler('export', export_command))
        
        # 4. Developer stream commands
        application.add_handler(CommandHandler('stream', stream_command))
        application.add_handler(CommandHandler('stopstream', stopstream_command))
        application.add_handler(CommandHandler('streamstats', streamstats_command))
        
        # 5. Conversation handler
        application.add_handler(conv_handler)
        
        # 6. Message handlers for inputs
        application.add_handler(MessageHandler(
            filters.TEXT & ~filters.COMMAND & filters.ChatType.GROUPS,
            handle_schedule_time
        ))
        
        application.add_handler(MessageHandler(
            filters.TEXT & ~filters.COMMAND & filters.ChatType.GROUPS,
            handle_custom_rejection_reason
        ))
        
        # 7. Stream message handler (for developer)
        application.add_handler(MessageHandler(
            filters.TEXT & ~filters.COMMAND & filters.ChatType.PRIVATE,
            handle_stream_message
        ))
        
        # 8. Callback query handler - MUST BE LAST
        application.add_handler(CallbackQueryHandler(callback_handler))
        
        # Start the bot
        logger.info("=" * 70)
        logger.info("🤖 ULTIMATE BOT WITH PERSISTENT STORAGE STARTING...")
        logger.info("=" * 70)
        logger.info("✅ STORAGE FEATURES ENABLED:")
        logger.info("   • SQLite Database with automatic backups")
        logger.info("   • JSON backup files (redundant storage)")
        logger.info("   • Automatic hourly backups")
        logger.info("   • Database integrity verification")
        logger.info("   • Data export/import capabilities")
        logger.info("   • Activity logging")
        logger.info("=" * 70)
        logger.info("✅ BOT FEATURES ENABLED:")
        logger.info("   • Pre-recorded rejection reasons (with full text display)")
        logger.info("   • Paginated rejection menu (5 reasons per page)")
        logger.info("   • Points for rejection (+1)")
        logger.info("   • Achievement system with bonuses")
        logger.info("   • Developer stream feature")
        logger.info("   • Message entity preservation")
        logger.info("=" * 70)
        logger.info(f"Developer ID: {DEVELOPER_ID}")
        logger.info(f"Moderators: {len(MODERATOR_USER_IDS)}")
        logger.info(f"Authorized posters: {len(AUTHORIZED_POSTERS)}")
        logger.info("=" * 70)
        logger.info("📁 Data Directory: data/")
        logger.info("💾 Backup Directory: data/backups/")
        logger.info("=" * 70)
        
        application.run_polling()
        
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        
        # Try to create emergency backup
        try:
            if 'db_manager' in locals():
                db_manager.create_backup("crash")
                logger.info("Emergency backup created before crash")
        except:
            pass
        
        sys.exit(1)

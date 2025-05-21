"""
Sistema de caché inteligente para operaciones de scraping.
Permite almacenar temporalmente resultados de operaciones costosas
y se limpia automáticamente al finalizar el procesamiento.
"""

import os
import json
import time
import shutil
import logging
import sqlite3
from pathlib import Path
from typing import Dict, Any, Optional, Union, List, Tuple
from datetime import datetime, timedelta

from src.core.config import DATA_DIR

logger = logging.getLogger("cache_manager")

class CacheManager:
    """
    Gestor de caché para operaciones de scraping.
    Implementa un sistema de caché con TTL (Time-To-Live) configurable
    y limpieza automática para evitar crecimiento excesivo.
    """
    
    def __init__(
        self, 
        cache_dir: Optional[str] = None, 
        ttl_seconds: int = 3600,
        max_size_mb: int = 100,
        storage_type: str = "sqlite"  # Opciones: "sqlite", "json", "memory"
    ):
        """
        Inicializa el gestor de caché.
        
        Args:
            cache_dir: Directorio donde se almacenará la caché
            ttl_seconds: Tiempo de vida de los elementos en caché (segundos)
            max_size_mb: Tamaño máximo de la caché en MB
            storage_type: Tipo de almacenamiento ("sqlite", "json", "memory")
        """
        if cache_dir is None:
            cache_dir = str(DATA_DIR / "cache")
            
        self.cache_dir = Path(cache_dir)
        self.ttl_seconds = ttl_seconds
        self.max_size_mb = max_size_mb
        self.storage_type = storage_type
        self.memory_cache = {}  # Caché en memoria
        self.stats = {
            "hits": 0,
            "misses": 0,
            "items_added": 0,
            "items_expired": 0,
            "cleanups_performed": 0,
            "last_cleanup": None
        }
        
        # Crear directorio de caché si no existe
        os.makedirs(self.cache_dir, exist_ok=True)
        
        # Inicializar almacenamiento según el tipo
        if storage_type == "sqlite":
            self._init_sqlite_storage()
        elif storage_type == "json":
            self._init_json_storage()
        
        # Realizar limpieza inicial para eliminar elementos expirados
        self.cleanup(force=True)
        
        logger.info(f"Caché inicializada: {self.storage_type} en {self.cache_dir}")
    
    def _init_sqlite_storage(self):
        """Inicializa el almacenamiento SQLite."""
        self.db_path = self.cache_dir / "cache.db"
        
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        
        # Crear tabla si no existe
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS cache (
            key TEXT PRIMARY KEY,
            value TEXT,
            created_at REAL,
            expires_at REAL,
            metadata TEXT
        )
        ''')
        
        # Crear índice para expiración
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_expires_at ON cache (expires_at)')
        
        conn.commit()
        conn.close()
    
    def _init_json_storage(self):
        """Inicializa el almacenamiento JSON."""
        self.json_path = self.cache_dir / "cache_index.json"
        
        # Crear archivo de índice si no existe
        if not self.json_path.exists():
            with open(self.json_path, 'w', encoding='utf-8') as f:
                json.dump({
                    "metadata": {
                        "created_at": time.time(),
                        "last_updated": time.time()
                    },
                    "items": {}
                }, f)
    
    def _get_cache_size(self) -> float:
        """
        Obtiene el tamaño actual de la caché en MB.
        
        Returns:
            Tamaño de la caché en MB
        """
        if self.storage_type == "memory":
            # Estimación aproximada para caché en memoria
            import sys
            return sys.getsizeof(str(self.memory_cache)) / (1024 * 1024)
        
        elif self.storage_type == "sqlite":
            if self.db_path.exists():
                return self.db_path.stat().st_size / (1024 * 1024)
        
        elif self.storage_type == "json":
            total_size = 0
            
            # Tamaño del índice
            if self.json_path.exists():
                total_size += self.json_path.stat().st_size
            
            # Tamaño de los archivos de caché
            for file_path in self.cache_dir.glob("cache_*.json"):
                total_size += file_path.stat().st_size
            
            return total_size / (1024 * 1024)
        
        return 0
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Obtiene un valor de la caché.
        
        Args:
            key: Clave del elemento a obtener
            default: Valor por defecto si no existe o está expirado
            
        Returns:
            Valor almacenado o valor por defecto
        """
        # Caché en memoria
        if self.storage_type == "memory":
            if key in self.memory_cache:
                item = self.memory_cache[key]
                
                # Verificar expiración
                if item["expires_at"] > time.time():
                    self.stats["hits"] += 1
                    return item["value"]
                else:
                    # Eliminar elemento expirado
                    del self.memory_cache[key]
                    self.stats["items_expired"] += 1
            
            self.stats["misses"] += 1
            return default
        
        # Caché SQLite
        elif self.storage_type == "sqlite":
            try:
                conn = sqlite3.connect(str(self.db_path))
                cursor = conn.cursor()
                
                cursor.execute(
                    "SELECT value, expires_at FROM cache WHERE key = ?", 
                    (key,)
                )
                result = cursor.fetchone()
                
                if result:
                    value_str, expires_at = result
                    
                    # Verificar expiración
                    if expires_at > time.time():
                        self.stats["hits"] += 1
                        conn.close()
                        return json.loads(value_str)
                    else:
                        # Eliminar elemento expirado
                        cursor.execute("DELETE FROM cache WHERE key = ?", (key,))
                        conn.commit()
                        self.stats["items_expired"] += 1
                
                conn.close()
                self.stats["misses"] += 1
                return default
                
            except Exception as e:
                logger.error(f"Error al obtener de caché SQLite: {e}")
                return default
        
        # Caché JSON
        elif self.storage_type == "json":
            try:
                if not self.json_path.exists():
                    self.stats["misses"] += 1
                    return default
                
                with open(self.json_path, 'r', encoding='utf-8') as f:
                    index = json.load(f)
                
                if key in index["items"]:
                    item_meta = index["items"][key]
                    
                    # Verificar expiración
                    if item_meta["expires_at"] > time.time():
                        # Cargar valor desde archivo individual
                        cache_file = self.cache_dir / f"cache_{item_meta['file_id']}.json"
                        
                        if cache_file.exists():
                            with open(cache_file, 'r', encoding='utf-8') as f:
                                cache_data = json.load(f)
                                
                                if key in cache_data:
                                    self.stats["hits"] += 1
                                    return cache_data[key]["value"]
                    else:
                        # Marcar como expirado (se eliminará en la próxima limpieza)
                        self.stats["items_expired"] += 1
                
                self.stats["misses"] += 1
                return default
                
            except Exception as e:
                logger.error(f"Error al obtener de caché JSON: {e}")
                return default
    
    def set(self, key: str, value: Any, ttl: Optional[int] = None, metadata: Dict = None) -> bool:
        """
        Almacena un valor en la caché.
        
        Args:
            key: Clave del elemento
            value: Valor a almacenar
            ttl: Tiempo de vida en segundos (opcional, usa el valor por defecto si no se especifica)
            metadata: Metadatos adicionales (opcional)
            
        Returns:
            True si se almacenó correctamente, False en caso contrario
        """
        # Verificar tamaño de la caché antes de añadir
        if self._get_cache_size() >= self.max_size_mb:
            logger.warning(f"Caché alcanzó tamaño máximo ({self.max_size_mb}MB). Ejecutando limpieza...")
            self.cleanup(force=True)
            
            # Si sigue excediendo después de la limpieza, no almacenar
            if self._get_cache_size() >= self.max_size_mb:
                logger.error(f"No se puede almacenar en caché: tamaño máximo excedido incluso después de limpieza")
                return False
        
        # Calcular tiempo de expiración
        ttl = ttl or self.ttl_seconds
        created_at = time.time()
        expires_at = created_at + ttl
        
        # Metadatos por defecto
        if metadata is None:
            metadata = {}
        
        # Caché en memoria
        if self.storage_type == "memory":
            self.memory_cache[key] = {
                "value": value,
                "created_at": created_at,
                "expires_at": expires_at,
                "metadata": metadata
            }
            self.stats["items_added"] += 1
            return True
        
        # Caché SQLite
        elif self.storage_type == "sqlite":
            try:
                conn = sqlite3.connect(str(self.db_path))
                cursor = conn.cursor()
                
                # Serializar valor y metadatos
                value_json = json.dumps(value, ensure_ascii=False)
                metadata_json = json.dumps(metadata, ensure_ascii=False)
                
                # Insertar o actualizar
                cursor.execute(
                    """
                    INSERT OR REPLACE INTO cache 
                    (key, value, created_at, expires_at, metadata) 
                    VALUES (?, ?, ?, ?, ?)
                    """, 
                    (key, value_json, created_at, expires_at, metadata_json)
                )
                
                conn.commit()
                conn.close()
                
                self.stats["items_added"] += 1
                return True
                
            except Exception as e:
                logger.error(f"Error al almacenar en caché SQLite: {e}")
                return False
        
        # Caché JSON
        elif self.storage_type == "json":
            try:
                # Cargar índice
                if self.json_path.exists():
                    with open(self.json_path, 'r', encoding='utf-8') as f:
                        index = json.load(f)
                else:
                    index = {
                        "metadata": {
                            "created_at": time.time(),
                            "last_updated": time.time()
                        },
                        "items": {}
                    }
                
                # Generar ID de archivo
                file_id = str(int(time.time() * 1000))
                
                # Actualizar índice
                index["items"][key] = {
                    "file_id": file_id,
                    "created_at": created_at,
                    "expires_at": expires_at,
                    "metadata": metadata
                }
                
                index["metadata"]["last_updated"] = time.time()
                
                # Guardar índice
                with open(self.json_path, 'w', encoding='utf-8') as f:
                    json.dump(index, f, ensure_ascii=False)
                
                # Guardar valor en archivo individual
                cache_file = self.cache_dir / f"cache_{file_id}.json"
                
                with open(cache_file, 'w', encoding='utf-8') as f:
                    json.dump({
                        key: {
                            "value": value,
                            "created_at": created_at,
                            "expires_at": expires_at
                        }
                    }, f, ensure_ascii=False)
                
                self.stats["items_added"] += 1
                return True
                
            except Exception as e:
                logger.error(f"Error al almacenar en caché JSON: {e}")
                return False
    
    def delete(self, key: str) -> bool:
        """
        Elimina un elemento de la caché.
        
        Args:
            key: Clave del elemento a eliminar
            
        Returns:
            True si se eliminó correctamente, False en caso contrario
        """
        # Caché en memoria
        if self.storage_type == "memory":
            if key in self.memory_cache:
                del self.memory_cache[key]
                return True
            return False
        
        # Caché SQLite
        elif self.storage_type == "sqlite":
            try:
                conn = sqlite3.connect(str(self.db_path))
                cursor = conn.cursor()
                
                cursor.execute("DELETE FROM cache WHERE key = ?", (key,))
                deleted = cursor.rowcount > 0
                
                conn.commit()
                conn.close()
                
                return deleted
                
            except Exception as e:
                logger.error(f"Error al eliminar de caché SQLite: {e}")
                return False
        
        # Caché JSON
        elif self.storage_type == "json":
            try:
                if not self.json_path.exists():
                    return False
                
                with open(self.json_path, 'r', encoding='utf-8') as f:
                    index = json.load(f)
                
                if key in index["items"]:
                    # Obtener ID de archivo
                    file_id = index["items"][key]["file_id"]
                    
                    # Eliminar del índice
                    del index["items"][key]
                    
                    # Actualizar índice
                    index["metadata"]["last_updated"] = time.time()
                    
                    with open(self.json_path, 'w', encoding='utf-8') as f:
                        json.dump(index, f, ensure_ascii=False)
                    
                    # Intentar eliminar archivo si no hay más elementos que lo usen
                    cache_file = self.cache_dir / f"cache_{file_id}.json"
                    
                    # Verificar si otros elementos usan el mismo archivo
                    file_in_use = any(
                        item["file_id"] == file_id 
                        for item in index["items"].values()
                    )
                    
                    if not file_in_use and cache_file.exists():
                        cache_file.unlink()
                    
                    return True
                
                return False
                
            except Exception as e:
                logger.error(f"Error al eliminar de caché JSON: {e}")
                return False
    
    def cleanup(self, force: bool = False) -> int:
        """
        Limpia elementos expirados de la caché.
        
        Args:
            force: Si es True, realiza la limpieza incluso si no es necesario
            
        Returns:
            Número de elementos eliminados
        """
        # Verificar si es necesario realizar limpieza
        current_size = self._get_cache_size()
        
        if not force and current_size < self.max_size_mb * 0.8:
            # No es necesario limpiar si está por debajo del 80% del tamaño máximo
            return 0
        
        items_removed = 0
        now = time.time()
        
        # Caché en memoria
        if self.storage_type == "memory":
            # Identificar claves expiradas
            expired_keys = [
                k for k, v in self.memory_cache.items() 
                if v["expires_at"] <= now
            ]
            
            # Eliminar elementos expirados
            for key in expired_keys:
                del self.memory_cache[key]
                items_removed += 1
        
        # Caché SQLite
        elif self.storage_type == "sqlite":
            try:
                conn = sqlite3.connect(str(self.db_path))
                cursor = conn.cursor()
                
                # Eliminar elementos expirados
                cursor.execute("DELETE FROM cache WHERE expires_at <= ?", (now,))
                items_removed = cursor.rowcount
                
                # Optimizar base de datos
                cursor.execute("VACUUM")
                
                conn.commit()
                conn.close()
                
            except Exception as e:
                logger.error(f"Error al limpiar caché SQLite: {e}")
        
        # Caché JSON
        elif self.storage_type == "json":
            try:
                if not self.json_path.exists():
                    return 0
                
                with open(self.json_path, 'r', encoding='utf-8') as f:
                    index = json.load(f)
                
                # Identificar elementos expirados
                expired_keys = [
                    k for k, v in index["items"].items() 
                    if v["expires_at"] <= now
                ]
                
                # Eliminar elementos expirados
                for key in expired_keys:
                    file_id = index["items"][key]["file_id"]
                    del index["items"][key]
                    items_removed += 1
                    
                    # Verificar si otros elementos usan el mismo archivo
                    file_in_use = any(
                        item["file_id"] == file_id 
                        for item in index["items"].values()
                    )
                    
                    # Eliminar archivo si no está en uso
                    if not file_in_use:
                        cache_file = self.cache_dir / f"cache_{file_id}.json"
                        if cache_file.exists():
                            cache_file.unlink()
                
                # Actualizar índice
                index["metadata"]["last_updated"] = time.time()
                
                with open(self.json_path, 'w', encoding='utf-8') as f:
                    json.dump(index, f, ensure_ascii=False)
                
            except Exception as e:
                logger.error(f"Error al limpiar caché JSON: {e}")
        
        # Actualizar estadísticas
        self.stats["cleanups_performed"] += 1
        self.stats["last_cleanup"] = now
        self.stats["items_expired"] += items_removed
        
        logger.info(f"Limpieza de caché completada: {items_removed} elementos eliminados")
        return items_removed
    
    def clear(self) -> bool:
        """
        Elimina todos los elementos de la caché.
        
        Returns:
            True si se limpió correctamente, False en caso contrario
        """
        try:
            # Caché en memoria
            if self.storage_type == "memory":
                self.memory_cache.clear()
            
            # Caché SQLite
            elif self.storage_type == "sqlite":
                conn = sqlite3.connect(str(self.db_path))
                cursor = conn.cursor()
                
                cursor.execute("DELETE FROM cache")
                cursor.execute("VACUUM")
                
                conn.commit()
                conn.close()
            
            # Caché JSON
            elif self.storage_type == "json":
                # Eliminar todos los archivos de caché
                for file_path in self.cache_dir.glob("cache_*.json"):
                    file_path.unlink()
                
                # Reiniciar índice
                with open(self.json_path, 'w', encoding='utf-8') as f:
                    json.dump({
                        "metadata": {
                            "created_at": time.time(),
                            "last_updated": time.time()
                        },
                        "items": {}
                    }, f)
            
            logger.info("Caché completamente limpiada")
            return True
            
        except Exception as e:
            logger.error(f"Error al limpiar completamente la caché: {e}")
            return False
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Obtiene estadísticas de uso de la caché.
        
        Returns:
            Diccionario con estadísticas
        """
        # Actualizar estadísticas con información actual
        current_stats = {
            **self.stats,
            "cache_size_mb": self._get_cache_size(),
            "max_size_mb": self.max_size_mb,
            "storage_type": self.storage_type,
            "ttl_seconds": self.ttl_seconds,
            "item_count": self.count(),
            "hit_ratio": self._calculate_hit_ratio()
        }
        
        return current_stats
    
    def _calculate_hit_ratio(self) -> float:
        """
        Calcula la proporción de aciertos de caché.
        
        Returns:
            Proporción de aciertos (0.0 - 1.0)
        """
        total = self.stats["hits"] + self.stats["misses"]
        if total == 0:
            return 0.0
        return self.stats["hits"] / total
    
    def count(self) -> int:
        """
        Cuenta el número de elementos en la caché.
        
        Returns:
            Número de elementos
        """
        # Caché en memoria
        if self.storage_type == "memory":
            return len(self.memory_cache)
        
        # Caché SQLite
        elif self.storage_type == "sqlite":
            try:
                conn = sqlite3.connect(str(self.db_path))
                cursor = conn.cursor()
                
                cursor.execute("SELECT COUNT(*) FROM cache")
                count = cursor.fetchone()[0]
                
                conn.close()
                return count
                
            except Exception as e:
                logger.error(f"Error al contar elementos en caché SQLite: {e}")
                return 0
        
        # Caché JSON
        elif self.storage_type == "json":
            try:
                if not self.json_path.exists():
                    return 0
                
                with open(self.json_path, 'r', encoding='utf-8') as f:
                    index = json.load(f)
                
                return len(index["items"])
                
            except Exception as e:
                logger.error(f"Error al contar elementos en caché JSON: {e}")
                return 0
    
    def purge_expired(self) -> int:
        """
        Elimina todos los elementos expirados.
        
        Returns:
            Número de elementos eliminados
        """
        return self.cleanup(force=True)
    
    def compact(self) -> bool:
        """
        Compacta el almacenamiento de caché para reducir espacio.
        
        Returns:
            True si se compactó correctamente, False en caso contrario
        """
        try:
            # Caché SQLite
            if self.storage_type == "sqlite":
                conn = sqlite3.connect(str(self.db_path))
                cursor = conn.cursor()
                
                cursor.execute("VACUUM")
                
                conn.commit()
                conn.close()
                
                logger.info("Caché SQLite compactada")
                return True
            
            # Caché JSON
            elif self.storage_type == "json":
                # Primero eliminar elementos expirados
                self.cleanup(force=True)
                
                if not self.json_path.exists():
                    return True
                
                # Cargar índice actual
                with open(self.json_path, 'r', encoding='utf-8') as f:
                    index = json.load(f)
                
                # Crear nuevo directorio temporal
                temp_dir = self.cache_dir / "temp_compact"
                os.makedirs(temp_dir, exist_ok=True)
                
                # Crear nuevo índice
                new_index = {
                    "metadata": {
                        "created_at": index["metadata"]["created_at"],
                        "last_updated": time.time(),
                        "compacted_at": time.time()
                    },
                    "items": {}
                }
                
                # Agrupar elementos por archivo
                files_to_items = {}
                
                for key, item in index["items"].items():
                    file_id = item["file_id"]
                    
                    if file_id not in files_to_items:
                        files_to_items[file_id] = []
                    
                    files_to_items[file_id].append((key, item))
                
                # Procesar cada archivo
                for file_id, items in files_to_items.items():
                    # Cargar archivo original
                    cache_file = self.cache_dir / f"cache_{file_id}.json"
                    
                    if not cache_file.exists():
                        continue
                    
                    with open(cache_file, 'r', encoding='utf-8') as f:
                        cache_data = json.load(f)
                    
                    # Crear nuevo archivo compacto
                    new_file_id = str(int(time.time() * 1000) + len(new_index["items"]))
                    new_cache_file = temp_dir / f"cache_{new_file_id}.json"
                    
                    new_cache_data = {}
                    
                    # Copiar elementos al nuevo archivo
                    for key, item in items:
                        if key in cache_data:
                            new_cache_data[key] = cache_data[key]
                            
                            # Actualizar índice
                            new_index["items"][key] = {
                                **item,
                                "file_id": new_file_id
                            }
                    
                    # Guardar nuevo archivo
                    with open(new_cache_file, 'w', encoding='utf-8') as f:
                        json.dump(new_cache_data, f, ensure_ascii=False)
                
                # Guardar nuevo índice en ubicación temporal
                temp_index_path = temp_dir / "cache_index.json"
                
                with open(temp_index_path, 'w', encoding='utf-8') as f:
                    json.dump(new_index, f, ensure_ascii=False)
                
                # Reemplazar archivos originales con los nuevos
                # Primero hacer backup del índice original
                backup_path = self.json_path.with_suffix('.json.bak')
                if self.json_path.exists():
                    shutil.copy2(self.json_path, backup_path)
                
                # Eliminar archivos originales
                for file_path in self.cache_dir.glob("cache_*.json"):
                    file_path.unlink()
                
                # Mover archivos nuevos
                for file_path in temp_dir.glob("cache_*.json"):
                    shutil.move(str(file_path), str(self.cache_dir))
                
                # Mover nuevo índice
                shutil.move(str(temp_index_path), str(self.json_path))
                
                # Eliminar directorio temporal
                shutil.rmtree(temp_dir)
                
                logger.info("Caché JSON compactada")
                return True
            
            # No es necesario compactar caché en memoria
            elif self.storage_type == "memory":
                return True
                
        except Exception as e:
            logger.error(f"Error al compactar caché: {e}")
            return False
    
    def __del__(self):
        """Método destructor para asegurar limpieza de recursos."""
        try:
            # Guardar estadísticas antes de cerrar
            stats_path = self.cache_dir / "cache_stats.json"
            
            with open(stats_path, 'w', encoding='utf-8') as f:
                json.dump(self.get_stats(), f, ensure_ascii=False, indent=2)
                
        except Exception:
            pass

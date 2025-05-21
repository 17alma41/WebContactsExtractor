"""
Sistema de puntos de control para recuperación de operaciones de scraping.
"""

import os
import json
import time
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional

from src.core.config import DATA_DIR

class CheckpointManager:
    """
    Gestiona puntos de control para el scraping, permitiendo reanudar 
    el proceso en caso de interrupciones.
    """
    
    def __init__(self, checkpoint_dir: Optional[str] = None, job_name: str = None):
        """
        Inicializa el gestor de checkpoints.
        
        Args:
            checkpoint_dir: Directorio donde se guardarán los checkpoints
            job_name: Nombre único para el trabajo actual (ej: nombre del archivo CSV)
        """
        if checkpoint_dir is None:
            checkpoint_dir = str(DATA_DIR / "checkpoints")
            
        if job_name is None:
            job_name = f"job_{int(time.time())}"
            
        self.checkpoint_dir = Path(checkpoint_dir)
        self.job_name = job_name
        self.checkpoint_file = self.checkpoint_dir / f"{job_name}_checkpoint.json"
        self.checkpoint_data = {
            "job_name": job_name,
            "started_at": time.time(),
            "last_updated": time.time(),
            "processed_urls": {},
            "completed_rows": [],
            "failed_rows": {},
            "total_rows": 0,
            "current_position": 0,
            "is_completed": False
        }
        
        # Crear directorio si no existe
        os.makedirs(self.checkpoint_dir, exist_ok=True)
        
        # Cargar checkpoint existente si existe
        self._load_checkpoint()
    
    def _load_checkpoint(self) -> None:
        """Carga el checkpoint desde el archivo si existe."""
        if self.checkpoint_file.exists():
            try:
                with open(self.checkpoint_file, 'r', encoding='utf-8') as f:
                    loaded_data = json.load(f)
                    self.checkpoint_data.update(loaded_data)
                    logging.info(f"Checkpoint cargado: {self.job_name} - "
                                f"Procesadas {len(self.checkpoint_data['completed_rows'])} filas")
            except Exception as e:
                logging.error(f"Error al cargar checkpoint {self.checkpoint_file}: {e}")
                # Crear backup del archivo corrupto
                if self.checkpoint_file.exists():
                    backup_file = self.checkpoint_file.with_suffix('.json.bak')
                    self.checkpoint_file.rename(backup_file)
    
    def save(self) -> None:
        """Guarda el estado actual del checkpoint."""
        self.checkpoint_data["last_updated"] = time.time()
        try:
            # Primero escribir a un archivo temporal
            temp_file = self.checkpoint_file.with_suffix('.json.tmp')
            with open(temp_file, 'w', encoding='utf-8') as f:
                json.dump(self.checkpoint_data, f, ensure_ascii=False, indent=2)
            
            # Luego reemplazar el archivo original (más seguro ante interrupciones)
            if temp_file.exists():
                if self.checkpoint_file.exists():
                    self.checkpoint_file.unlink()
                temp_file.rename(self.checkpoint_file)
        except Exception as e:
            logging.error(f"Error al guardar checkpoint {self.checkpoint_file}: {e}")
    
    def set_total_rows(self, total: int) -> None:
        """Establece el número total de filas a procesar."""
        self.checkpoint_data["total_rows"] = total
        self.save()
    
    def mark_url_processed(self, row_id: int, url: str, success: bool, 
                          result: Optional[Dict] = None, error: Optional[str] = None) -> None:
        """
        Marca una URL como procesada con su resultado.
        
        Args:
            row_id: ID de la fila (índice)
            url: URL procesada
            success: Si el procesamiento fue exitoso
            result: Resultados del scraping (emails, redes, etc.)
            error: Mensaje de error si falló
        """
        self.checkpoint_data["processed_urls"][url] = {
            "row_id": row_id,
            "success": success,
            "timestamp": time.time(),
            "result": result or {},
            "error": error
        }
        
        if success:
            if row_id not in self.checkpoint_data["completed_rows"]:
                self.checkpoint_data["completed_rows"].append(row_id)
                # Eliminar de fallidos si estaba ahí
                if str(row_id) in self.checkpoint_data["failed_rows"]:
                    del self.checkpoint_data["failed_rows"][str(row_id)]
        else:
            self.checkpoint_data["failed_rows"][str(row_id)] = {
                "url": url,
                "error": error,
                "timestamp": time.time()
            }
        
        # Actualizar posición actual
        self.checkpoint_data["current_position"] = max(
            self.checkpoint_data["current_position"], 
            row_id + 1
        )
        
        # Guardar cada 5 URLs procesadas para no sobrecargar I/O
        if len(self.checkpoint_data["processed_urls"]) % 5 == 0:
            self.save()
    
    def is_url_processed(self, url: str) -> bool:
        """Verifica si una URL ya fue procesada."""
        return url in self.checkpoint_data["processed_urls"]
    
    def is_row_completed(self, row_id: int) -> bool:
        """Verifica si una fila ya fue completada exitosamente."""
        return row_id in self.checkpoint_data["completed_rows"]
    
    def get_pending_rows(self, all_row_ids: List[int]) -> List[int]:
        """
        Obtiene las filas pendientes de procesar.
        
        Args:
            all_row_ids: Lista con todos los IDs de filas
            
        Returns:
            Lista de IDs de filas pendientes
        """
        return [row_id for row_id in all_row_ids 
                if row_id not in self.checkpoint_data["completed_rows"]]
    
    def get_failed_rows(self) -> Dict[int, Dict]:
        """Obtiene las filas que fallaron durante el procesamiento."""
        return {int(k): v for k, v in self.checkpoint_data["failed_rows"].items()}
    
    def get_progress(self) -> Dict[str, Any]:
        """Obtiene información sobre el progreso actual."""
        total = self.checkpoint_data["total_rows"]
        completed = len(self.checkpoint_data["completed_rows"])
        failed = len(self.checkpoint_data["failed_rows"])
        pending = total - completed
        
        return {
            "total": total,
            "completed": completed,
            "failed": failed,
            "pending": pending,
            "percent_complete": (completed / total * 100) if total > 0 else 0,
            "started_at": self.checkpoint_data["started_at"],
            "last_updated": self.checkpoint_data["last_updated"],
            "elapsed_seconds": time.time() - self.checkpoint_data["started_at"]
        }
    
    def mark_completed(self) -> None:
        """Marca el trabajo como completado."""
        self.checkpoint_data["is_completed"] = True
        self.save()
    
    def is_completed(self) -> bool:
        """Verifica si el trabajo está marcado como completado."""
        return self.checkpoint_data["is_completed"]

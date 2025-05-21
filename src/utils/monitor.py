"""
Sistema de monitoreo para operaciones de scraping.
Proporciona visualización en tiempo real del progreso y uso de recursos.
"""

import os
import time
import psutil
import logging
import threading
from typing import Dict, Any, Optional, List, Callable
from datetime import datetime
import json
from pathlib import Path

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger("monitor")

# Intentar importar tqdm para barras de progreso
try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False

# Intentar importar componentes del proyecto
try:
    from src.core.config import LOGS_DIR
    from src.core.resource_manager import ResourceManager
    from src.core.checkpoint_manager import CheckpointManager
    from src.core.cache_manager import CacheManager
except ImportError:
    # Fallback si no se pueden importar
    LOGS_DIR = Path(__file__).resolve().parent.parent.parent / "logs"
    ResourceManager = None
    CheckpointManager = None
    CacheManager = None

class ProgressMonitor:
    """
    Monitor de progreso para operaciones de scraping.
    Proporciona información en tiempo real sobre el estado de las operaciones.
    """
    
    def __init__(
        self,
        total_items: int = 0,
        description: str = "Procesando",
        update_interval: float = 1.0,
        log_to_file: bool = True,
        log_file: Optional[str] = None,
        checkpoint_manager: Optional[Any] = None,
        resource_manager: Optional[Any] = None,
        cache_manager: Optional[Any] = None,
        show_eta: bool = True,
        show_resources: bool = True
    ):
        """
        Inicializa el monitor de progreso.
        
        Args:
            total_items: Número total de elementos a procesar
            description: Descripción de la operación
            update_interval: Intervalo de actualización en segundos
            log_to_file: Si se debe registrar el progreso en un archivo
            log_file: Ruta al archivo de log (opcional)
            checkpoint_manager: Instancia de CheckpointManager (opcional)
            resource_manager: Instancia de ResourceManager (opcional)
            cache_manager: Instancia de CacheManager (opcional)
            show_eta: Si se debe mostrar tiempo estimado de finalización
            show_resources: Si se debe mostrar información de recursos
        """
        self.total_items = total_items
        self.description = description
        self.update_interval = update_interval
        self.log_to_file = log_to_file
        self.show_eta = show_eta
        self.show_resources = show_resources
        
        # Inicializar contadores
        self.processed_items = 0
        self.successful_items = 0
        self.failed_items = 0
        self.skipped_items = 0
        
        # Tiempos
        self.start_time = time.time()
        self.last_update_time = self.start_time
        self.last_item_time = self.start_time
        
        # Estado
        self.is_running = False
        self.is_paused = False
        self.stop_requested = False
        
        # Estadísticas
        self.stats = {
            "items_per_second": 0.0,
            "elapsed_time": 0.0,
            "estimated_remaining": 0.0,
            "percent_complete": 0.0,
            "memory_usage_mb": 0.0,
            "cpu_usage_percent": 0.0
        }
        
        # Componentes externos
        self.checkpoint_manager = checkpoint_manager
        self.resource_manager = resource_manager
        self.cache_manager = cache_manager
        
        # Configurar archivo de log
        if log_to_file:
            if log_file is None:
                os.makedirs(LOGS_DIR, exist_ok=True)
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                log_file = LOGS_DIR / f"progress_{timestamp}.log"
            
            self.log_file = Path(log_file)
        else:
            self.log_file = None
        
        # Inicializar barra de progreso
        self.progress_bar = None
        if TQDM_AVAILABLE:
            self.progress_bar = tqdm(
                total=total_items,
                desc=description,
                unit="item"
            )
        
        # Hilo de actualización
        self.update_thread = None
    
    def start(self):
        """Inicia el monitor de progreso."""
        if self.is_running:
            return
        
        self.is_running = True
        self.is_paused = False
        self.stop_requested = False
        self.start_time = time.time()
        self.last_update_time = self.start_time
        
        # Iniciar hilo de actualización
        self.update_thread = threading.Thread(
            target=self._update_loop,
            daemon=True
        )
        self.update_thread.start()
        
        logger.info(f"Monitor de progreso iniciado: {self.description}")
        self._log_status("Iniciado")
    
    def stop(self):
        """Detiene el monitor de progreso."""
        if not self.is_running:
            return
        
        self.stop_requested = True
        
        if self.update_thread:
            self.update_thread.join(timeout=2.0)
        
        self.is_running = False
        
        # Cerrar barra de progreso
        if self.progress_bar:
            self.progress_bar.close()
        
        logger.info(f"Monitor de progreso detenido: {self.description}")
        self._log_status("Detenido")
    
    def pause(self):
        """Pausa el monitor de progreso."""
        if not self.is_running or self.is_paused:
            return
        
        self.is_paused = True
        logger.info(f"Monitor de progreso pausado: {self.description}")
        self._log_status("Pausado")
    
    def resume(self):
        """Reanuda el monitor de progreso."""
        if not self.is_running or not self.is_paused:
            return
        
        self.is_paused = False
        logger.info(f"Monitor de progreso reanudado: {self.description}")
        self._log_status("Reanudado")
    
    def update(self, increment: int = 1, successful: bool = True, skipped: bool = False):
        """
        Actualiza el progreso.
        
        Args:
            increment: Número de elementos procesados
            successful: Si el procesamiento fue exitoso
            skipped: Si el elemento fue omitido
        """
        self.processed_items += increment
        
        if skipped:
            self.skipped_items += increment
        elif successful:
            self.successful_items += increment
        else:
            self.failed_items += increment
        
        # Actualizar barra de progreso
        if self.progress_bar:
            self.progress_bar.update(increment)
        
        # Actualizar tiempo del último elemento
        self.last_item_time = time.time()
    
    def _update_loop(self):
        """Bucle de actualización en segundo plano."""
        while self.is_running and not self.stop_requested:
            if not self.is_paused:
                self._update_stats()
                self._display_progress()
            
            # Esperar hasta la próxima actualización
            time.sleep(self.update_interval)
    
    def _update_stats(self):
        """Actualiza las estadísticas de progreso."""
        current_time = time.time()
        elapsed = current_time - self.start_time
        
        # Calcular velocidad de procesamiento
        if elapsed > 0:
            items_per_second = self.processed_items / elapsed
        else:
            items_per_second = 0
        
        # Calcular tiempo restante estimado
        if items_per_second > 0 and self.total_items > 0:
            remaining_items = self.total_items - self.processed_items
            estimated_remaining = remaining_items / items_per_second
        else:
            estimated_remaining = 0
        
        # Calcular porcentaje completado
        if self.total_items > 0:
            percent_complete = (self.processed_items / self.total_items) * 100
        else:
            percent_complete = 0
        
        # Obtener uso de recursos
        memory_usage_mb = 0
        cpu_usage_percent = 0
        
        if self.show_resources:
            try:
                process = psutil.Process(os.getpid())
                memory_info = process.memory_info()
                memory_usage_mb = memory_info.rss / (1024 * 1024)
                cpu_usage_percent = process.cpu_percent(interval=0.1)
            except Exception:
                pass
        
        # Actualizar estadísticas
        self.stats.update({
            "items_per_second": items_per_second,
            "elapsed_time": elapsed,
            "estimated_remaining": estimated_remaining,
            "percent_complete": percent_complete,
            "memory_usage_mb": memory_usage_mb,
            "cpu_usage_percent": cpu_usage_percent
        })
        
        # Actualizar tiempo de última actualización
        self.last_update_time = current_time
    
    def _display_progress(self):
        """Muestra el progreso actual en la consola."""
        if not self.progress_bar:
            # Formatear mensaje de progreso
            elapsed = time.time() - self.start_time
            
            if self.total_items > 0:
                percent = (self.processed_items / self.total_items) * 100
                progress_msg = f"{self.description}: {self.processed_items}/{self.total_items} ({percent:.1f}%)"
            else:
                progress_msg = f"{self.description}: {self.processed_items} elementos"
            
            # Añadir información de tiempo
            time_msg = f"Tiempo: {self._format_time(elapsed)}"
            
            if self.show_eta and self.stats["estimated_remaining"] > 0:
                time_msg += f", ETA: {self._format_time(self.stats['estimated_remaining'])}"
            
            # Añadir información de recursos
            if self.show_resources:
                resource_msg = f"Memoria: {self.stats['memory_usage_mb']:.1f} MB, CPU: {self.stats['cpu_usage_percent']:.1f}%"
            else:
                resource_msg = ""
            
            # Mostrar mensaje completo
            logger.info(f"{progress_msg} | {time_msg} | {resource_msg}")
        
        # Registrar en archivo si está habilitado
        if self.log_to_file and self.log_file:
            self._log_progress()
    
    def _log_progress(self):
        """Registra el progreso actual en el archivo de log."""
        if not self.log_file:
            return
        
        try:
            # Crear directorio si no existe
            os.makedirs(self.log_file.parent, exist_ok=True)
            
            # Preparar datos para el log
            log_data = {
                "timestamp": time.time(),
                "description": self.description,
                "total_items": self.total_items,
                "processed_items": self.processed_items,
                "successful_items": self.successful_items,
                "failed_items": self.failed_items,
                "skipped_items": self.skipped_items,
                **self.stats
            }
            
            # Añadir datos de checkpoint si está disponible
            if self.checkpoint_manager:
                try:
                    checkpoint_progress = self.checkpoint_manager.get_progress()
                    log_data["checkpoint"] = checkpoint_progress
                except Exception:
                    pass
            
            # Añadir datos de caché si está disponible
            if self.cache_manager:
                try:
                    cache_stats = self.cache_manager.get_stats()
                    log_data["cache"] = cache_stats
                except Exception:
                    pass
            
            # Escribir en archivo
            with open(self.log_file, 'a', encoding='utf-8') as f:
                f.write(json.dumps(log_data) + "\n")
                
        except Exception as e:
            logger.error(f"Error al registrar progreso en archivo: {e}")
    
    def _log_status(self, status: str):
        """Registra un cambio de estado en el archivo de log."""
        if not self.log_to_file or not self.log_file:
            return
        
        try:
            # Crear directorio si no existe
            os.makedirs(self.log_file.parent, exist_ok=True)
            
            # Preparar datos para el log
            log_data = {
                "timestamp": time.time(),
                "description": self.description,
                "status": status,
                "processed_items": self.processed_items,
                "total_items": self.total_items
            }
            
            # Escribir en archivo
            with open(self.log_file, 'a', encoding='utf-8') as f:
                f.write(json.dumps(log_data) + "\n")
                
        except Exception as e:
            logger.error(f"Error al registrar estado en archivo: {e}")
    
    def _format_time(self, seconds: float) -> str:
        """
        Formatea un tiempo en segundos a formato legible.
        
        Args:
            seconds: Tiempo en segundos
            
        Returns:
            Tiempo formateado
        """
        if seconds < 60:
            return f"{seconds:.1f}s"
        elif seconds < 3600:
            minutes = seconds / 60
            return f"{minutes:.1f}m"
        else:
            hours = seconds / 3600
            return f"{hours:.1f}h"
    
    def get_summary(self) -> Dict[str, Any]:
        """
        Obtiene un resumen del progreso actual.
        
        Returns:
            Diccionario con resumen de progreso
        """
        # Actualizar estadísticas
        self._update_stats()
        
        return {
            "description": self.description,
            "total_items": self.total_items,
            "processed_items": self.processed_items,
            "successful_items": self.successful_items,
            "failed_items": self.failed_items,
            "skipped_items": self.skipped_items,
            "percent_complete": self.stats["percent_complete"],
            "elapsed_time": self.stats["elapsed_time"],
            "estimated_remaining": self.stats["estimated_remaining"],
            "items_per_second": self.stats["items_per_second"],
            "start_time": self.start_time,
            "is_running": self.is_running,
            "is_paused": self.is_paused
        }
    
    def __enter__(self):
        """Soporte para uso con 'with'."""
        self.start()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Limpieza al salir del bloque 'with'."""
        self.stop()
    
    def __del__(self):
        """Método destructor para asegurar limpieza de recursos."""
        try:
            if self.is_running:
                self.stop()
                
        except Exception:
            pass


class SystemMonitor:
    """
    Monitor del sistema para supervisar recursos durante operaciones de scraping.
    """
    
    def __init__(
        self,
        check_interval: float = 5.0,
        log_to_file: bool = True,
        log_file: Optional[str] = None,
        alert_threshold_memory: float = 80.0,
        alert_threshold_cpu: float = 70.0
    ):
        """
        Inicializa el monitor del sistema.
        
        Args:
            check_interval: Intervalo de verificación en segundos
            log_to_file: Si se debe registrar en archivo
            log_file: Ruta al archivo de log (opcional)
            alert_threshold_memory: Umbral de alerta para memoria (%)
            alert_threshold_cpu: Umbral de alerta para CPU (%)
        """
        self.check_interval = check_interval
        self.log_to_file = log_to_file
        self.alert_threshold_memory = alert_threshold_memory
        self.alert_threshold_cpu = alert_threshold_cpu
        
        # Estado
        self.is_running = False
        self.stop_requested = False
        
        # Estadísticas
        self.stats = {
            "memory": {},
            "cpu": {},
            "disk": {},
            "network": {},
            "process": {},
            "alerts": []
        }
        
        # Configurar archivo de log
        if log_to_file:
            if log_file is None:
                os.makedirs(LOGS_DIR, exist_ok=True)
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                log_file = LOGS_DIR / f"system_monitor_{timestamp}.log"
            
            self.log_file = Path(log_file)
        else:
            self.log_file = None
        
        # Hilo de monitoreo
        self.monitor_thread = None
    
    def start(self):
        """Inicia el monitor del sistema."""
        if self.is_running:
            return
        
        self.is_running = True
        self.stop_requested = False
        
        # Iniciar hilo de monitoreo
        self.monitor_thread = threading.Thread(
            target=self._monitor_loop,
            daemon=True
        )
        self.monitor_thread.start()
        
        logger.info("Monitor del sistema iniciado")
    
    def stop(self):
        """Detiene el monitor del sistema."""
        if not self.is_running:
            return
        
        self.stop_requested = True
        
        if self.monitor_thread:
            self.monitor_thread.join(timeout=2.0)
        
        self.is_running = False
        logger.info("Monitor del sistema detenido")
    
    def _monitor_loop(self):
        """Bucle de monitoreo en segundo plano."""
        while self.is_running and not self.stop_requested:
            try:
                # Recopilar estadísticas
                self._collect_stats()
                
                # Verificar alertas
                self._check_alerts()
                
                # Registrar en archivo
                if self.log_to_file:
                    self._log_stats()
                
            except Exception as e:
                logger.error(f"Error en monitoreo del sistema: {e}")
            
            # Esperar hasta la próxima verificación
            time.sleep(self.check_interval)
    
    def _collect_stats(self):
        """Recopila estadísticas del sistema."""
        # Memoria
        memory = psutil.virtual_memory()
        self.stats["memory"] = {
            "total_mb": memory.total / (1024 * 1024),
            "available_mb": memory.available / (1024 * 1024),
            "used_mb": memory.used / (1024 * 1024),
            "percent": memory.percent
        }
        
        # CPU
        self.stats["cpu"] = {
            "percent": psutil.cpu_percent(interval=0.1),
            "count": psutil.cpu_count(),
            "count_logical": psutil.cpu_count(logical=True)
        }
        
        # Disco
        disk = psutil.disk_usage('/')
        self.stats["disk"] = {
            "total_gb": disk.total / (1024 * 1024 * 1024),
            "used_gb": disk.used / (1024 * 1024 * 1024),
            "free_gb": disk.free / (1024 * 1024 * 1024),
            "percent": disk.percent
        }
        
        # Proceso actual
        process = psutil.Process(os.getpid())
        self.stats["process"] = {
            "memory_mb": process.memory_info().rss / (1024 * 1024),
            "cpu_percent": process.cpu_percent(interval=0.1),
            "threads": process.num_threads(),
            "created_time": process.create_time()
        }
        
        # Red (solo estadísticas básicas)
        try:
            net_io = psutil.net_io_counters()
            self.stats["network"] = {
                "bytes_sent_mb": net_io.bytes_sent / (1024 * 1024),
                "bytes_recv_mb": net_io.bytes_recv / (1024 * 1024),
                "packets_sent": net_io.packets_sent,
                "packets_recv": net_io.packets_recv
            }
        except Exception:
            self.stats["network"] = {}
    
    def _check_alerts(self):
        """Verifica si hay condiciones de alerta."""
        alerts = []
        
        # Alerta de memoria
        if self.stats["memory"]["percent"] > self.alert_threshold_memory:
            alert = {
                "type": "memory",
                "level": "warning",
                "message": f"Uso de memoria alto: {self.stats['memory']['percent']:.1f}%",
                "timestamp": time.time()
            }
            alerts.append(alert)
            logger.warning(alert["message"])
        
        # Alerta de CPU
        if self.stats["cpu"]["percent"] > self.alert_threshold_cpu:
            alert = {
                "type": "cpu",
                "level": "warning",
                "message": f"Uso de CPU alto: {self.stats['cpu']['percent']:.1f}%",
                "timestamp": time.time()
            }
            alerts.append(alert)
            logger.warning(alert["message"])
        
        # Alerta de disco
        if self.stats["disk"]["percent"] > 90:
            alert = {
                "type": "disk",
                "level": "warning",
                "message": f"Espacio en disco bajo: {self.stats['disk']['percent']:.1f}%",
                "timestamp": time.time()
            }
            alerts.append(alert)
            logger.warning(alert["message"])
        
        # Actualizar lista de alertas
        self.stats["alerts"] = alerts
    
    def _log_stats(self):
        """Registra estadísticas en el archivo de log."""
        if not self.log_file:
            return
        
        try:
            # Crear directorio si no existe
            os.makedirs(self.log_file.parent, exist_ok=True)
            
            # Preparar datos para el log
            log_data = {
                "timestamp": time.time(),
                **self.stats
            }
            
            # Escribir en archivo
            with open(self.log_file, 'a', encoding='utf-8') as f:
                f.write(json.dumps(log_data) + "\n")
                
        except Exception as e:
            logger.error(f"Error al registrar estadísticas en archivo: {e}")
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Obtiene las estadísticas actuales del sistema.
        
        Returns:
            Diccionario con estadísticas
        """
        # Actualizar estadísticas
        self._collect_stats()
        
        return self.stats
    
    def __enter__(self):
        """Soporte para uso con 'with'."""
        self.start()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Limpieza al salir del bloque 'with'."""
        self.stop()
    
    def __del__(self):
        """Método destructor para asegurar limpieza de recursos."""
        try:
            if self.is_running:
                self.stop()
                
        except Exception:
            pass


# Función de utilidad para monitorear una operación
def monitor_operation(
    operation_func: Callable,
    total_items: int,
    description: str = "Procesando",
    show_progress: bool = True,
    monitor_system: bool = True,
    *args, **kwargs
) -> Any:
    """
    Monitorea una operación y muestra su progreso.
    
    Args:
        operation_func: Función a ejecutar
        total_items: Número total de elementos
        description: Descripción de la operación
        show_progress: Si se debe mostrar barra de progreso
        monitor_system: Si se debe monitorear el sistema
        *args, **kwargs: Argumentos para la función
        
    Returns:
        Resultado de la función
    """
    result = None
    
    # Crear monitores
    progress_monitor = None
    system_monitor = None
    
    try:
        # Iniciar monitor de progreso
        if show_progress:
            progress_monitor = ProgressMonitor(
                total_items=total_items,
                description=description
            )
            progress_monitor.start()
        
        # Iniciar monitor del sistema
        if monitor_system:
            system_monitor = SystemMonitor()
            system_monitor.start()
        
        # Ejecutar operación
        start_time = time.time()
        result = operation_func(*args, **kwargs)
        elapsed = time.time() - start_time
        
        # Mostrar resumen
        logger.info(f"Operación completada en {elapsed:.2f} segundos")
        
        if progress_monitor:
            summary = progress_monitor.get_summary()
            logger.info(
                f"Resumen: {summary['processed_items']}/{summary['total_items']} "
                f"({summary['percent_complete']:.1f}%) - "
                f"Exitosos: {summary['successful_items']}, "
                f"Fallidos: {summary['failed_items']}, "
                f"Omitidos: {summary['skipped_items']}"
            )
        
    finally:
        # Detener monitores
        if progress_monitor:
            progress_monitor.stop()
        
        if system_monitor:
            system_monitor.stop()
    
    return result

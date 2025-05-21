"""
Sistema de gestión de recursos para optimizar el uso de memoria y CPU.
Proporciona mecanismos para controlar el consumo de recursos durante
operaciones intensivas de scraping.
"""

import os
import time
import psutil
import logging
import threading
from typing import Dict, Any, Optional, Callable, List
from functools import wraps

logger = logging.getLogger("resource_manager")

class ResourceManager:
    """
    Gestor de recursos del sistema para operaciones de scraping.
    Permite monitorear y controlar el uso de CPU, memoria y otros recursos
    para evitar sobrecargas del sistema durante operaciones intensivas.
    """
    
    def __init__(
        self,
        max_memory_percent: float = 80.0,
        max_cpu_percent: float = 70.0,
        check_interval_seconds: float = 5.0,
        enable_monitoring: bool = True
    ):
        """
        Inicializa el gestor de recursos.
        
        Args:
            max_memory_percent: Porcentaje máximo de memoria a utilizar
            max_cpu_percent: Porcentaje máximo de CPU a utilizar
            check_interval_seconds: Intervalo de verificación de recursos
            enable_monitoring: Si se debe activar el monitoreo automático
        """
        self.max_memory_percent = max_memory_percent
        self.max_cpu_percent = max_cpu_percent
        self.check_interval_seconds = check_interval_seconds
        
        self.process = psutil.Process(os.getpid())
        self.monitoring_thread = None
        self.stop_monitoring = threading.Event()
        
        self.stats = {
            "memory_warnings": 0,
            "cpu_warnings": 0,
            "throttling_events": 0,
            "peak_memory_percent": 0.0,
            "peak_cpu_percent": 0.0,
            "last_check": time.time(),
            "current_memory_percent": 0.0,
            "current_cpu_percent": 0.0
        }
        
        # Iniciar monitoreo si está habilitado
        if enable_monitoring:
            self.start_monitoring()
    
    def get_memory_usage(self) -> Dict[str, float]:
        """
        Obtiene información sobre el uso actual de memoria.
        
        Returns:
            Diccionario con información de uso de memoria
        """
        # Información del proceso actual
        process_memory = self.process.memory_info()
        
        # Información del sistema
        system_memory = psutil.virtual_memory()
        
        # Calcular porcentajes
        process_percent = process_memory.rss / system_memory.total * 100
        
        return {
            "process_rss_mb": process_memory.rss / (1024 * 1024),
            "process_vms_mb": process_memory.vms / (1024 * 1024),
            "process_percent": process_percent,
            "system_total_mb": system_memory.total / (1024 * 1024),
            "system_available_mb": system_memory.available / (1024 * 1024),
            "system_used_percent": system_memory.percent
        }
    
    def get_cpu_usage(self) -> Dict[str, float]:
        """
        Obtiene información sobre el uso actual de CPU.
        
        Returns:
            Diccionario con información de uso de CPU
        """
        # Uso de CPU del proceso actual (porcentaje)
        process_cpu = self.process.cpu_percent(interval=0.1)
        
        # Uso de CPU del sistema (porcentaje por núcleo)
        system_cpu = psutil.cpu_percent(interval=0.1)
        
        # Información de carga del sistema
        try:
            load_avg = os.getloadavg()
        except (AttributeError, OSError):
            # Windows no soporta getloadavg
            load_avg = (0, 0, 0)
        
        return {
            "process_cpu_percent": process_cpu,
            "system_cpu_percent": system_cpu,
            "load_avg_1min": load_avg[0],
            "load_avg_5min": load_avg[1],
            "load_avg_15min": load_avg[2],
            "cpu_count": psutil.cpu_count()
        }
    
    def check_resources(self) -> Dict[str, Any]:
        """
        Verifica el estado actual de los recursos del sistema.
        
        Returns:
            Diccionario con estado de recursos y advertencias
        """
        memory_info = self.get_memory_usage()
        cpu_info = self.get_cpu_usage()
        
        # Actualizar estadísticas
        self.stats["current_memory_percent"] = memory_info["process_percent"]
        self.stats["current_cpu_percent"] = cpu_info["process_cpu_percent"]
        self.stats["last_check"] = time.time()
        
        # Actualizar picos
        self.stats["peak_memory_percent"] = max(
            self.stats["peak_memory_percent"],
            memory_info["process_percent"]
        )
        self.stats["peak_cpu_percent"] = max(
            self.stats["peak_cpu_percent"],
            cpu_info["process_cpu_percent"]
        )
        
        # Verificar límites
        memory_warning = memory_info["process_percent"] > self.max_memory_percent
        cpu_warning = cpu_info["process_cpu_percent"] > self.max_cpu_percent
        
        # Incrementar contadores si hay advertencias
        if memory_warning:
            self.stats["memory_warnings"] += 1
        
        if cpu_warning:
            self.stats["cpu_warnings"] += 1
        
        # Determinar si es necesario throttling
        throttling_needed = memory_warning or cpu_warning
        
        if throttling_needed:
            self.stats["throttling_events"] += 1
        
        return {
            "memory": memory_info,
            "cpu": cpu_info,
            "memory_warning": memory_warning,
            "cpu_warning": cpu_warning,
            "throttling_needed": throttling_needed,
            "timestamp": time.time()
        }
    
    def _monitoring_loop(self):
        """Bucle de monitoreo de recursos en segundo plano."""
        logger.info("Iniciando monitoreo de recursos en segundo plano")
        
        while not self.stop_monitoring.is_set():
            try:
                # Verificar recursos
                status = self.check_resources()
                
                # Registrar advertencias
                if status["memory_warning"]:
                    logger.warning(
                        f"Advertencia de memoria: {status['memory']['process_percent']:.1f}% "
                        f"(límite: {self.max_memory_percent:.1f}%)"
                    )
                
                if status["cpu_warning"]:
                    logger.warning(
                        f"Advertencia de CPU: {status['cpu']['process_cpu_percent']:.1f}% "
                        f"(límite: {self.max_cpu_percent:.1f}%)"
                    )
                
                # Esperar hasta la próxima verificación
                self.stop_monitoring.wait(self.check_interval_seconds)
                
            except Exception as e:
                logger.error(f"Error en monitoreo de recursos: {e}")
                # Continuar el bucle a pesar del error
                time.sleep(self.check_interval_seconds)
        
        logger.info("Monitoreo de recursos detenido")
    
    def start_monitoring(self) -> bool:
        """
        Inicia el monitoreo de recursos en segundo plano.
        
        Returns:
            True si se inició correctamente, False si ya estaba en ejecución
        """
        if self.monitoring_thread and self.monitoring_thread.is_alive():
            logger.warning("El monitoreo de recursos ya está en ejecución")
            return False
        
        # Reiniciar evento de parada
        self.stop_monitoring.clear()
        
        # Crear y comenzar hilo de monitoreo
        self.monitoring_thread = threading.Thread(
            target=self._monitoring_loop,
            daemon=True  # El hilo se cerrará cuando el programa principal termine
        )
        self.monitoring_thread.start()
        
        logger.info("Monitoreo de recursos iniciado")
        return True
    
    def stop_monitoring(self) -> bool:
        """
        Detiene el monitoreo de recursos en segundo plano.
        
        Returns:
            True si se detuvo correctamente, False si no estaba en ejecución
        """
        if not self.monitoring_thread or not self.monitoring_thread.is_alive():
            logger.warning("El monitoreo de recursos no está en ejecución")
            return False
        
        # Señalizar parada
        self.stop_monitoring.set()
        
        # Esperar a que el hilo termine (con timeout)
        self.monitoring_thread.join(timeout=2.0)
        
        logger.info("Monitoreo de recursos detenido")
        return True
    
    def throttle_if_needed(self, sleep_time: float = 1.0) -> bool:
        """
        Reduce la velocidad de ejecución si los recursos están sobrecargados.
        
        Args:
            sleep_time: Tiempo de espera en segundos si es necesario throttling
            
        Returns:
            True si se aplicó throttling, False en caso contrario
        """
        status = self.check_resources()
        
        if status["throttling_needed"]:
            logger.info(
                f"Aplicando throttling por {sleep_time}s - "
                f"Memoria: {status['memory']['process_percent']:.1f}%, "
                f"CPU: {status['cpu']['process_cpu_percent']:.1f}%"
            )
            time.sleep(sleep_time)
            return True
        
        return False
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Obtiene estadísticas de uso de recursos.
        
        Returns:
            Diccionario con estadísticas
        """
        # Actualizar con información actual
        self.check_resources()
        
        return {
            **self.stats,
            "max_memory_percent": self.max_memory_percent,
            "max_cpu_percent": self.max_cpu_percent,
            "check_interval_seconds": self.check_interval_seconds,
            "monitoring_active": (
                self.monitoring_thread is not None and 
                self.monitoring_thread.is_alive()
            )
        }
    
    def optimize_memory(self, aggressive: bool = False) -> Dict[str, Any]:
        """
        Intenta optimizar el uso de memoria liberando caché y forzando GC.
        
        Args:
            aggressive: Si se debe realizar una optimización más agresiva
            
        Returns:
            Diccionario con información de la optimización
        """
        import gc
        
        # Información antes de optimizar
        before = self.get_memory_usage()
        
        # Forzar recolección de basura
        gc.collect()
        
        if aggressive:
            # Más ciclos de GC en modo agresivo
            for _ in range(3):
                gc.collect()
        
        # Información después de optimizar
        after = self.get_memory_usage()
        
        # Calcular diferencia
        memory_diff = before["process_rss_mb"] - after["process_rss_mb"]
        
        return {
            "before_mb": before["process_rss_mb"],
            "after_mb": after["process_rss_mb"],
            "diff_mb": memory_diff,
            "percent_reduction": (memory_diff / before["process_rss_mb"] * 100) if before["process_rss_mb"] > 0 else 0,
            "aggressive_mode": aggressive,
            "timestamp": time.time()
        }
    
    def __del__(self):
        """Método destructor para asegurar limpieza de recursos."""
        try:
            # Detener monitoreo si está activo
            if hasattr(self, 'monitoring_thread') and self.monitoring_thread and self.monitoring_thread.is_alive():
                self.stop_monitoring.set()
                
        except Exception:
            pass


# Decoradores útiles para gestión de recursos

def throttle_on_high_usage(
    resource_manager: Optional[ResourceManager] = None,
    check_interval: int = 10,
    sleep_time: float = 1.0
):
    """
    Decorador que aplica throttling en funciones que consumen muchos recursos.
    
    Args:
        resource_manager: Instancia de ResourceManager (se crea una nueva si es None)
        check_interval: Cada cuántas llamadas verificar recursos
        sleep_time: Tiempo de espera si es necesario throttling
    """
    # Contador de llamadas compartido entre todas las funciones decoradas
    call_counter = {"count": 0}
    
    # Crear resource manager si no se proporciona
    if resource_manager is None:
        resource_manager = ResourceManager(enable_monitoring=False)
    
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Incrementar contador
            call_counter["count"] += 1
            
            # Verificar recursos cada check_interval llamadas
            if call_counter["count"] % check_interval == 0:
                resource_manager.throttle_if_needed(sleep_time)
            
            # Ejecutar función
            return func(*args, **kwargs)
        
        return wrapper
    
    return decorator


def optimize_memory_after_execution(
    resource_manager: Optional[ResourceManager] = None,
    aggressive: bool = False
):
    """
    Decorador que optimiza memoria después de ejecutar una función.
    
    Args:
        resource_manager: Instancia de ResourceManager (se crea una nueva si es None)
        aggressive: Si se debe realizar una optimización más agresiva
    """
    # Crear resource manager si no se proporciona
    if resource_manager is None:
        resource_manager = ResourceManager(enable_monitoring=False)
    
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Ejecutar función
            result = func(*args, **kwargs)
            
            # Optimizar memoria
            resource_manager.optimize_memory(aggressive=aggressive)
            
            return result
        
        return wrapper
    
    return decorator


def batch_process(batch_size: int = 10):
    """
    Decorador para procesar listas en lotes para reducir uso de memoria.
    La función decorada debe recibir una lista como primer argumento.
    
    Args:
        batch_size: Tamaño del lote
    """
    def decorator(func):
        @wraps(func)
        def wrapper(items, *args, **kwargs):
            results = []
            
            # Procesar en lotes
            for i in range(0, len(items), batch_size):
                batch = items[i:i + batch_size]
                batch_result = func(batch, *args, **kwargs)
                
                # Acumular resultados
                if isinstance(batch_result, list):
                    results.extend(batch_result)
                else:
                    results.append(batch_result)
            
            return results
        
        return wrapper
    
    return decorator

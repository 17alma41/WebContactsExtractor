"""
Sistema centralizado de gestión de errores para operaciones de scraping.
"""

import logging
import time
import traceback
from typing import Dict, Any, Callable, Optional, List, Tuple
from functools import wraps
from selenium.common.exceptions import (
    WebDriverException, TimeoutException, NoSuchElementException,
    StaleElementReferenceException, ElementNotInteractableException
)

from src.core.config import LOG_DIR

class ErrorHandler:
    """
    Gestor centralizado de errores para operaciones de scraping.
    Proporciona mecanismos para:
    - Reintentar operaciones fallidas
    - Registrar errores detallados
    - Clasificar errores por tipo y gravedad
    """
    
    # Clasificación de errores
    NETWORK_ERRORS = (
        "ERR_CONNECTION_REFUSED", "ERR_NAME_NOT_RESOLVED",
        "ERR_CONNECTION_RESET", "ERR_CONNECTION_ABORTED",
        "ERR_CONNECTION_FAILED", "ERR_INTERNET_DISCONNECTED",
        "ERR_SSL_PROTOCOL_ERROR", "ERR_TIMED_OUT"
    )
    
    def __init__(self, log_file: Optional[str] = None):
        """
        Inicializa el gestor de errores.
        
        Args:
            log_file: Ruta al archivo de log específico para errores
        """
        self.logger = logging.getLogger("error_handler")
        
        # Configurar logger si no está configurado
        if not self.logger.handlers:
            if not log_file:
                log_file = str(LOG_DIR / "scraping_errors.log")
                
            handler = logging.FileHandler(log_file)
            formatter = logging.Formatter(
                '%(asctime)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)
        
        self.error_stats: Dict[str, int] = {}
        self.recent_errors: List[Dict[str, Any]] = []
        self.max_recent_errors = 100  # Máximo número de errores recientes a almacenar
    
    def log_error(self, error: Exception, context: Dict[str, Any]) -> None:
        """
        Registra un error con contexto detallado.
        
        Args:
            error: La excepción capturada
            context: Información contextual (URL, operación, etc.)
        """
        error_type = type(error).__name__
        error_msg = str(error)
        
        # Incrementar contador para este tipo de error
        if error_type not in self.error_stats:
            self.error_stats[error_type] = 0
        self.error_stats[error_type] += 1
        
        # Registrar error con detalles
        error_details = {
            "type": error_type,
            "message": error_msg,
            "context": context,
            "timestamp": time.time(),
            "traceback": traceback.format_exc()
        }
        
        # Añadir a errores recientes (limitando tamaño)
        self.recent_errors.append(error_details)
        if len(self.recent_errors) > self.max_recent_errors:
            self.recent_errors.pop(0)
        
        # Determinar nivel de gravedad
        if isinstance(error, (TimeoutException, ConnectionError)) or any(
            net_err in error_msg for net_err in self.NETWORK_ERRORS
        ):
            log_level = logging.WARNING
            category = "RED"
        elif isinstance(error, (NoSuchElementException, StaleElementReferenceException)):
            log_level = logging.INFO
            category = "DOM"
        else:
            log_level = logging.ERROR
            category = "GENERAL"
        
        # Construir mensaje de log
        log_message = (
            f"[{category}] {error_type}: {error_msg} | "
            f"URL: {context.get('url', 'N/A')} | "
            f"Operación: {context.get('operation', 'N/A')}"
        )
        
        # Registrar en el logger
        self.logger.log(log_level, log_message)
    
    def is_retriable_error(self, error: Exception) -> bool:
        """
        Determina si un error puede ser reintentado.
        
        Args:
            error: La excepción a evaluar
            
        Returns:
            True si el error es temporal y se puede reintentar
        """
        # Errores de red son generalmente reintentables
        if isinstance(error, (TimeoutException, ConnectionError)):
            return True
        
        # Errores de Selenium específicos que pueden ser temporales
        if isinstance(error, (WebDriverException, StaleElementReferenceException)):
            error_msg = str(error).lower()
            return any(term in error_msg for term in [
                "timeout", "connection", "network", "refused",
                "reset", "aborted", "failed", "disconnected"
            ])
        
        return False
    
    def with_retry(self, max_retries: int = 3, delay: float = 2.0, 
                  backoff_factor: float = 2.0, exceptions_to_retry: Tuple = None):
        """
        Decorador para reintentar funciones que pueden fallar temporalmente.
        
        Args:
            max_retries: Número máximo de reintentos
            delay: Retraso inicial entre reintentos (segundos)
            backoff_factor: Factor de incremento del retraso
            exceptions_to_retry: Tupla de excepciones a reintentar
            
        Returns:
            Decorador configurado
        """
        if exceptions_to_retry is None:
            exceptions_to_retry = (Exception,)
            
        def decorator(func: Callable):
            @wraps(func)
            def wrapper(*args, **kwargs):
                last_exception = None
                current_delay = delay
                
                # Extraer URL del contexto si está disponible
                url = kwargs.get('url', 'unknown')
                if not url and args and isinstance(args[0], str):
                    url = args[0]
                
                for attempt in range(max_retries + 1):
                    try:
                        return func(*args, **kwargs)
                    except exceptions_to_retry as e:
                        last_exception = e
                        
                        # Solo reintentar si no es el último intento
                        if attempt < max_retries:
                            # Verificar si el error es reintentable
                            if not self.is_retriable_error(e):
                                # Si no es reintentable, registrar y propagar
                                self.log_error(e, {
                                    "url": url,
                                    "operation": func.__name__,
                                    "attempt": attempt + 1,
                                    "max_retries": max_retries,
                                    "reintentable": False
                                })
                                raise
                            
                            # Registrar el reintento
                            self.logger.warning(
                                f"Reintentando {func.__name__} para {url} "
                                f"(intento {attempt + 1}/{max_retries}): {str(e)}"
                            )
                            
                            # Esperar antes de reintentar con backoff exponencial
                            time.sleep(current_delay)
                            current_delay *= backoff_factor
                        else:
                            # Registrar el error final después de todos los reintentos
                            self.log_error(e, {
                                "url": url,
                                "operation": func.__name__,
                                "attempt": attempt + 1,
                                "max_retries": max_retries,
                                "reintentable": True,
                                "reintentos_agotados": True
                            })
                
                # Si llegamos aquí, todos los reintentos fallaron
                if last_exception:
                    raise last_exception
                
            return wrapper
        return decorator
    
    def get_error_summary(self) -> Dict[str, Any]:
        """
        Genera un resumen de los errores registrados.
        
        Returns:
            Diccionario con estadísticas de errores
        """
        return {
            "total_errors": sum(self.error_stats.values()),
            "error_types": self.error_stats,
            "recent_errors_count": len(self.recent_errors),
            "most_common_error": max(self.error_stats.items(), 
                                    key=lambda x: x[1])[0] if self.error_stats else None
        }

"""
Script de limpieza y optimización del proyecto.
Permite eliminar archivos temporales, limpiar cachés y optimizar recursos.
"""

import os
import shutil
import logging
import argparse
import time
from pathlib import Path
from typing import List, Dict, Any, Tuple, Set

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger("cleanup")

# Importar rutas de configuración
try:
    from src.core.config import (
        DATA_DIR, LOGS_DIR, TEMP_DIR, 
        OUTPUT_DIR, PROJECT_ROOT
    )
except ImportError:
    # Fallback si no se puede importar config
    PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
    DATA_DIR = PROJECT_ROOT / "data"
    LOGS_DIR = PROJECT_ROOT / "logs"
    TEMP_DIR = PROJECT_ROOT / "temp"
    OUTPUT_DIR = PROJECT_ROOT / "output"

# Tipos de archivos temporales a limpiar
TEMP_FILE_PATTERNS = [
    "*.tmp", "*.temp", "*.bak", "*.old", 
    "*.log.*", "*.pyc", "__pycache__",
    "*.swp", "*.swo", "~*", "*.cache"
]

# Directorios que pueden contener archivos temporales
CLEANUP_DIRS = [
    TEMP_DIR,
    LOGS_DIR,
    DATA_DIR / "cache",
    DATA_DIR / "temp",
    PROJECT_ROOT / "__pycache__",
]

# Archivos a preservar siempre
PRESERVE_FILES = [
    "README.md",
    "requirements.txt",
    ".gitignore",
    "setup.py",
    "config.json",
    "config.yaml",
]

def find_temp_files(
    directory: Path, 
    patterns: List[str] = TEMP_FILE_PATTERNS,
    max_age_days: float = None,
    recursive: bool = True
) -> List[Path]:
    """
    Encuentra archivos temporales en un directorio.
    
    Args:
        directory: Directorio a buscar
        patterns: Patrones de archivos a buscar
        max_age_days: Edad máxima en días (None = cualquier edad)
        recursive: Si se debe buscar recursivamente
        
    Returns:
        Lista de rutas de archivos encontrados
    """
    if not directory.exists():
        return []
    
    temp_files = []
    now = time.time()
    
    # Función para verificar si un archivo coincide con los patrones
    def matches_pattern(file_path: Path) -> bool:
        return any(file_path.match(pattern) for pattern in patterns)
    
    # Función para verificar si un archivo es más antiguo que max_age_days
    def is_old_enough(file_path: Path) -> bool:
        if max_age_days is None:
            return True
        
        try:
            mtime = file_path.stat().st_mtime
            age_days = (now - mtime) / (60 * 60 * 24)
            return age_days >= max_age_days
        except (FileNotFoundError, PermissionError):
            return False
    
    # Buscar archivos
    if recursive:
        for root, dirs, files in os.walk(directory):
            root_path = Path(root)
            
            # Verificar archivos
            for file in files:
                file_path = root_path / file
                if matches_pattern(file_path) and is_old_enough(file_path):
                    temp_files.append(file_path)
            
            # Verificar directorios (para __pycache__ y similares)
            for dir_name in dirs:
                dir_path = root_path / dir_name
                if matches_pattern(dir_path) and is_old_enough(dir_path):
                    temp_files.append(dir_path)
    else:
        # Solo buscar en el directorio actual (no recursivo)
        for item in directory.iterdir():
            if matches_pattern(item) and is_old_enough(item):
                temp_files.append(item)
    
    return temp_files

def delete_files(files: List[Path], dry_run: bool = False) -> Tuple[int, int, List[str]]:
    """
    Elimina archivos de la lista proporcionada.
    
    Args:
        files: Lista de archivos a eliminar
        dry_run: Si es True, solo simula la eliminación
        
    Returns:
        Tupla con (archivos eliminados, errores, lista de errores)
    """
    deleted = 0
    errors = 0
    error_messages = []
    
    for file_path in files:
        try:
            if file_path.name in PRESERVE_FILES:
                logger.info(f"Preservando archivo: {file_path}")
                continue
                
            if dry_run:
                logger.info(f"[SIMULACIÓN] Eliminaría: {file_path}")
                deleted += 1
            else:
                if file_path.is_dir():
                    logger.info(f"Eliminando directorio: {file_path}")
                    shutil.rmtree(file_path, ignore_errors=True)
                else:
                    logger.info(f"Eliminando archivo: {file_path}")
                    file_path.unlink()
                deleted += 1
        except Exception as e:
            errors += 1
            error_msg = f"Error al eliminar {file_path}: {e}"
            logger.error(error_msg)
            error_messages.append(error_msg)
    
    return deleted, errors, error_messages

def cleanup_empty_directories(
    directory: Path, 
    recursive: bool = True,
    dry_run: bool = False
) -> Tuple[int, int, List[str]]:
    """
    Elimina directorios vacíos.
    
    Args:
        directory: Directorio a limpiar
        recursive: Si se debe buscar recursivamente
        dry_run: Si es True, solo simula la eliminación
        
    Returns:
        Tupla con (directorios eliminados, errores, lista de errores)
    """
    if not directory.exists() or not directory.is_dir():
        return 0, 0, []
    
    deleted = 0
    errors = 0
    error_messages = []
    
    # Recopilar directorios
    dirs_to_check = []
    
    if recursive:
        # Recopilar todos los directorios de abajo hacia arriba
        for root, dirs, _ in os.walk(directory, topdown=False):
            for dir_name in dirs:
                dirs_to_check.append(Path(root) / dir_name)
    else:
        # Solo verificar subdirectorios directos
        dirs_to_check = [d for d in directory.iterdir() if d.is_dir()]
    
    # Eliminar directorios vacíos
    for dir_path in dirs_to_check:
        try:
            # Verificar si está vacío
            if any(dir_path.iterdir()):
                continue
            
            if dry_run:
                logger.info(f"[SIMULACIÓN] Eliminaría directorio vacío: {dir_path}")
                deleted += 1
            else:
                logger.info(f"Eliminando directorio vacío: {dir_path}")
                dir_path.rmdir()
                deleted += 1
        except Exception as e:
            errors += 1
            error_msg = f"Error al eliminar directorio {dir_path}: {e}"
            logger.error(error_msg)
            error_messages.append(error_msg)
    
    return deleted, errors, error_messages

def cleanup_logs(
    log_dir: Path = LOGS_DIR, 
    max_age_days: float = 7.0,
    preserve_latest: int = 5,
    dry_run: bool = False
) -> Tuple[int, int, List[str]]:
    """
    Limpia archivos de log antiguos.
    
    Args:
        log_dir: Directorio de logs
        max_age_days: Edad máxima en días
        preserve_latest: Número de archivos más recientes a preservar
        dry_run: Si es True, solo simula la eliminación
        
    Returns:
        Tupla con (archivos eliminados, errores, lista de errores)
    """
    if not log_dir.exists():
        return 0, 0, []
    
    deleted = 0
    errors = 0
    error_messages = []
    
    # Recopilar archivos de log
    log_files = []
    
    for item in log_dir.iterdir():
        if item.is_file() and item.suffix == '.log':
            log_files.append(item)
    
    # Ordenar por fecha de modificación (más reciente primero)
    log_files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
    
    # Preservar los más recientes
    preserved = log_files[:preserve_latest]
    to_check = log_files[preserve_latest:]
    
    # Filtrar por edad
    now = time.time()
    to_delete = []
    
    for file_path in to_check:
        try:
            mtime = file_path.stat().st_mtime
            age_days = (now - mtime) / (60 * 60 * 24)
            
            if age_days >= max_age_days:
                to_delete.append(file_path)
        except Exception as e:
            errors += 1
            error_msg = f"Error al verificar edad de {file_path}: {e}"
            logger.error(error_msg)
            error_messages.append(error_msg)
    
    # Eliminar archivos
    if to_delete:
        del_count, del_errors, del_messages = delete_files(to_delete, dry_run)
        deleted += del_count
        errors += del_errors
        error_messages.extend(del_messages)
    
    return deleted, errors, error_messages

def cleanup_temp_directories(
    temp_dirs: List[Path] = None,
    max_age_days: float = 1.0,
    dry_run: bool = False
) -> Tuple[int, int, List[str]]:
    """
    Limpia directorios temporales.
    
    Args:
        temp_dirs: Lista de directorios temporales
        max_age_days: Edad máxima en días
        dry_run: Si es True, solo simula la eliminación
        
    Returns:
        Tupla con (archivos eliminados, errores, lista de errores)
    """
    if temp_dirs is None:
        temp_dirs = [d for d in CLEANUP_DIRS if d.exists()]
    
    deleted = 0
    errors = 0
    error_messages = []
    
    for temp_dir in temp_dirs:
        if not temp_dir.exists():
            continue
        
        logger.info(f"Limpiando directorio temporal: {temp_dir}")
        
        # Encontrar archivos temporales
        temp_files = find_temp_files(
            temp_dir, 
            max_age_days=max_age_days,
            recursive=True
        )
        
        # Eliminar archivos
        if temp_files:
            del_count, del_errors, del_messages = delete_files(temp_files, dry_run)
            deleted += del_count
            errors += del_errors
            error_messages.extend(del_messages)
        
        # Limpiar directorios vacíos
        dir_count, dir_errors, dir_messages = cleanup_empty_directories(
            temp_dir, 
            recursive=True,
            dry_run=dry_run
        )
        deleted += dir_count
        errors += dir_errors
        error_messages.extend(dir_messages)
    
    return deleted, errors, error_messages

def cleanup_cache(
    cache_dir: Path = DATA_DIR / "cache",
    max_age_days: float = 7.0,
    dry_run: bool = False
) -> Tuple[int, int, List[str]]:
    """
    Limpia archivos de caché.
    
    Args:
        cache_dir: Directorio de caché
        max_age_days: Edad máxima en días
        dry_run: Si es True, solo simula la eliminación
        
    Returns:
        Tupla con (archivos eliminados, errores, lista de errores)
    """
    if not cache_dir.exists():
        return 0, 0, []
    
    deleted = 0
    errors = 0
    error_messages = []
    
    # Intentar usar el CacheManager si está disponible
    try:
        from src.core.cache_manager import CacheManager
        
        if dry_run:
            logger.info(f"[SIMULACIÓN] Limpiaría caché usando CacheManager")
            deleted += 1
        else:
            logger.info("Limpiando caché usando CacheManager")
            cache = CacheManager(cache_dir=str(cache_dir))
            items_removed = cache.cleanup(force=True)
            deleted += items_removed
            logger.info(f"Eliminados {items_removed} elementos de caché")
    except ImportError:
        # Fallback si no se puede importar CacheManager
        logger.info("CacheManager no disponible, usando limpieza manual")
        
        # Encontrar archivos de caché antiguos
        cache_files = []
        
        for item in cache_dir.glob("**/*"):
            if item.is_file():
                try:
                    mtime = item.stat().st_mtime
                    age_days = (time.time() - mtime) / (60 * 60 * 24)
                    
                    if age_days >= max_age_days:
                        cache_files.append(item)
                except Exception as e:
                    errors += 1
                    error_msg = f"Error al verificar edad de {item}: {e}"
                    logger.error(error_msg)
                    error_messages.append(error_msg)
        
        # Eliminar archivos
        if cache_files:
            del_count, del_errors, del_messages = delete_files(cache_files, dry_run)
            deleted += del_count
            errors += del_errors
            error_messages.extend(del_messages)
    
    return deleted, errors, error_messages

def find_duplicate_files(
    directory: Path,
    extensions: List[str] = None
) -> Dict[str, List[Path]]:
    """
    Encuentra archivos duplicados basados en su tamaño y hash.
    
    Args:
        directory: Directorio a buscar
        extensions: Lista de extensiones a considerar (None = todas)
        
    Returns:
        Diccionario de hashes con listas de archivos duplicados
    """
    import hashlib
    
    if not directory.exists():
        return {}
    
    # Primero agrupar por tamaño (más rápido)
    size_groups = {}
    
    for root, _, files in os.walk(directory):
        for filename in files:
            file_path = Path(root) / filename
            
            # Filtrar por extensión si se especifica
            if extensions and file_path.suffix.lower() not in extensions:
                continue
            
            try:
                file_size = file_path.stat().st_size
                
                if file_size == 0:  # Ignorar archivos vacíos
                    continue
                
                if file_size not in size_groups:
                    size_groups[file_size] = []
                
                size_groups[file_size].append(file_path)
            except (FileNotFoundError, PermissionError):
                continue
    
    # Filtrar grupos con un solo archivo (no pueden ser duplicados)
    size_groups = {size: paths for size, paths in size_groups.items() if len(paths) > 1}
    
    # Calcular hash para cada archivo y agrupar por hash
    hash_groups = {}
    
    for size, file_paths in size_groups.items():
        for file_path in file_paths:
            try:
                # Calcular hash del archivo
                hasher = hashlib.md5()
                
                with open(file_path, 'rb') as f:
                    # Leer en bloques para archivos grandes
                    for chunk in iter(lambda: f.read(4096), b''):
                        hasher.update(chunk)
                
                file_hash = hasher.hexdigest()
                
                if file_hash not in hash_groups:
                    hash_groups[file_hash] = []
                
                hash_groups[file_hash].append(file_path)
            except (FileNotFoundError, PermissionError):
                continue
    
    # Filtrar grupos con un solo archivo (no son duplicados)
    hash_groups = {h: paths for h, paths in hash_groups.items() if len(paths) > 1}
    
    return hash_groups

def cleanup_duplicates(
    directory: Path = OUTPUT_DIR,
    extensions: List[str] = None,
    preserve_newest: bool = True,
    dry_run: bool = False
) -> Tuple[int, int, List[str]]:
    """
    Elimina archivos duplicados.
    
    Args:
        directory: Directorio a limpiar
        extensions: Lista de extensiones a considerar (None = todas)
        preserve_newest: Si es True, preserva el archivo más reciente
        dry_run: Si es True, solo simula la eliminación
        
    Returns:
        Tupla con (archivos eliminados, errores, lista de errores)
    """
    if not directory.exists():
        return 0, 0, []
    
    deleted = 0
    errors = 0
    error_messages = []
    
    # Encontrar duplicados
    logger.info(f"Buscando archivos duplicados en {directory}")
    duplicates = find_duplicate_files(directory, extensions)
    
    if not duplicates:
        logger.info("No se encontraron archivos duplicados")
        return 0, 0, []
    
    # Procesar cada grupo de duplicados
    for file_hash, file_paths in duplicates.items():
        try:
            if preserve_newest:
                # Ordenar por fecha de modificación (más reciente primero)
                file_paths.sort(key=lambda x: x.stat().st_mtime, reverse=True)
                
                # Preservar el más reciente
                to_keep = file_paths[0]
                to_delete = file_paths[1:]
                
                logger.info(f"Preservando archivo más reciente: {to_keep}")
            else:
                # Preservar el primero alfabéticamente
                file_paths.sort()
                
                to_keep = file_paths[0]
                to_delete = file_paths[1:]
                
                logger.info(f"Preservando archivo: {to_keep}")
            
            # Eliminar duplicados
            for file_path in to_delete:
                if dry_run:
                    logger.info(f"[SIMULACIÓN] Eliminaría duplicado: {file_path}")
                    deleted += 1
                else:
                    logger.info(f"Eliminando duplicado: {file_path}")
                    file_path.unlink()
                    deleted += 1
        except Exception as e:
            errors += 1
            error_msg = f"Error al procesar duplicados con hash {file_hash}: {e}"
            logger.error(error_msg)
            error_messages.append(error_msg)
    
    return deleted, errors, error_messages

def optimize_project_structure(
    project_root: Path = PROJECT_ROOT,
    dry_run: bool = False
) -> Tuple[int, int, List[str]]:
    """
    Optimiza la estructura del proyecto.
    
    Args:
        project_root: Directorio raíz del proyecto
        dry_run: Si es True, solo simula la optimización
        
    Returns:
        Tupla con (cambios realizados, errores, lista de errores)
    """
    changes = 0
    errors = 0
    error_messages = []
    
    # Verificar estructura de directorios
    required_dirs = [
        "src",
        "src/core",
        "src/utils",
        "src/scraping",
        "src/exclusion",
        "src/masking",
        "data",
        "logs",
        "output",
        "temp"
    ]
    
    # Crear directorios faltantes
    for dir_path in required_dirs:
        full_path = project_root / dir_path
        
        if not full_path.exists():
            if dry_run:
                logger.info(f"[SIMULACIÓN] Crearía directorio: {full_path}")
                changes += 1
            else:
                logger.info(f"Creando directorio: {full_path}")
                full_path.mkdir(parents=True, exist_ok=True)
                changes += 1
    
    # Verificar archivos __init__.py en módulos Python
    python_dirs = [
        "src",
        "src/core",
        "src/utils",
        "src/scraping",
        "src/exclusion",
        "src/masking",
        "src/core/excel"
    ]
    
    for dir_path in python_dirs:
        full_path = project_root / dir_path
        init_file = full_path / "__init__.py"
        
        if full_path.exists() and not init_file.exists():
            if dry_run:
                logger.info(f"[SIMULACIÓN] Crearía archivo: {init_file}")
                changes += 1
            else:
                logger.info(f"Creando archivo: {init_file}")
                init_file.touch()
                changes += 1
    
    # Verificar archivos README.md
    if not (project_root / "README.md").exists():
        if dry_run:
            logger.info(f"[SIMULACIÓN] Crearía archivo README.md")
            changes += 1
        else:
            logger.info(f"Creando archivo README.md")
            with open(project_root / "README.md", 'w', encoding='utf-8') as f:
                f.write("# WebContactsExtractor Project\n\n")
                f.write("Proyecto para extracción de datos desde archivos CSV y URLs.\n")
            changes += 1
    
    # Verificar requirements.txt
    if not (project_root / "requirements.txt").exists():
        if dry_run:
            logger.info(f"[SIMULACIÓN] Crearía archivo requirements.txt")
            changes += 1
        else:
            logger.info(f"Creando archivo requirements.txt")
            with open(project_root / "requirements.txt", 'w', encoding='utf-8') as f:
                f.write("# Dependencias principales\n")
                f.write("pandas>=1.3.0\n")
                f.write("selenium>=4.0.0\n")
                f.write("requests>=2.25.0\n")
                f.write("beautifulsoup4>=4.9.0\n")
                f.write("openpyxl>=3.0.0\n")
                f.write("psutil>=5.8.0\n")
                f.write("tqdm>=4.62.0\n")
            changes += 1
    
    return changes, errors, error_messages

def main():
    """Función principal del script de limpieza."""
    parser = argparse.ArgumentParser(description="Limpieza y optimización del proyecto")
    
    parser.add_argument(
        "--temp", action="store_true",
        help="Limpiar archivos temporales"
    )
    parser.add_argument(
        "--logs", action="store_true",
        help="Limpiar archivos de log antiguos"
    )
    parser.add_argument(
        "--cache", action="store_true",
        help="Limpiar caché"
    )
    parser.add_argument(
        "--duplicates", action="store_true",
        help="Eliminar archivos duplicados"
    )
    parser.add_argument(
        "--optimize", action="store_true",
        help="Optimizar estructura del proyecto"
    )
    parser.add_argument(
        "--all", action="store_true",
        help="Ejecutar todas las operaciones de limpieza"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Simular operaciones sin realizar cambios"
    )
    parser.add_argument(
        "--max-age", type=float, default=7.0,
        help="Edad máxima en días para archivos a eliminar (default: 7.0)"
    )
    
    args = parser.parse_args()
    
    # Si no se especifica ninguna operación, mostrar ayuda
    if not any([args.temp, args.logs, args.cache, args.duplicates, args.optimize, args.all]):
        parser.print_help()
        return
    
    start_time = time.time()
    total_deleted = 0
    total_errors = 0
    
    logger.info("Iniciando limpieza del proyecto")
    
    if args.dry_run:
        logger.info("Modo simulación activado (no se realizarán cambios)")
    
    # Limpiar archivos temporales
    if args.temp or args.all:
        logger.info("Limpiando archivos temporales...")
        deleted, errors, _ = cleanup_temp_directories(
            max_age_days=args.max_age,
            dry_run=args.dry_run
        )
        total_deleted += deleted
        total_errors += errors
        logger.info(f"Archivos temporales: {deleted} eliminados, {errors} errores")
    
    # Limpiar logs
    if args.logs or args.all:
        logger.info("Limpiando archivos de log antiguos...")
        deleted, errors, _ = cleanup_logs(
            max_age_days=args.max_age,
            dry_run=args.dry_run
        )
        total_deleted += deleted
        total_errors += errors
        logger.info(f"Archivos de log: {deleted} eliminados, {errors} errores")
    
    # Limpiar caché
    if args.cache or args.all:
        logger.info("Limpiando caché...")
        deleted, errors, _ = cleanup_cache(
            max_age_days=args.max_age,
            dry_run=args.dry_run
        )
        total_deleted += deleted
        total_errors += errors
        logger.info(f"Caché: {deleted} elementos eliminados, {errors} errores")
    
    # Eliminar duplicados
    if args.duplicates or args.all:
        logger.info("Buscando y eliminando archivos duplicados...")
        deleted, errors, _ = cleanup_duplicates(
            dry_run=args.dry_run
        )
        total_deleted += deleted
        total_errors += errors
        logger.info(f"Duplicados: {deleted} eliminados, {errors} errores")
    
    # Optimizar estructura
    if args.optimize or args.all:
        logger.info("Optimizando estructura del proyecto...")
        changes, errors, _ = optimize_project_structure(
            dry_run=args.dry_run
        )
        total_deleted += changes
        total_errors += errors
        logger.info(f"Optimización: {changes} cambios realizados, {errors} errores")
    
    # Resumen
    elapsed = time.time() - start_time
    logger.info(f"Limpieza completada en {elapsed:.2f} segundos")
    logger.info(f"Total: {total_deleted} elementos procesados, {total_errors} errores")

if __name__ == "__main__":
    main()

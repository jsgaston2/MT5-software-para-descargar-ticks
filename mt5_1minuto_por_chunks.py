import pandas as pd
import MetaTrader5 as mt5
import time
from datetime import datetime, timedelta
import os
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def initialize_mt5():
    """Inicializar conexión con MetaTrader 5"""
    if not mt5.initialize():
        logger.error("Error al inicializar MT5")
        return False
    logger.info("Conexión con MT5 establecida correctamente")
    return True

def calculate_chunks(start_date, end_date, max_bars=100000):
    """
    Calcular chunks de tiempo respetando el límite máximo de barras
    
    Args:
        start_date: fecha de inicio (datetime)
        end_date: fecha de fin (datetime)
        max_bars: número máximo de barras por solicitud
    
    Returns:
        Lista de tuplas (chunk_start, chunk_end)
    """
    chunks = []
    current_start = start_date
    
    # Calcular el total de minutos en el período
    total_minutes = int((end_date - start_date).total_seconds() / 60)
    
    # Si el total es menor que el máximo, devolver un solo chunk
    if total_minutes <= max_bars:
        return [(start_date, end_date)]
    
    # Calcular el tamaño de cada chunk (90% del máximo para estar seguros)
    chunk_minutes = int(max_bars * 0.9)
    chunk_delta = timedelta(minutes=chunk_minutes)
    
    while current_start < end_date:
        chunk_end = min(current_start + chunk_delta, end_date)
        chunks.append((current_start, chunk_end))
        current_start = chunk_end + timedelta(minutes=1)  # Evitar solapamiento
    
    return chunks

def download_chunk(symbol, timeframe, chunk_start, chunk_end):
    """
    Descargar un chunk de datos
    
    Args:
        symbol: símbolo a descargar
        timeframe: timeframe de velas
        chunk_start: inicio del chunk
        chunk_end: fin del chunk
    
    Returns:
        DataFrame con los datos o None si hay error
    """
    try:
        # Convertir a formato de tiempo de MT5
        from_date = chunk_start
        to_date = chunk_end
        
        # Descargar datos
        rates = mt5.copy_rates_range(symbol, timeframe, from_date, to_date)
        
        if rates is None:
            logger.warning(f"No se pudieron descargar datos para {chunk_start} - {chunk_end}")
            return None
        
        # Crear DataFrame
        df = pd.DataFrame(rates)
        if len(df) == 0:
            return None
            
        # Convertir timestamp a datetime
        df['time'] = pd.to_datetime(df['time'], unit='s')
        
        logger.info(f"Descargado chunk {chunk_start} - {chunk_end}: {len(df)} velas")
        return df
        
    except Exception as e:
        logger.error(f"Error descargando chunk {chunk_start} - {chunk_end}: {str(e)}")
        return None

def download_historical_data(symbol, timeframe, start_date, end_date=None, max_bars=100000):
    """
    Descargar datos históricos en chunks
    
    Args:
        symbol: símbolo a descargar
        timeframe: timeframe de MT5 (ej. mt5.TIMEFRAME_M1)
        start_date: fecha de inicio (datetime)
        end_date: fecha de fin (datetime, opcional, por defecto ahora)
        max_bars: máximo de barras por chunk
    
    Returns:
        DataFrame combinado con todos los datos
    """
    if end_date is None:
        end_date = datetime.now()
    
    # Calcular chunks
    chunks = calculate_chunks(start_date, end_date, max_bars)
    logger.info(f"Se descargarán {len(chunks)} chunks de datos")
    
    all_data = []
    for i, (chunk_start, chunk_end) in enumerate(chunks):
        logger.info(f"Descargando chunk {i+1}/{len(chunks)}")
        
        # Descargar chunk
        chunk_data = download_chunk(symbol, timeframe, chunk_start, chunk_end)
        
        if chunk_data is not None and len(chunk_data) > 0:
            all_data.append(chunk_data)
        
        # Pequeña pausa para no saturar MT5
        time.sleep(0.1)
    
    # Combinar todos los datos
    if all_data:
        combined_data = pd.concat(all_data, ignore_index=True)
        combined_data.sort_values('time', inplace=True)
        combined_data.drop_duplicates('time', inplace=True)
        
        logger.info(f"Datos combinados: {len(combined_data)} velas desde {combined_data['time'].min()} hasta {combined_data['time'].max()}")
        return combined_data
    else:
        logger.error("No se pudieron descargar datos")
        return None

def save_to_csv(df, filename):
    """Guardar DataFrame a CSV"""
    try:
        df.to_csv(filename, index=False)
        logger.info(f"Datos guardados en {filename}")
        return True
    except Exception as e:
        logger.error(f"Error guardando datos: {str(e)}")
        return False

def main():
    """Función principal"""
    # Configuración
    symbol = "EURUSD"
    timeframe = mt5.TIMEFRAME_M1  # 1 minuto
    start_date = datetime.now() - timedelta(days=365*10)  # Último año
    output_filename = f"{symbol}_M10_1year.csv"
    max_bars_per_chunk = 100000  # Ajustar según las limitaciones de MT5
    
    # Inicializar MT5
    if not initialize_mt5():
        return
    
    try:
        # Descargar datos
        logger.info(f"Iniciando descarga de {symbol} desde {start_date}")
        data = download_historical_data(symbol, timeframe, start_date, max_bars=max_bars_per_chunk)
        
        if data is not None:
            # Guardar datos
            if save_to_csv(data, output_filename):
                logger.info("Descarga completada exitosamente")
            else:
                logger.error("Error al guardar los datos")
        else:
            logger.error("No se pudieron descargar datos")
            
    except Exception as e:
        logger.error(f"Error en la descarga: {str(e)}")
    finally:
        # Cerrar conexión MT5
        mt5.shutdown()
        logger.info("Conexión MT5 cerrada")

if __name__ == "__main__":
    main()
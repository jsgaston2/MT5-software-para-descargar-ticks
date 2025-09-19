# Script: download_ticks_by_date.py
import MetaTrader5 as mt5
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
import os

class TickDownloader:
    def __init__(self, symbol, start_date, end_date=None, max_ticks_per_batch=100000000):
        self.symbol = symbol
        self.start_date = start_date
        self.end_date = end_date if end_date else datetime.now()
        self.max_ticks_per_batch = max_ticks_per_batch
        self.downloaded_ticks = 0
        self.all_ticks = None  # Inicializar como None en lugar de lista vacía
        
    def initialize_mt5(self):
        """Inicializar MT5"""
        if not mt5.initialize():
            print("Error al inicializar MT5")
            return False
        
        if not mt5.symbol_select(self.symbol, True):
            print(f"Error: Símbolo {self.symbol} no disponible")
            mt5.shutdown()
            return False
            
        return True
    
    def get_ticks_by_date_range(self, start_date, end_date):
        """Obtener ticks en un rango de fechas específico"""
        try:
            print(f"📅 Descargando: {start_date.strftime('%Y-%m-%d %H:%M')} - {end_date.strftime('%Y-%m-%d %H:%M')}")
            ticks = mt5.copy_ticks_range(self.symbol, start_date, end_date, mt5.COPY_TICKS_ALL)
            
            if ticks is None:
                print("   ⚠️  No se encontraron ticks")
                return None
                
            print(f"   ✅ {len(ticks):,} ticks encontrados")
            return ticks
            
        except Exception as e:
            print(f"❌ Error al obtener ticks: {e}")
            return None
    
    def download_by_date_range(self):
        """Descargar ticks por rango de fechas completo"""
        if not self.initialize_mt5():
            return False
        
        try:
            print(f"🎯 Iniciando descarga desde {self.start_date} hasta {self.end_date}")
            print(f"📊 Símbolo: {self.symbol}")
            
            # Intentar descarga completa primero
            print("🔄 Intentando descarga completa del rango...")
            all_ticks = self.get_ticks_by_date_range(self.start_date, self.end_date)
            
            if all_ticks is not None and len(all_ticks) > 0:
                print(f"✅ Descarga completa exitosa: {len(all_ticks):,} ticks")
                self.all_ticks = all_ticks
                self.downloaded_ticks = len(all_ticks)
                return True
            
            # Si falla la descarga completa, intentar por lotes más pequeños
            print("⏳ La descarga completa falló, intentando por lotes...")
            return self.download_in_batches()
            
        except Exception as e:
            print(f"❌ Error durante la descarga: {e}")
            import traceback
            traceback.print_exc()
            return False
        finally:
            mt5.shutdown()
    
    def download_in_batches(self):
        """Descargar ticks en lotes por intervalos de tiempo"""
        current_end = self.end_date
        batch_number = 0
        all_ticks_list = []
        
        # Empezar desde la fecha final y retroceder
        while current_end > self.start_date:
            batch_number += 1
            
            # Calcular fecha de inicio del lote (1 día antes del current_end)
            batch_start = current_end - timedelta(days=1)
            if batch_start < self.start_date:
                batch_start = self.start_date
            
            print(f"\n📦 Lote #{batch_number}: {batch_start.strftime('%Y-%m-%d')} a {current_end.strftime('%Y-%m-%d')}")
            
            # Descargar lote actual
            ticks = self.get_ticks_by_date_range(batch_start, current_end)
            
            if ticks is not None and len(ticks) > 0:
                all_ticks_list.append(ticks)
                self.downloaded_ticks += len(ticks)
                print(f"   ✅ {len(ticks):,} ticks descargados | Total: {self.downloaded_ticks:,}")
            else:
                print("   ⚠️  No hay ticks en este período")
            
            # Mover al período anterior
            current_end = batch_start
            
            # Pequeña pausa para no sobrecargar
            time.sleep(0.1)
            
            # Verificar si hemos alcanzado la fecha de inicio
            if current_end <= self.start_date:
                break
        
        if all_ticks_list:
            # Combinar todos los ticks
            self.all_ticks = np.concatenate(all_ticks_list)
            print(f"\n✅ Descarga por lotes completada: {len(self.all_ticks):,} ticks")
            return True
        
        print("❌ No se encontraron ticks en el rango especificado")
        return False
    
    def save_ticks_to_csv(self, filename=None):
        """Guardar ticks en archivo CSV"""
        # Verificar si hay ticks para guardar (manera correcta para arrays numpy)
        if self.all_ticks is None or (hasattr(self.all_ticks, 'size') and self.all_ticks.size == 0):
            print("❌ No hay ticks para guardar")
            return None
        
        if len(self.all_ticks) == 0:
            print("❌ Array de ticks está vacío")
            return None
        
        try:
            # Convertir a DataFrame
            df = pd.DataFrame(self.all_ticks)
            df['time'] = pd.to_datetime(df['time'], unit='s')
            df = df.sort_values('time')
            
            # Crear nombre de archivo descriptivo
            if filename is None:
                first_date = df['time'].min().strftime('%Y%m%d')
                last_date = df['time'].max().strftime('%Y%m%d')
                filename = f"{self.symbol}_ticks_{first_date}_to_{last_date}_{len(df)}.csv"
            
            # Guardar en CSV
            print(f"💾 Guardando {len(df):,} ticks en archivo CSV...")
            df.to_csv(filename, index=False)
            
            # Verificar que el archivo se creó correctamente
            if os.path.exists(filename):
                file_size = os.path.getsize(filename) / (1024*1024)  # MB
                print(f"✅ Archivo guardado: {filename}")
                print(f"📊 Total ticks: {len(df):,}")
                print(f"📅 Rango temporal: {df['time'].min()} to {df['time'].max()}")
                print(f"📏 Tamaño del archivo: {file_size:.2f} MB")
                return filename
            else:
                print("❌ Error: El archivo no se creó correctamente")
                return None
                
        except Exception as e:
            print(f"❌ Error al guardar el archivo: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def show_statistics(self):
        """Mostrar estadísticas de los ticks descargados"""
        if self.all_ticks is None or len(self.all_ticks) == 0:
            print("❌ No hay datos para mostrar estadísticas")
            return
        
        try:
            df = pd.DataFrame(self.all_ticks)
            df['time'] = pd.to_datetime(df['time'], unit='s')
            
            print("\n📈 ESTADÍSTICAS DETALLADAS:")
            print("=" * 50)
            print(f"   • Total ticks descargados: {len(df):,}")
            print(f"   • Período completo: {df['time'].min()} to {df['time'].max()}")
            print(f"   • Duración total: {(df['time'].max() - df['time'].min()).days} días")
            
            # Calcular ticks por día
            if len(df) > 0:
                days = (df['time'].max() - df['time'].min()).days or 1
                ticks_per_day = len(df) / days
                ticks_per_hour = ticks_per_day / 24
                ticks_per_minute = ticks_per_hour / 60
                
                print(f"   • Ticks por día: {ticks_per_day:,.0f}")
                print(f"   • Ticks por hora: {ticks_per_hour:,.0f}")
                print(f"   • Ticks por minuto: {ticks_per_minute:,.1f}")
            
            # Estadísticas de precios
            print(f"   • Precio Bid mínimo: {df['bid'].min():.5f}")
            print(f"   • Precio Bid máximo: {df['bid'].max():.5f}")
            print(f"   • Precio Ask mínimo: {df['ask'].min():.5f}")
            print(f"   • Precio Ask máximo: {df['ask'].max():.5f}")
            print(f"   • Spread promedio: {(df['ask'] - df['bid']).mean() * 10000:.1f} pips")
            
            # Volumen
            print(f"   • Volumen total: {df['volume'].sum():,}")
            print(f"   • Volumen promedio por tick: {df['volume'].mean():.2f}")
            
        except Exception as e:
            print(f"❌ Error al calcular estadísticas: {e}")

# Función principal simplificada
def download_ticks_by_date(symbol, start_date, end_date=None, output_file=None):
    """
    Función simple para descargar ticks por fechas
    
    Args:
        symbol (str): Símbolo a descargar (ej: "EURUSD")
        start_date (datetime): Fecha de inicio
        end_date (datetime): Fecha de fin (opcional, por defecto ahora)
        output_file (str): Nombre del archivo de salida (opcional)
    """
    
    downloader = TickDownloader(symbol, start_date, end_date)
    
    if downloader.download_by_date_range():
        filename = downloader.save_ticks_to_csv(output_file)
        if filename:
            downloader.show_statistics()
            return filename
        else:
            print("❌ Error al guardar el archivo")
            return None
    else:
        print("❌ La descarga falló")
        return None

# Ejemplos de uso
if __name__ == "__main__":
    print("🚀 DESCARGADOR DE TICKS POR FECHAS")
    print("=" * 50)
    
    # Configuración - ¡MODIFICA ESTAS FECHAS!
    SYMBOL = "EURUSD"
    
    # Ejemplo 1: Últimos 7 días
    # END_DATE = datetime.now()
    # START_DATE = END_DATE - timedelta(days=7)
    
    # Ejemplo 2: Mes específico
    # START_DATE = datetime(2024, 1, 1)
    # END_DATE = datetime(2024, 1, 31)
    
    # Ejemplo 3: Rango personalizado (MODIFICA AQUÍ)
    START_DATE = datetime(2024, 9, 19,12,41,59)   # Año, Mes, Día
    END_DATE = datetime.now() # datetime(2014, 6, 15)    # Año, Mes, Día
    
    # Ejemplo 4: Año completo
    # START_DATE = datetime(2024, 1, 1)
    # END_DATE = datetime(2024, 12, 31, 23, 59, 59)
    
    # Ejemplo 5: Día específico
    # START_DATE = datetime(2024, 6, 10, 0, 0, 0)
    # END_DATE = datetime(2024, 6, 10, 23, 59, 59)
    
    print(f"📊 Símbolo: {SYMBOL}")
    print(f"📅 Desde: {START_DATE}")
    print(f"📅 Hasta: {END_DATE}")
    print("=" * 50)
    
    # Ejecutar descarga
    result_file = download_ticks_by_date(SYMBOL, START_DATE, END_DATE)
    
    if result_file:
        print(f"\n🎉 ¡Descarga completada exitosamente!")
        print(f"📁 Archivo guardado como: {result_file}")
        
        # Mostrar ubicación completa
        full_path = os.path.abspath(result_file)
        print(f"📂 Ruta completa: {full_path}")
    else:
        print("\n💥 La descarga falló")
    
    # Función para descargar múltiples períodos
    def download_multiple_periods():
        """Ejemplo: Descargar múltiples períodos"""
        periods = [

            (datetime(2020, 1, 1), datetime(2020, 1, 31), "ENERO_2020"),
            (datetime(2020, 2, 1), datetime(2020, 2, 29), "FEBRERO_2020"),
            (datetime(2020, 3, 1), datetime(2020, 3, 31), "MARZO_2020"),
            (datetime(2020, 4, 1), datetime(2020, 5, 30), "ABRIL_2020"),
            (datetime(2020, 5, 1), datetime(2020, 5, 31), "MAYO_2020"),
            (datetime(2020, 6, 1), datetime(2020, 6, 30), "JUNIO_2020"),
            (datetime(2020, 7, 1), datetime(2020, 7, 31), "JULIO_2020"),
            (datetime(2020, 8, 1), datetime(2020, 8, 31), "AGOSTO_2020"),
            (datetime(2020, 9, 1), datetime(2020, 9, 30), "SEPTIEMBRE_2020"),
            (datetime(2020, 10, 1), datetime(2020, 10, 30), "OCTUBRE_2020"),
            (datetime(2020, 11, 1), datetime(2020, 11, 30), "NOVIEMBRE_2020"),
            (datetime(2020, 12, 1), datetime(2020, 12, 31), "DICIEMBRE_2020"),            

            (datetime(2021, 1, 1), datetime(2021, 1, 31), "ENERO_2021"),
            (datetime(2021, 2, 1), datetime(2021, 2, 28), "FEBRERO_2021"),
            (datetime(2021, 3, 1), datetime(2021, 3, 31), "MARZO_2021"),
            (datetime(2021, 4, 1), datetime(2021, 5, 30), "ABRIL_2021"),
            (datetime(2021, 5, 1), datetime(2021, 5, 31), "MAYO_2021"),
            (datetime(2021, 6, 1), datetime(2021, 6, 30), "JUNIO_2021"),
            (datetime(2021, 7, 1), datetime(2021, 7, 31), "JULIO_2021"),
            (datetime(2021, 8, 1), datetime(2021, 8, 31), "AGOSTO_2021"),
            (datetime(2021, 9, 1), datetime(2021, 9, 30), "SEPTIEMBRE_2021"),
            (datetime(2021, 10, 1), datetime(2021, 10, 30), "OCTUBRE_2021"),
            (datetime(2021, 11, 1), datetime(2021, 11, 30), "NOVIEMBRE_2021"),
            (datetime(2021, 12, 1), datetime(2021, 12, 31), "DICIEMBRE_2021"),

            (datetime(2022, 1, 1), datetime(2022, 1, 31), "ENERO_2022"),
            (datetime(2022, 2, 1), datetime(2022, 2, 28), "FEBRERO_2022"),
            (datetime(2022, 3, 1), datetime(2022, 3, 31), "MARZO_2022"),
            (datetime(2022, 4, 1), datetime(2022, 5, 30), "ABRIL_2022"),
            (datetime(2022, 5, 1), datetime(2022, 5, 31), "MAYO_2022"),
            (datetime(2022, 6, 1), datetime(2022, 6, 30), "JUNIO_2022"),
            (datetime(2022, 7, 1), datetime(2022, 7, 31), "JULIO_2022"),
            (datetime(2022, 8, 1), datetime(2022, 8, 31), "AGOSTO_2022"),
            (datetime(2022, 9, 1), datetime(2022, 9, 30), "SEPTIEMBRE_2022"),
            (datetime(2022, 10, 1), datetime(2022, 10, 30), "OCTUBRE_2022"),
            (datetime(2022, 11, 1), datetime(2022, 11, 30), "NOVIEMBRE_2022"),
            (datetime(2022, 12, 1), datetime(2022, 12, 31), "DICIEMBRE_2022"),

            (datetime(2023, 1, 1), datetime(2023, 1, 31), "ENERO_2023"),
            (datetime(2023, 2, 1), datetime(2023, 2, 28), "FEBRERO_2023"),
            (datetime(2023, 3, 1), datetime(2023, 3, 31), "MARZO_2023"),
            (datetime(2023, 4, 1), datetime(2023, 5, 30), "ABRIL_2023"),
            (datetime(2023, 5, 1), datetime(2023, 5, 31), "MAYO_2023"),
            (datetime(2023, 6, 1), datetime(2023, 6, 30), "JUNIO_2023"),
            (datetime(2023, 7, 1), datetime(2023, 7, 31), "JULIO_2023"),
            (datetime(2023, 8, 1), datetime(2023, 8, 31), "AGOSTO_2023"),
            (datetime(2023, 9, 1), datetime(2023, 9, 30), "SEPTIEMBRE_2023"),
            (datetime(2023, 10, 1), datetime(2023, 10, 30), "OCTUBRE_2023"),
            (datetime(2023, 11, 1), datetime(2023, 11, 30), "NOVIEMBRE_2023"),
            (datetime(2023, 12, 1), datetime(2023, 12, 31), "DICIEMBRE_2023"),

            (datetime(2024, 1, 1), datetime(2024, 1, 31), "ENERO_2024"),
            (datetime(2024, 2, 1), datetime(2024, 2, 29), "FEBRERO_2024"),
            (datetime(2024, 3, 1), datetime(2024, 3, 31), "MARZO_2024"),
            (datetime(2024, 4, 1), datetime(2024, 5, 30), "ABRIL_2024"),
            (datetime(2024, 5, 1), datetime(2024, 5, 31), "MAYO_2024"),
            (datetime(2024, 6, 1), datetime(2024, 6, 30), "JUNIO_2024"),
            (datetime(2024, 7, 1), datetime(2024, 7, 31), "JULIO_2024"),
            (datetime(2024, 8, 1), datetime(2024, 8, 31), "AGOSTO_2024"),
            (datetime(2024, 9, 1), datetime(2024, 9, 30), "SEPTIEMBRE_2024"),
            (datetime(2024, 10, 1), datetime(2024, 10, 30), "OCTUBRE_2024"),
            (datetime(2024, 11, 1), datetime(2024, 11, 30), "NOVIEMBRE_2024"),
            (datetime(2024, 12, 1), datetime(2024, 12, 31), "DICIEMBRE_2024"),


            (datetime(2025, 1, 1), datetime(2025, 1, 31), "ENERO_2025"),
            (datetime(2025, 2, 1), datetime(2025, 2, 28), "FEBRERO_2025"),
            (datetime(2025, 3, 1), datetime(2025, 3, 31), "MARZO_2025"),
            (datetime(2025, 4, 1), datetime(2025, 5, 30), "ABRIL_2025"),
            (datetime(2025, 5, 1), datetime(2025, 5, 31), "MAYO_2025"),
            (datetime(2025, 6, 1), datetime(2025, 6, 30), "JUNIO_2025"),
            (datetime(2025, 7, 1), datetime(2025, 7, 31), "JULIO_2025"),
            (datetime(2025, 8, 1), datetime(2025, 8, 31), "AGOSTO_2025"),
            (datetime(2025, 9, 1), datetime(2025, 9, 30), "SEPTIEMBRE_2025"),
            (datetime(2025, 10, 1), datetime(2025, 10, 30), "OCTUBRE_2025"),
            (datetime(2025, 11, 1), datetime(2025, 11, 30), "NOVIEMBRE_2025"),
            (datetime(2025, 12, 1), datetime(2025, 12, 31), "DICIEMBRE_2025"),
        ]
        
        for start, end, name in periods:
            print(f"\n{'='*60}")
            print(f"📥 Descargando {name}...")
            print(f"{'='*60}")
            filename = f"{SYMBOL}_{name}.csv"
            result = download_ticks_by_date(SYMBOL, start, end, filename)
            if result:
                print(f"✅ {name} completado: {result}")
            else:
                print(f"❌ {name} falló")
            time.sleep(1)  # Pausa entre descargas
    
    # Preguntar si quiere descargar múltiples períodos
    print("\n¿Quieres descargar múltiples períodos? (s/n)")
    respuesta = input().strip().lower()
    
    if respuesta == 's':
        download_multiple_periods()
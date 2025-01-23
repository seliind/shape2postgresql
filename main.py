import os
import zipfile
import requests
import geopandas as gpd
from sqlalchemy import create_engine
import psycopg2
from datetime import datetime

# PostgreSQL bağlantı ayarları
DB_HOST = "127.0.0.1"
DB_PORT = "5432"
DB_NAME = "testcode4"
DB_USER = "postgres"
DB_PASSWORD = "1234"

# İndirme linki ve zip dosyasının çıkarılacağı klasör
zip_url = "https://download.geofabrik.de/europe/turkey-latest-free.shp.zip"
shapefile_folder = "downloaded_shapefiles"

# Zip dosyasını indir
zip_file_path = os.path.join(shapefile_folder, "turkey-latest-free.shp.zip")

# Zip dosyasını indirme işlemi
print("Zip dosyası indiriliyor lütfen bekleyiniz...")
response = requests.get(zip_url)
with open(zip_file_path, "wb") as file:
    file.write(response.content)
print("Zip dosyası başarıyla indirildi.")

# Zip dosyasını çıkarma
if not os.path.exists(shapefile_folder):
    os.makedirs(shapefile_folder)

with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
    zip_ref.extractall(shapefile_folder)
    print("Zip dosyası çıkarıldı.")

# Shapefile dosyalarının listesini otomatik oluşturma
layers = []
for file_name in os.listdir(shapefile_folder):
    if file_name.endswith(".shp"):
        if "gis_osm_" in file_name:
            layer_base_name = file_name.replace("gis_osm_", "").split("_free_1")[0] 
            layers.append(layer_base_name)

print(f"Bulunan katmanlar: {layers}")

# PostgreSQL bağlantı engine
connection_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(connection_string)

def log_osm_update(status, message):
    """Logs the status and message to the osm_log table."""
    try:
        # Connect to the PostgreSQL database and insert log entry
        with psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
        ) as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO osm_log (log_date, status, message)
                    VALUES (%s, %s, %s);
                """, (datetime.now(), status, message))
                conn.commit()
    except Exception as e:
        print(f"Log kaydı yapılamadı: {e}")
# Katmanları işle
for layer_name in layers:
    shapefile_path = os.path.join(shapefile_folder, f"gis_osm_{layer_name}_free_1.shp") 
    
    if os.path.exists(shapefile_path):
        print(f"{layer_name} katmanı işleniyor...")
        try:
            # Shapefile'i GeoDataFrame'e yükle
            gdf = gpd.read_file(shapefile_path)
            
            # Koordinat referans sistemini EPSG:4326'ya dönüştür
            if gdf.crs != "EPSG:4326":
                gdf = gdf.to_crs(epsg=4326)
                print(f"{layer_name} katmanının CRS EPSG:4326'ya dönüştürüldü.")
            
            # `fclass` sütunundaki benzersiz değerleri al
            if "fclass" in gdf.columns:
                unique_fclasses = gdf["fclass"].unique()
                
                for fclass_value in unique_fclasses:
                    # `fclass` değerine göre veriyi filtrele
                    subset_gdf = gdf[gdf["fclass"] == fclass_value]
                    
                    # Tablo adını oluştur (ör: water_<fclass>)
                    if layer_name.endswith("_a"):
                        layer_name = layer_name.rstrip("_a")
                        
                    fclass_table_name = f"{layer_name}_{fclass_value}".replace("-", "_").replace(" ", "_")
                    
                    # Varsa mevcut tabloyu sil
                    with psycopg2.connect(
                        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
                    ) as conn:
                        with conn.cursor() as cursor:
                            cursor.execute(f"""
                                DROP TABLE IF EXISTS public.{fclass_table_name};
                            """)
                            conn.commit()
                    
                    # PostgreSQL'e kaydet (yeni tablonun oluşturulması)
                    subset_gdf.to_postgis(
                        name=fclass_table_name, 
                        con=engine, 
                        if_exists="replace", 
                        index=False
                    )
                    
                    # primary key ekle
                    with psycopg2.connect(
                        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
                    ) as conn:
                        with conn.cursor() as cursor:
                            cursor.execute(f"""
                                ALTER TABLE public.{fclass_table_name}
                                ADD COLUMN gid SERIAL PRIMARY KEY;
                            """)
                            conn.commit()

                            cursor.execute(f"""
                                ALTER TABLE public.{fclass_table_name}
                                DROP COLUMN fclass;
                            """)
                            conn.commit()

                            cursor.execute(f"""
                                ALTER TABLE public.{fclass_table_name}
                                RENAME COLUMN geometry TO geom;
                            """)
                            conn.commit()
                    
                    print(f"Yeni {fclass_table_name} tablosu başarıyla oluşturuldu.")
            else:
                print(f"{layer_name} katmanında 'fclass' sütunu bulunamadı, orijinal tablo kaydediliyor.")
                gdf.to_postgis(name=layer_name, con=engine, if_exists="replace", index=False)
                log_osm_update("Error", f"{layer_name} katmanında 'fclass' sütunu bulunamadı, orijinal tablo kaydedildi.")
                
        except Exception as e:
            print(f"{layer_name} katmanı yüklenirken bir hata oluştu: {e}")
            log_osm_update("Error", f"{layer_name} katmanı yüklenirken bir hata oluştu: {e}")
    else:
        print(f"{layer_name}.shp bulunamadı.")
        log_osm_update("Error", f"{layer_name}.shp bulunamadı.")


log_osm_update("Success", "Tablolar başarıyla güncellendi.")
print("Tüm katmanlar işlemleri tamamlandı. Teşekkürler!")

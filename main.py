import os
import zipfile
import requests
import geopandas as gpd
from sqlalchemy import create_engine
import psycopg2

# PostgreSQL bağlantı ayarları
DB_HOST = "127.0.0.1"
DB_PORT = "5432"
DB_NAME = "testcode"
DB_USER = "postgres"
DB_PASSWORD = "1234"

# İndirme linki ve zip dosyasının çıkarılacağı klasör
zip_url = "https://download.geofabrik.de/europe/turkey-latest-free.shp.zip"
shapefile_folder = "downloaded_shapefiles"

# Zip dosyasını indir
zip_file_path = os.path.join(shapefile_folder, "turkey-latest-free.shp.zip")

# Zip dosyasını indirme işlemi
print("Zip dosyası indiriliyor...")
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

# PostgreSQL connection engine
connection_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(connection_string)

# Çıkarılan shapefile'lar ile işlem yapma
layers = [
    "natural"
    # "buildings", "landuse", "places", "pofw", "pois", "railways",
    # "road", "traffic", "transport", "water", "waterways", "building"
]

for layer_name in layers:
    shapefile_path = os.path.join(shapefile_folder, f"gis_osm_{layer_name}_a_free_1.shp")
    
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
                    fclass_table_name = f"{layer_name}_{fclass_value}".replace("-", "_").replace(" ", "_")
                    
                    # Tabloyu silme işlemi (varsa)
                    with psycopg2.connect(
                        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
                    ) as conn:
                        with conn.cursor() as cursor:
                            cursor.execute(f"""
                                DROP TABLE IF EXISTS public.{fclass_table_name};
                            """)
                            conn.commit()
                            print(f"{fclass_table_name} tablosu silindi.")
                    
                    # PostgreSQL'e kaydet (yeni tablonun oluşturulması)
                    subset_gdf.to_postgis(
                        name=fclass_table_name, 
                        con=engine, 
                        if_exists="replace", 
                        index=False
                    )
                    
                    # `gid` sütununu ekle ve birincil anahtar yap
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
                    
                    print(f"{fclass_table_name} tablosu başarıyla oluşturuldu, gid sütunu eklendi, fclass sütunu silindi ve geometry kolonu geom olarak adlandırıldı.")
            else:
                print(f"{layer_name} katmanında 'fclass' sütunu bulunamadı, orijinal tablo kaydediliyor.")
                gdf.to_postgis(name=layer_name, con=engine, if_exists="replace", index=False)
                
        except Exception as e:
            print(f"{layer_name} katmanı yüklenirken bir hata oluştu: {e}")
    else:
        print(f"{layer_name}.shp bulunamadı.")

print("Tüm katmanlar işleme tamamlandı.")

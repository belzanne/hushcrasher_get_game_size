#!/usr/bin/env python3
"""
Scraper pour récupérer les tailles des jeux Steam et les stocker dans DuckDB
"""

import steam.monkey
steam.monkey.patch_minimal()

import os
import time
import random
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional
import duckdb
from steam.client import SteamClient
from steam.client.cdn import CDNClient
from gevent import sleep
from dotenv import load_dotenv

# Charger les variables d'environnement depuis .env
load_dotenv()

# Configuration depuis les variables d'environnement
STEAM_DB_PATH = os.getenv('STEAM_DB_PATH')
GAME_SIZES_DB_PATH = os.getenv('GAME_SIZES_DB_PATH')
BATCH_SIZE = 200
MAX_RETRIES = 2

class SteamGameSizesToDuckDB:
    def __init__(self):
        self.setup_logging()
        self.setup_database()
        self.client = None
        self.cdn = None
        self.logger.info("🚀 Steam Game Sizes to DuckDB initialisé")
    
    def setup_logging(self):
        """Configuration du système de logging"""
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)
        
        log_filename = log_dir / f"steam_game_sizes_duckdb.log"
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_filename, encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        
        self.logger = logging.getLogger(__name__)
        self.logger.info(f"📝 Logging configuré - Fichier: {log_filename}")
    
    def setup_database(self):
        """Configuration de la base de données DuckDB"""
        try:
            # Créer la base de données si elle n'existe pas
            self.db_conn = duckdb.connect(GAME_SIZES_DB_PATH)
            
            # Créer la table des tailles de jeux si elle n'existe pas
            create_table_query = """
            CREATE TABLE IF NOT EXISTS game_sizes (
                app_id INTEGER,
                depot_id INTEGER,
                disk_size BIGINT,
                download_size BIGINT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (app_id, depot_id)
            )
            """
            
            self.db_conn.execute(create_table_query)
            
            # Créer la table des app_id échoués si elle n'existe pas
            create_failed_table_query = """
            CREATE TABLE IF NOT EXISTS failed_app_ids (
                app_id INTEGER PRIMARY KEY,
                error_count INTEGER DEFAULT 1,
                last_failed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
            
            self.db_conn.execute(create_failed_table_query)
            self.logger.info(f"✅ Base de données configurée: {GAME_SIZES_DB_PATH}")
            
            # Vérifier combien d'AppID ont déjà été traités
            count_query = "SELECT COUNT(DISTINCT app_id) FROM game_sizes"
            processed_count = self.db_conn.execute(count_query).fetchone()[0]
            
        except Exception as e:
            self.logger.error(f"❌ Erreur lors de la configuration de la base de données: {e}")
            raise
    
    def connect_to_steam(self):
        """Connexion à Steam"""
        try:
            self.client = SteamClient()
            self.client.anonymous_login()
            
            while not self.client.logged_on:
                self.client.run_forever()
                sleep(1)
            
            self.cdn = CDNClient(self.client)
            self.logger.info("✅ Connexion à Steam établie")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Erreur de connexion Steam: {e}")
            return False
    
    def disconnect_from_steam(self):
        """Déconnexion de Steam"""
        try:
            if self.client:
                self.client.logout()
                self.logger.info("🔌 Déconnexion de Steam effectuée")
        except Exception as e:
            self.logger.error(f"❌ Erreur de déconnexion: {e}")
    
    def get_app_ids(self, limit: Optional[int] = None) -> List[int]:
        """Récupération des app_id depuis la base de données Steam"""
        try:
            # Récupérer tous les AppID depuis la base Steam
            steam_conn = duckdb.connect(STEAM_DB_PATH)
            steam_query = "SELECT DISTINCT app_id FROM app_details WHERE app_id IS NOT NULL ORDER BY app_id"
            if limit:
                steam_query += f" LIMIT {limit}"
            
            steam_result = steam_conn.execute(steam_query).fetchall()
            all_app_ids = [row[0] for row in steam_result]
            steam_conn.close()
            
            # Récupérer les AppID déjà traités depuis game_sizes
            if os.path.exists(GAME_SIZES_DB_PATH):
                game_sizes_conn = duckdb.connect(GAME_SIZES_DB_PATH)
                processed_query = "SELECT DISTINCT app_id FROM game_sizes"
                processed_result = game_sizes_conn.execute(processed_query).fetchall()
                processed_app_ids = set(row[0] for row in processed_result)
                
                # Récupérer les AppID qui ont échoué
                failed_query = "SELECT app_id FROM failed_app_ids"
                failed_result = game_sizes_conn.execute(failed_query).fetchall()
                failed_app_ids = set(row[0] for row in failed_result)
                
                game_sizes_conn.close()
            else:
                processed_app_ids = set()
                failed_app_ids = set()
            
            # Filtrer les AppID non traités et non échoués
            app_ids = [app_id for app_id in all_app_ids if app_id not in processed_app_ids and app_id not in failed_app_ids]
            
            self.logger.info(f"📊 Récupéré {len(app_ids)} app_id non traités depuis la base de données")
            self.logger.info(f"📊 Exclus: {len(processed_app_ids)} déjà traités, {len(failed_app_ids)} échoués précédemment")
            return app_ids
            
        except Exception as e:
            self.logger.error(f"❌ Erreur lors de la récupération des app_id: {e}")
            raise
    
    def get_game_size_info(self, app_id: int) -> Optional[List[Dict]]:
        """Récupération des informations de taille pour un AppID"""
        for attempt in range(MAX_RETRIES):
            try:
                depot_info = self.cdn.get_app_depot_info(app_id)
                
                if not depot_info:
                    return None
                
                depot_data_list = []
                
                for depot_id, depot_data in depot_info.items():
                    if isinstance(depot_data, dict) and 'manifests' in depot_data:
                        manifests = depot_data.get('manifests', {})
                        if 'public' in manifests:
                            public_manifest = manifests['public']
                            disk_size = int(public_manifest.get('size', 0))
                            download_size = int(public_manifest.get('download', 0))
                            
                            if disk_size > 0:
                                depot_data_list.append({
                                    'app_id': app_id,
                                    'depot_id': depot_id,
                                    'disk_size': disk_size,
                                    'download_size': download_size
                                })
                
                return depot_data_list if depot_data_list else None
                
            except Exception as e:
                #self.logger.warning(f"⚠️ Erreur pour app_id {app_id} (tentative {attempt + 1}): {e}")
                
                if attempt < MAX_RETRIES - 1:
                    # Délai seulement lors des tentatives de retry (0.5 à 1.5 sec)
                    delay = random.uniform(0.5, 1.5)
                    time.sleep(delay)
        
        self.logger.error(f"❌ Échec pour app_id {app_id} après {MAX_RETRIES} tentatives")
        self.record_failed_app_id(app_id)
        return None
    
    def record_failed_app_id(self, app_id: int):
        """Enregistre un app_id qui a échoué dans la base de données"""
        try:
            # Utiliser INSERT OR REPLACE pour mettre à jour le compteur d'erreurs
            insert_query = """
            INSERT OR REPLACE INTO failed_app_ids (app_id, error_count, last_failed_at)
            VALUES (?, 
                    COALESCE((SELECT error_count FROM failed_app_ids WHERE app_id = ?), 0) + 1,
                    CURRENT_TIMESTAMP)
            """
            
            self.db_conn.execute(insert_query, (app_id, app_id))
            self.db_conn.commit()
            
        except Exception as e:
            self.logger.error(f"❌ Erreur lors de l'enregistrement de l'app_id échoué {app_id}: {e}")
    
    def save_batch_to_db(self, batch_data: List[Dict]):
        """Sauvegarde d'un batch de données dans DuckDB"""
        try:
            if not batch_data:
                return
            
            # Préparer les données pour l'insertion
            data_to_insert = []
            for item in batch_data:
                data_to_insert.append((
                    item['app_id'],
                    item['depot_id'],
                    item['disk_size'],
                    item['download_size']
                ))
            
            # Insertion avec gestion des doublons (ON CONFLICT DO NOTHING)
            insert_query = """
            INSERT OR IGNORE INTO game_sizes (app_id, depot_id, disk_size, download_size)
            VALUES (?, ?, ?, ?)
            """
            
            self.db_conn.executemany(insert_query, data_to_insert)
            self.db_conn.commit()
            
            self.logger.info(f"💾 Batch sauvegardé: {len(batch_data)} enregistrements")
            
        except Exception as e:
            self.logger.error(f"❌ Erreur lors de la sauvegarde du batch: {e}")
            raise
    
    def scrape_batch(self, app_ids: List[int], batch_number: int):
        """Scraping d'un batch d'app_id"""
        batch_data = []
        successful = 0
        failed = 0
        
        self.logger.info(f"🚀 Début du scraping du batch {batch_number} ({len(app_ids)} app_id)")
        
        for i, app_id in enumerate(app_ids, 1):
            try:
                depot_info = self.get_game_size_info(app_id)
                
                if depot_info:
                    batch_data.extend(depot_info)
                    successful += 1
                    
                    total_size = sum(item['disk_size'] for item in depot_info)
                    self.logger.debug(f"✅ AppID {app_id}: {len(depot_info)} dépôts, {total_size / (1024**3):.2f} GB")
                else:
                    failed += 1
                
                # Pas de délai entre les requêtes normales pour optimiser la vitesse
                
                # Log de progression
                if i % 50 == 0 or i == len(app_ids):
                    self.logger.info(f"📊 Batch {batch_number} - Progression: {i}/{len(app_ids)} ({successful} succès, {failed} échecs)")
                
            except Exception as e:
                self.logger.error(f"❌ Erreur inattendue pour app_id {app_id}: {e}")
                failed += 1
        
        # Sauvegarder le batch dans la base de données
        if batch_data:
            self.save_batch_to_db(batch_data)
        
        self.logger.info(f"✅ Batch {batch_number} terminé: {successful} succès, {failed} échecs")
        return successful, failed
    
    def run(self, limit: Optional[int] = None):
        """Exécution principale du scraper"""
        try:
            # Connexion à Steam
            if not self.connect_to_steam():
                return
            
            # Récupération des app_id non traités
            app_ids = self.get_app_ids(limit)
            
            if not app_ids:
                self.logger.info("🎉 Tous les AppID ont déjà été traités !")
                return
            
            # Division en batches
            batches = [app_ids[i:i + BATCH_SIZE] for i in range(0, len(app_ids), BATCH_SIZE)]
            
            self.logger.info(f"🎯 Début du scraping: {len(app_ids)} app_id répartis en {len(batches)} batches")
            
            total_successful = 0
            total_failed = 0
            
            for batch_number, batch_app_ids in enumerate(batches, 1):
                try:
                    successful, failed = self.scrape_batch(batch_app_ids, batch_number)
                    total_successful += successful
                    total_failed += failed
                    
                    # Pause entre les batches
                    if batch_number < len(batches):
                        pause_duration = random.uniform(0.2, 1)
                        self.logger.info(f"⏸️ Pause entre batches de {pause_duration:.1f}s")
                        time.sleep(pause_duration)
                
                except Exception as e:
                    self.logger.error(f"❌ Erreur lors du traitement du batch {batch_number}: {e}")
                    total_failed += len(batch_app_ids)
            
            # Résumé final
            self.logger.info(f"🎉 Scraping terminé!")
            self.logger.info(f"📊 Résumé: {total_successful} succès, {total_failed} échecs")
            
            # Statistiques finales
            stats_query = "SELECT COUNT(DISTINCT app_id), COUNT(*) FROM game_sizes"
            stats = self.db_conn.execute(stats_query).fetchone()
            self.logger.info(f"📈 Total dans la base: {stats[0]} AppID, {stats[1]} dépôts")
            
        except Exception as e:
            self.logger.error(f"❌ Erreur fatale lors du scraping: {e}")
            raise
        finally:
            self.disconnect_from_steam()
            if hasattr(self, 'db_conn'):
                self.db_conn.close()

def main():
    """Fonction principale"""
    import argparse
    import random
    
    parser = argparse.ArgumentParser(description="Scraper Steam pour les tailles des jeux vers DuckDB")
    parser.add_argument("--limit", type=int, help="Limiter le nombre d'app_id à traiter")
    parser.add_argument("--test", action="store_true", help="Mode test avec seulement 10 app_id")
    
    args = parser.parse_args()
    
    # Mode test
    if args.test:
        args.limit = 10
    
    scraper = SteamGameSizesToDuckDB()
    scraper.run(limit=args.limit)

if __name__ == "__main__":
    main()

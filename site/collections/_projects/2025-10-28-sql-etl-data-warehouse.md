---
title: II.    SQL-ETL-data-warehouse-Inside-Airbnb
date: 2025-10-28
github_url: https://github.com/YassineEng/SQL-ETL-data-warehouse-Inside-Airbnb
order: 2
---

<!-- Badges (must be outside YAML front matter) -->
<div style="margin-left: 20px;">
  <img src="https://img.shields.io/badge/Python-3.x-blue?logo=python">
  <img src="https://img.shields.io/badge/SQL%20Server-Database-red?logo=microsoftsqlserver">
  <img src="https://img.shields.io/badge/Pandas-Library-150458?logo=pandas">
  <img src="https://img.shields.io/badge/PySpark-ETL-E25A1C?logo=apachespark">
</div>


<p style="margin-left: 20px;">This is an Airbnb Data Warehouse ETL (Extract, Transform, Load) pipeline, designed to process raw Airbnb data (csv.gz) and load it into a SQL Server data warehouse. The pipeline includes modules for analysis, cleaning, validation, and loading, along with robust database management features.</p>

<table>
  <thead>
    <tr>
      <th>Table</th>
      <th>Rows</th>
      <th>Size</th>
      <th>Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>📅 fact_calendar</td>
      <td>62,473,247</td>
      <td>3.77 GB</td>
      <td>Daily availability &amp; price data for listings.</td>
    </tr>
    <tr>
      <td>💬 fact_reviews</td>
      <td>6,357,239</td>
      <td>4.74 GB</td>
      <td>Historical review records from guests.</td>
    </tr>
    <tr>
      <td>🗺️ dim_listing_id_map</td>
      <td>1,499,856</td>
      <td>150.1 MB</td>
      <td>Mapping table for internal and external listing IDs.</td>
    </tr>
    <tr>
      <td>🏠 dim_listings</td>
      <td>1,494,030</td>
      <td>431.9 MB</td>
      <td>Core property details and attributes.</td>
    </tr>
    <tr>
      <td>👤 dim_hosts</td>
      <td>740,651</td>
      <td>57.9 MB</td>
      <td>Host information and profile details.</td>
    </tr>
    <tr>
      <td>📆 dim_dates</td>
      <td>640</td>
      <td>0.1 MB</td>
      <td>Date dimension for time-based analysis.</td>
    </tr>
  </tbody>
</table>

<div class="code-window-container">
  <div class="code-window">
    <div class="code-header">
      <span class="red"></span>
      <span class="yellow"></span>
      <span class="green"></span>
    </div>
    <div class="code-body">
<pre><code>
<span class="comment"># main.py</span>
<span class="string">"""
Airbnb Data Warehouse ETL Pipeline
Main entry point for the ETL process - Updated for SQL Server
"""</span>

<span class="imports">import</span> sys
<span class="imports">import</span> os
<span class="imports">import</span> glob
<span class="imports">from</span> typing <span class="imports">import</span> Optional

<span class="comment"># Add project root to Python path</span>
sys.path.<span class="function">append</span>(os.path.<span class="function">dirname</span>(os.path.<span class="function">abspath</span>(__file__)))

<span class="imports">from</span> config.settings <span class="imports">import</span> Config
<span class="imports">from</span> config.database_config <span class="imports">import</span> DatabaseConfig  <span class="comment"># ← Add this import</span>
<span class="imports">from</span> modules.data_analyzer <span class="imports">import</span> AirbnbDataAnalyzer
<span class="imports">from</span> modules.data_cleaner <span class="imports">import</span> AirbnbDataCleaner
<span class="imports">from</span> modules.data_loader <span class="imports">import</span> AirbnbDataLoader
<span class="imports">from</span> utils.logger <span class="imports">import</span> setup_logging, get_logger
logger = <span class="function">get_logger</span>(__name__)
<span class="imports">from</span> utils.utility <span class="imports">import</span> validate_directory, create_timestamp


<span class="keyword">def</span> <span class="function">main</span>():
    <span class="string">"""Main ETL pipeline execution"""</span>
    <span class="function">setup_logging</span>(log_level=<span class="string">"DEBUG"</span>)
    config = <span class="imports">Config</span>()

    <span class="comment"># Validate paths</span>
    <span class="keyword">if</span> <span class="function">not</span> config.<span class="function">validate_paths</span>():
        <span class="function">print</span>(<span class="string">"❌ Configuration validation failed!"</span>)
        <span class="keyword">return</span>

    <span class="comment"># Check if raw data exists for EDA and cleaning</span>
    raw_files = config.<span class="function">get_data_files</span>()
    <span class="keyword">if</span> <span class="function">not</span> raw_files:
        <span class="function">print</span>(<span class="string">"❌ No raw data files found for EDA and cleaning!"</span>)
        <span class="function">print</span>(<span class="string">f"💡 Please ensure your raw CSV files are in: {config.RAW_DATA_FOLDER}"</span>)
        <span class="keyword">return</span>

    logger.<span class="function">info</span>(<span class="string">"🏠 Airbnb Data Warehouse ETL Pipeline"</span>)
    logger.<span class="function">info</span>(<span class="string">f"📅 Started at: {create_timestamp()}"</span>)
    logger.<span class="function">info</span>(<span class="string">"="</span> * <span class="number">50</span>)

    db_config = <span class="imports">DatabaseConfig</span>(config)

    <span class="keyword">while</span> <span class="keyword">True</span>:
        logger.<span class="function">info</span>(<span class="string">"\n📊 ETL Pipeline Options:"</span>)
        logger.<span class="function">info</span>(<span class="string">"1. 🔍 Run EDA Analysis (Extract & Analyze) - Uses RAW data"</span>)
        logger.<span class="function">info</span>(<span class="string">"2. 🧹 Run Data Cleaning (Transform) - RAW → Cleaned data"</span>)
        logger.<span class="function">info</span>(<span class="string">"3. 📥 Run SQL Server Data Loading (Load to Database) - Uses CLEANED data"</span>)
        logger.<span class="function">info</span>(<span class="string">"4. 🔄 Run Complete ETL Pipeline"</span>)
        logger.<span class="function">info</span>(<span class="string">"5. 🗃️ Database Management"</span>)
        logger.<span class="function">info</span>(<span class="string">"6. 🖼️ Create/Update Views"</span>)
        logger.<span class="function">info</span>(<span class="string">"7. 🚪 Exit"</span>)

        choice = <span class="function">input</span>(<span class="string">"\nEnter your choice (1-7): "</span>).<span class="function">strip</span>()

        <span class="keyword">if</span> choice == <span class="string">'1'</span>:
            <span class="function">run_eda_analysis</span>(config)
        <span class="keyword">elif</span> choice == <span class="string">'2'</span>:
            <span class="function">run_data_cleaning</span>(config)
        <span class="keyword">elif</span> choice == <span class="string">'3'</span>:
            <span class="function">run_sql_data_loading</span>(config, db_config)
        <span class="keyword">elif</span> choice == <span class="string">'4'</span>:
            <span class="function">run_complete_etl</span>(config, db_config)
        <span class="keyword">elif</span> choice == <span class="string">'5'</span>:
            <span class="function">run_database_management</span>(config, db_config)
        <span class="keyword">elif</span> choice == <span class="string">'6'</span>:
            <span class="function">run_create_views</span>(config, db_config)
        <span class="keyword">elif</span> choice == <span class="string">'7'</span>:
            logger.<span class="function">info</span>(<span class="string">"👋 Exiting ETL Pipeline. Goodbye!"</span>)
            <span class="keyword">break</span>
        <span class="keyword">else</span>:
            logger.<span class="function">warning</span>(<span class="string">"❌ Invalid choice. Please enter 1-7."</span>)


<span class="keyword">def</span> <span class="function">run_eda_analysis</span>(config: <span class="imports">Config</span>):
    <span class="string">"""Run Exploratory Data Analysis on raw data"""</span>
    logger.<span class="function">info</span>(<span class="string">"\n"</span> + <span class="string">"="</span>*<span class="number">60</span>)
    logger.<span class="function">info</span>(<span class="string">"🔍 STARTING EDA ANALYSIS (RAW DATA)"</span>)
    logger.<span class="function">info</span>(<span class="string">"="</span>*<span class="number">60</span>)

    <span class="comment"># Check for raw data files</span>
    raw_files = config.<span class="function">get_data_files</span>()
    <span class="keyword">if</span> <span class="function">not</span> raw_files:
        logger.<span class="function">error</span>(<span class="string">"❌ No raw data files found!"</span>)
        logger.<span class="function">info</span>(<span class="string">f"💡 Please ensure your raw CSV files are in: {config.RAW_DATA_FOLDER}"</span>)
        <span class="keyword">return</span>

    analyzer = <span class="imports">AirbnbDataAnalyzer</span>(config)
    analyzer.<span class="function">analyze_all_files</span>()


<span class="keyword">def</span> <span class="function">run_data_cleaning</span>(config: <span class="imports">Config</span>):
    <span class="string">"""Run data cleaning and transformation"""</span>
    logger.<span class="function">info</span>(<span class="string">"\n"</span> + <span class="string">"="</span>*<span class="number">60</span>)
    logger.<span class="function">info</span>(<span class="string">"🧹 STARTING DATA CLEANING & TRANSFORMATION"</span>)
    logger.<span class="function">info</span>(<span class="string">"="</span>*<span class="number">60</span>)

    <span class="comment"># Check for raw data files</span>
    raw_files = config.<span class="function">get_data_files</span>()
    <span class="keyword">if</span> <span class="function">not</span> raw_files:
        logger.<span class="function">error</span>(<span class="string">"❌ No raw data files found!"</span>)
        logger.<span class="function">info</span>(<span class="string">f"💡 Please ensure your raw CSV files are in: {config.RAW_DATA_FOLDER}"</span>)
        <span class="keyword">return</span>

    cleaner = <span class="imports">AirbnbDataCleaner</span>(config)
    cleaner.<span class="function">analyze_column_relevance</span>()
    response = <span class="function">input</span>(<span class="string">"\n🧹 Do you want to create cleaned datasets? (y/n): "</span>).<span class="function">lower</span>()
    <span class="keyword">if</span> response == <span class="string">'y'</span>:
        cleaner.<span class="function">create_cleaned_dataset</span>()
        logger.<span class="function">info</span>(<span class="string">"\n✅ Data cleaning completed!"</span>)

<span class="keyword">def</span> <span class="function">run_data_cleaning_non_interactive</span>(config: <span class="imports">Config</span>):
    <span class="string">"""Run data cleaning and transformation without user interaction."""</span>
    logger.<span class="function">info</span>(<span class="string">"\n"</span> + <span class="string">"="</span>*<span class="number">60</span>)
    logger.<span class="function">info</span>(<span class="string">"🧹 STARTING DATA CLEANING & TRANSFORMATION (NON-INTERACTIVE)"</span>)
    logger.<span class="function">info</span>(<span class="string">"="</span>*<span class="number">60</span>)

    <span class="comment"># Check for raw data files</span>
    raw_files = config.<span class="function">get_data_files</span>()
    <span class="keyword">if</span> <span class="function">not</span> raw_files:
        logger.<span class="function">error</span>(<span class="string">"❌ No raw data files found!"</span>)
        logger.<span class="function">info</span>(<span class="string">f"💡 Please ensure your raw CSV files are in: {config.RAW_DATA_FOLDER}"</span>)
        <span class="keyword">return</span>

    cleaner = <span class="imports">AirbnbDataCleaner</span>(config)
    cleaner.<span class="function">create_cleaned_dataset</span>()
    logger.<span class="function">info</span>(<span class="string">"\n✅ Data cleaning completed!"</span>)


<span class="keyword">def</span> <span class="function">run_sql_data_loading</span>(config: <span class="imports">Config</span>, db_config: <span class="imports">DatabaseConfig</span>):
    <span class="string">"""Load cleaned data into SQL Server data warehouse"""</span>
    logger.<span class="function">info</span>(<span class="string">"\n"</span> + <span class="string">"="</span>*<span class="number">60</span>)
    logger.<span class="function">info</span>(<span class="string">"📥 STARTING SQL SERVER DATA LOADING"</span>)
    logger.<span class="function">info</span>(<span class="string">"="</span>*<span class="number">60</span>)

    <span class="comment"># Check if cleaned data exists</span>
    cleaned_files = config.<span class="function">get_cleaned_data_files</span>()
    <span class="keyword">if</span> <span class="function">not</span> cleaned_files:
        logger.<span class="function">error</span>(<span class="string">"❌ No cleaned data files found!"</span>)

    loader = <span class="imports">AirbnbDataLoader</span>(config, db_config)

    logger.<span class="function">info</span>(<span class="string">'\nWhich load phase do you want to run?'</span>)
    logger.<span class="function">info</span>(<span class="string">'1. Listings'</span>)
    logger.<span class="function">info</span>(<span class="string">'2. Calendar'</span>)
    logger.<span class="function">info</span>(<span class="string">'3. Reviews'</span>)
    logger.<span class="function">info</span>(<span class="string">'4. All (Listings -> Calendar -> Reviews)'</span>)
    logger.<span class="function">info</span>(<span class="string">'5. Exit (return to main menu)'</span>)

    phase = <span class="function">input</span>(<span class="string">'Enter 1-5: '</span>).<span class="function">strip</span>()

    <span class="keyword">if</span> phase == <span class="string">'1'</span>:
        conn = db_config.<span class="function">create_connection</span>(database=config.SQL_DATABASE)
        <span class="keyword">try</span>:
            listings = glob.<span class="function">glob</span>(os.path.<span class="function">join</span>(config.CLEANED_DATA_FOLDER, <span class="string">'*listings*.csv.gz'</span>))
            <span class="keyword">for</span> f <span class="keyword">in</span> listings:
                loader.<span class="function">_load_listings_data</span>(conn, f)

            logger.<span class="function">info</span>(<span class="string">" ↳ Populating dim_hosts..."</span>)
            cursor = conn.<span class="function">cursor</span>()
            cursor.<span class="function">execute</span>(<span class="string">"TRUNCATE TABLE dim_hosts;"</span>)
            <span class="keyword">with</span> <span class="function">open</span>(<span class="string">'sql/data/02_load_hosts.sql'</span>, <span class="string">'r'</span>, encoding=<span class="string">'utf-8-sig'</span>) <span class="keyword">as</span> f:
                sql_script = f.<span class="function">read</span>()
            cursor.<span class="function">execute</span>(sql_script)
            <span class="comment"># Fetch the distinct hosts count</span>
            distinct_hosts_count = cursor.<span class="function">fetchone</span>()[<span class="number">0</span>]
            logger.<span class="function">info</span>(<span class="string">f"DEBUG: distinct_hosts_count = {distinct_hosts_count}"</span>)
            logger.<span class="function">info</span>(<span class="string">f" INFO: {distinct_hosts_count:,} distinct hosts found in dim_listings."</span>)
            <span class="comment"># Move to the next result set to get the inserted hosts count</span>
            cursor.<span class="function">nextset</span>()
            host_count = cursor.<span class="function">fetchone</span>()[<span class="number">0</span>]
            conn.<span class="function">commit</span>()
            logger.<span class="function">info</span>(<span class="string">f" ✅ dim_hosts populated: {host_count:,} hosts added."</span>)
        <span class="keyword">finally</span>:
            conn.<span class="function">close</span>()
    <span class="keyword">elif</span> phase == <span class="string">'2'</span>:
        conn = db_config.<span class="function">create_connection</span>(database=config.SQL_DATABASE)
        <span class="keyword">try</span>:
            logger.<span class="function">info</span>(<span class="string">" Clearing existing data from fact_calendar..."</span>)
            cursor = conn.<span class="function">cursor</span>()
            cursor.<span class="function">execute</span>(<span class="string">"TRUNCATE TABLE fact_calendar;"</span>)
            conn.<span class="function">commit</span>()
            logger.<span class="function">info</span>(<span class="string">" ✅ fact_calendar cleared successfully."</span>)
            calendars = glob.<span class="function">glob</span>(os.path.<span class="function">join</span>(config.CLEANED_DATA_FOLDER, <span class="string">'*calendar*.csv.gz'</span>))
            <span class="keyword">for</span> f <span class="keyword">in</span> calendars:
                loader.<span class="function">_load_calendar_data</span>(conn, f)
        <span class="keyword">finally</span>:
            conn.<span class="function">close</span>()
    <span class="keyword">elif</span> phase == <span class="string">'3'</span>:
        conn = db_config.<span class="function">create_connection</span>(database=config.SQL_DATABASE)
        <span class="keyword">try</span>:
            logger.<span class="function">info</span>(<span class="string">" Clearing existing data from fact_reviews..."</span>)
            cursor = conn.<span class="function">cursor</span>()
            cursor.<span class="function">execute</span>(<span class="string">"TRUNCATE TABLE fact_reviews;"</span>)
            conn.<span class="function">commit</span>()
            logger.<span class="function">info</span>(<span class="string">" ✅ fact_reviews cleared successfully."</span>)
            reviews = glob.<span class="function">glob</span>(os.path.<span class="function">join</span>(config.CLEANED_DATA_FOLDER, <span class="string">'*reviews*.csv.gz'</span>))
            <span class="keyword">for</span> f <span class="keyword">in</span> reviews:
                loader.<span class="function">_load_reviews_data</span>(conn, f)
        <span class="keyword">finally</span>:
            conn.<span class="function">close</span>()
    <span class="keyword">elif</span> phase == <span class="string">'4'</span>:
        loader.<span class="function">load_to_warehouse</span>()
    <span class="keyword">elif</span> phase == <span class="string">'5'</span>:
        logger.<span class="function">info</span>(<span class="string">'↩️ Returning to main menu without running SQL load'</span>)
        <span class="keyword">return</span>
    <span class="keyword">else</span>:
        logger.<span class="function">warning</span>(<span class="string">'Invalid choice, aborting SQL load'</span>)


<span class="keyword">def</span> <span class="function">run_complete_etl</span>(config: <span class="imports">Config</span>, db_config: <span class="imports">DatabaseConfig</span>):
    <span class="string">"""Run the complete ETL pipeline"""</span>
    logger.<span class="function">info</span>(<span class="string">"\n"</span> + <span class="string">"="</span>*<span class="number">60</span>)
    logger.<span class="function">info</span>(<span class="string">"🔄 STARTING COMPLETE ETL PIPELINE"</span>)
    logger.<span class="function">info</span>(<span class="string">"="</span>*<span class="number">60</span>)

    <span class="comment"># Extract & Analyze</span>
    logger.<span class="function">info</span>(<span class="string">"\n📊 STEP 1: EDA ANALYSIS (RAW DATA)"</span>)
    raw_files = config.<span class="function">get_data_files</span>()
    <span class="keyword">if</span> <span class="function">not</span> raw_files:
        logger.<span class="function">error</span>(<span class="string">"❌ No raw data files found!"</span>)
        <span class="keyword">return</span>
    analyzer = <span class="imports">AirbnbDataAnalyzer</span>(config)
    analyzer.<span class="function">analyze_all_files</span>()

    <span class="comment"># Transform</span>
    logger.<span class="function">info</span>(<span class="string">"\n🔄 STEP 2: DATA CLEANING & TRANSFORMATION"</span>)
    cleaner = <span class="imports">AirbnbDataCleaner</span>(config)
    cleaner.<span class="function">create_cleaned_dataset</span>()

    <span class="comment"># Load to SQL Server</span>
    logger.<span class="function">info</span>(<span class="string">"\n📥 STEP 3: SQL SERVER DATA LOADING (CLEANED DATA)"</span>)
    <span class="comment"># Check if cleaned data exists and update SQL scripts</span>
    cleaned_files = config.<span class="function">get_cleaned_data_files</span>()
    <span class="keyword">if</span> <span class="function">not</span> cleaned_files:
        logger.<span class="function">error</span>(<span class="string">"❌ No cleaned data files found after cleaning step!"</span>)
        <span class="keyword">return</span>
    loader = <span class="imports">AirbnbDataLoader</span>(config, db_config)
    loader.<span class="function">load_to_warehouse</span>()

    logger.<span class="function">info</span>(<span class="string">"\n✅ ETL PIPELINE COMPLETED SUCCESSFULLY!"</span>)


<span class="keyword">def</span> <span class="function">run_create_views</span>(config: <span class="imports">Config</span>, db_config: <span class="imports">DatabaseConfig</span>):
    <span class="string">"""Create or update the database views"""</span>
    logger.<span class="function">info</span>(<span class="string">"\n"</span> + <span class="string">"="</span>*<span class="number">60</span>)
    logger.<span class="function">info</span>(<span class="string">"🖼️ CREATING/UPDATING DATABASE VIEWS"</span>)
    logger.<span class="function">info</span>(<span class="string">"="</span>*<span class="number">60</span>)

    <span class="keyword">if</span> <span class="function">not</span> db_config.<span class="function">database_exists</span>():
        logger.<span class="function">warning</span>(<span class="string">f"❌ Database '{config.SQL_DATABASE}' does not exist"</span>)
        <span class="keyword">return</span>

    <span class="keyword">try</span>:
        conn = db_config.<span class="function">create_connection</span>(config.SQL_DATABASE)
        loader = <span class="imports">AirbnbDataLoader</span>(config, db_config)
        loader.<span class="function">create_views</span>(conn)
        logger.<span class="function">info</span>(<span class="string">"✅ Views created/updated successfully."</span>)
    <span class="keyword">except</span> Exception <span class="keyword">as</span> e:
        logger.<span class="function">error</span>(<span class="string">f"❌ Error creating views: {e}"</span>)
    <span class="keyword">finally</span>:
        <span class="keyword">if</span> <span class="string">'conn'</span> <span class="keyword">in</span> <span class="function">locals</span>() <span class="keyword">and</span> conn:
            conn.<span class="function">close</span>()


<span class="keyword">def</span> <span class="function">run_database_management</span>(config: <span class="imports">Config</span>, db_config: <span class="imports">DatabaseConfig</span>):
    <span class="string">"""Database management operations"""</span>
    logger.<span class="function">info</span>(<span class="string">"\n"</span> + <span class="string">"="</span>*<span class="number">60</span>)
    logger.<span class="function">info</span>(<span class="string">"🗃️ DATABASE MANAGEMENT"</span>)
    logger.<span class="function">info</span>(<span class="string">"="</span>*<span class="number">60</span>)

    logger.<span class="function">info</span>(<span class="string">"\n📊 Database Operations:"</span>)
    logger.<span class="function">info</span>(<span class="string">"1. 🔍 Test Database Connection"</span>)
    logger.<span class="function">info</span>(<span class="string">"2. 📋 Check Database Status"</span>)
    logger.<span class="function">info</span>(<span class="string">"3. 🗑️ Reset Database (Drop & Recreate)"</span>)
    logger.<span class="function">info</span>(<span class="string">"4. 📈 View Database Statistics"</span>)
    logger.<span class="function">info</span>(<span class="string">"5. ↩️ Back to Main Menu"</span>)

    choice = <span class="function">input</span>(<span class="string">"\nEnter your choice (1-5): "</span>).<span class="function">strip</span>()

    <span class="keyword">if</span> choice == <span class="string">'1'</span>:
        <span class="function">test_database_connection</span>(db_config)
    <span class="keyword">elif</span> choice == <span class="string">'2'</span>:
        <span class="function">check_database_status</span>(db_config, config)
    <span class="keyword">elif</span> choice == <span class="string">'3'</span>:
        <span class="function">reset_database</span>(db_config, config)
    <span class="keyword">elif</span> choice == <span class="string">'4'</span>:
        <span class="function">view_database_stats</span>(db_config, config)
    <span class="keyword">elif</span> choice == <span class="string">'5'</span>:
        <span class="keyword">return</span>
    <span class="keyword">else</span>:
        logger.<span class="function">warning</span>(<span class="string">"❌ Invalid choice."</span>)


<span class="keyword">def</span> <span class="function">test_database_connection</span>(db_config: <span class="imports">DatabaseConfig</span>):
    <span class="string">"""Test SQL Server connection"""</span>
    logger.<span class="function">info</span>(<span class="string">"\n🔌 Testing Database Connection..."</span>)
    <span class="keyword">if</span> db_config.<span class="function">test_connection</span>():
        logger.<span class="function">info</span>(<span class="string">"✅ Database connection successful!"</span>)
    <span class="keyword">else</span>:
        logger.<span class="function">error</span>(<span class="string">"❌ Database connection failed!"</span>)
        logger.<span class="function">info</span>(<span class="string">"\n🔧 Troubleshooting tips:"</span>)
        logger.<span class="function">info</span>(<span class="string">"• Ensure SQL Server Express is running"</span>)
        logger.<span class="function">info</span>(<span class="string">"• Verify ODBC Driver 17 is installed"</span>)
        logger.<span class="function">info</span>(<span class="string">"• Check if the server name is correct"</span>)
        logger.<span class="function">info</span>(<span class="string">"• Ensure Windows Authentication is enabled"</span>)


<span class="keyword">def</span> <span class="function">check_database_status</span>(db_config: <span class="imports">DatabaseConfig</span>, config: <span class="imports">Config</span>):
    <span class="string">"""Check if data warehouse database exists and its status"""</span>
    logger.<span class="function">info</span>(<span class="string">"\n📋 Checking Database Status..."</span>)
    <span class="keyword">if</span> <span class="function">not</span> db_config.<span class="function">test_connection</span>():
        logger.<span class="function">error</span>(<span class="string">"❌ Cannot connect to SQL Server"</span>)
        <span class="keyword">return</span>

    <span class="keyword">try</span>:
        conn = db_config.<span class="function">create_connection</span>(<span class="string">'master'</span>)
        cursor = conn.<span class="function">cursor</span>()

        <span class="comment"># Check if database exists</span>
        cursor.<span class="function">execute</span>(<span class="string">f"SELECT name, state_desc FROM sys.databases WHERE name = '{config.SQL_DATABASE}'"</span>)
        db_info = cursor.<span class="function">fetchone</span>()

        <span class="keyword">if</span> db_info:
            logger.<span class="function">info</span>(<span class="string">f"✅ Database '{db_info[0]}' exists - Status: {db_info[1]}"</span>)

            <span class="comment"># Check tables</span>
            conn_airbnb = db_config.<span class="function">create_connection</span>(config.SQL_DATABASE)
            cursor_airbnb = conn_airbnb.<span class="function">cursor</span>()
            cursor_airbnb.<span class="function">execute</span>(<span class="string">"""
                SELECT TABLE_NAME, TABLE_TYPE
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_TYPE = 'BASE TABLE'
                ORDER BY TABLE_NAME
            """</span>)
            tables = cursor_airbnb.<span class="function">fetchall</span>()
            logger.<span class="function">info</span>(<span class="string">f"\n📊 Tables in database: {len(tables)}"</span>)
            <span class="keyword">for</span> table <span class="keyword">in</span> tables:
                cursor_airbnb.<span class="function">execute</span>(<span class="string">f"SELECT COUNT(*) FROM {table[0]}"</span>)
                count = cursor_airbnb.<span class="function">fetchone</span>()[<span class="number">0</span>]
                logger.<span class="function">info</span>(<span class="string">f" • {table[0]}: {count:,} records"</span>)
            conn_airbnb.<span class="function">close</span>()
        <span class="keyword">else</span>:
            logger.<span class="function">warning</span>(<span class="string">f"❌ Database '{config.SQL_DATABASE}' does not exist"</span>)
            logger.<span class="function">info</span>(<span class="string">"💡 Run 'SQL Server Data Loading' to create it"</span>)
        conn.<span class="function">close</span>()
    <span class="keyword">except</span> Exception <span class="keyword">as</span> e:
        logger.<span class="function">error</span>(<span class="string">f"❌ Error checking database status: {e}"</span>)


<span class="keyword">def</span> <span class="function">reset_database</span>(db_config: <span class="imports">DatabaseConfig</span>, config: <span class="imports">Config</span>, interactive: <span class="imports">bool</span> = <span class="keyword">True</span>):
    <span class="string">"""Reset the entire database"""</span>
    <span class="keyword">if</span> interactive:
        logger.<span class="function">warning</span>(<span class="string">"\n⚠️ RESET DATABASE"</span>)
        logger.<span class="function">warning</span>(<span class="string">f"This will DROP and RECREATE the entire {config.SQL_DATABASE}!"</span>)
        confirmation = <span class="function">input</span>(<span class="string">"Type 'YES' to confirm: "</span>).<span class="function">strip</span>()
        <span class="keyword">if</span> confirmation != <span class="string">'YES'</span>:
            logger.<span class="function">info</span>(<span class="string">"❌ Reset cancelled"</span>)
            <span class="keyword">return</span>

    <span class="keyword">try</span>:
        conn = db_config.<span class="function">create_connection</span>(<span class="string">'master'</span>)
        conn.autocommit = <span class="keyword">True</span>
        cursor = conn.<span class="function">cursor</span>()

        cursor.<span class="function">execute</span>(<span class="string">f"""
            IF EXISTS (SELECT name FROM sys.databases WHERE name = '{config.SQL_DATABASE}')
            BEGIN
                ALTER DATABASE {config.SQL_DATABASE} SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                DROP DATABASE {config.SQL_DATABASE};
            END
        """</span>)
        <span class="comment"># Verify that the database was dropped</span>
        cursor.<span class="function">execute</span>(<span class="string">f"SELECT name FROM sys.databases WHERE name = '{config.SQL_DATABASE}'"</span>)
        <span class="keyword">if</span> cursor.<span class="function">fetchone</span>() <span class="keyword">is</span> <span class="keyword">not</span> <span class="keyword">None</span>:
            logger.<span class="function">error</span>(<span class="string">f"❌ Failed to drop database '{config.SQL_DATABASE}'. It still exists."</span>)
            conn.<span class="function">close</span>()
            <span class="keyword">return</span>
        logger.<span class="function">info</span>(<span class="string">"✅ Database dropped successfully."</span>)
        conn.<span class="function">close</span>()

        <span class="comment"># Recreate database</span>
        db_config.<span class="function">create_database</span>()
        logger.<span class="function">info</span>(<span class="string">"✅ Database recreated successfully."</span>)

        <span class="comment"># Re-apply the schema</span>
        logger.<span class="function">info</span>(<span class="string">"Applying schema to the new database..."</span>)
        conn_airbnb = db_config.<span class="function">create_connection</span>(config.SQL_DATABASE)
        loader = <span class="imports">AirbnbDataLoader</span>(config, db_config)
        loader.<span class="function">_execute_schema_scripts</span>(conn_airbnb)
        conn_airbnb.<span class="function">close</span>()
        logger.<span class="function">info</span>(<span class="string">"✅ Schema applied successfully."</span>)

    <span class="keyword">except</span> Exception <span class="keyword">as</span> e:
        logger.<span class="function">error</span>(<span class="string">f"❌ Error resetting database: {e}"</span>)

<span class="keyword">def</span> <span class="function">reset_database_non_interactive</span>(db_config: <span class="imports">DatabaseConfig</span>, config: <span class="imports">Config</span>):
    <span class="string">"""Reset the entire database without user interaction."""</span>
    <span class="function">reset_database</span>(db_config, config, interactive=<span class="keyword">False</span>)


<span class="keyword">def</span> <span class="function">view_database_stats</span>(db_config: <span class="imports">DatabaseConfig</span>, config: <span class="imports">Config</span>):
    <span class="string">"""View database statistics and sizes"""</span>
    logger.<span class="function">info</span>(<span class="string">"\n"</span> + <span class="string">"="</span>*<span class="number">60</span>)
    logger.<span class="function">info</span>(<span class="string">"📈 Database Statistics"</span>)
    logger.<span class="function">info</span>(<span class="string">"="</span>*<span class="number">60</span>)

    <span class="keyword">if</span> <span class="function">not</span> db_config.<span class="function">database_exists</span>():
        logger.<span class="function">warning</span>(<span class="string">f"❌ Database '{config.SQL_DATABASE}' does not exist"</span>)
        <span class="keyword">return</span>

    <span class="keyword">try</span>:
        conn = db_config.<span class="function">create_connection</span>(config.SQL_DATABASE)
        cursor = conn.<span class="function">cursor</span>()

        <span class="comment"># Database size</span>
        cursor.<span class="function">execute</span>(<span class="string">"""
            SELECT
                DB_NAME() AS DatabaseName,
                SUM(size * 8.0 / 1024) AS SizeMB
            FROM sys.database_files
            WHERE type = 0 -- ROWS data files only
        """</span>)
        db_size = cursor.<span class="function">fetchone</span>()
        logger.<span class="function">info</span>(<span class="string">f"💾 Database Size: {db_size[1]:.2f} MB"</span>)

        <span class="comment"># Table row counts</span>
        logger.<span class="function">info</span>(<span class="string">"\n📊 Table Statistics:"</span>)
        cursor.<span class="function">execute</span>(<span class="string">"""
            SELECT
                t.name AS TableName,
                p.rows AS RowCounts,
                SUM(a.total_pages) * 8 AS TotalSpaceKB
            FROM
                sys.tables t
            INNER JOIN
                sys.indexes i ON t.object_id = i.object_id
            INNER JOIN
                sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
            INNER JOIN
                sys.allocation_units a ON p.partition_id = a.container_id
            WHERE
                t.name NOT LIKE 'dt%' AND i.object_id > 255 AND i.index_id <= 1
            GROUP BY
                t.name, p.rows
            ORDER BY
                p.rows DESC
        """</span>)
        tables = cursor.<span class="function">fetchall</span>()
        <span class="keyword">for</span> table <span class="keyword">in</span> tables:
            logger.<span class="function">info</span>(<span class="string">f" • {table[0]}: {table[1]:,} rows ({table[2]/1024:.1f} MB)"</span>)
        conn.<span class="function">close</span>()
    <span class="keyword">except</span> Exception <span class="keyword">as</span> e:
        logger.<span class="function">error</span>(<span class="string">f"❌ Error viewing database stats: {e}"</span>)


<span class="keyword">def</span> <span class="function">run_sql_data_loading_non_interactive</span>(config: <span class="imports">Config</span>, db_config: <span class="imports">DatabaseConfig</span>):
    <span class="string">"""Load cleaned data into SQL Server data warehouse without user interaction."""</span>
    logger.<span class="function">info</span>(<span class="string">"\n"</span> + <span class="string">"="</span>*<span class="number">60</span>)
    logger.<span class="function">info</span>(<span class="string">"📥 STARTING SQL SERVER DATA LOADING (NON-INTERACTIVE)"</span>)
    logger.<span class="function">info</span>(<span class="string">"="</span>*<span class="number">60</span>)

    <span class="comment"># Check if cleaned data exists</span>
    cleaned_files = config.<span class="function">get_cleaned_data_files</span>()
    <span class="keyword">if</span> <span class="function">not</span> cleaned_files:
        logger.<span class="function">error</span>(<span class="string">"❌ No cleaned data files found!"</span>)
        <span class="keyword">return</span>

    loader = <span class="imports">AirbnbDataLoader</span>(config, db_config)
    loader.<span class="function">load_to_warehouse</span>()


<span class="keyword">if</span> __name__ == <span class="string">"__main__"</span>:
    <span class="function">main</span>()
</code></pre>
  </div>
</div>

<div class="code-window">
    <div class="code-header">
      <span class="red"></span>
      <span class="yellow"></span>
      <span class="green"></span>
    </div>
    <div class="code-body">
<pre><code>
<span class="sql-comment">-- sql/data/05_load_reviews.sql</span>
<span class="sql-keyword">USE</span> AirbnbDataWarehouse;

<span class="sql-comment">-- Disable foreign key constraints</span>
<span class="sql-keyword">ALTER</span> <span class="sql-keyword">TABLE</span> fact_reviews <span class="sql-keyword">NOCHECK</span> <span class="sql-keyword">CONSTRAINT</span> <span class="sql-keyword">ALL</span>;

<span class="sql-comment">-- Create temporary staging table</span>
<span class="sql-keyword">CREATE</span> <span class="sql-keyword">TABLE</span> #temp_reviews (
    listing_id <span class="sql-datatype">NVARCHAR</span>(<span class="sql-keyword">MAX</span>),
    id <span class="sql-datatype">NVARCHAR</span>(<span class="sql-keyword">MAX</span>),
    date <span class="sql-datatype">NVARCHAR</span>(<span class="sql-number">50</span>),
    reviewer_id <span class="sql-datatype">NVARCHAR</span>(<span class="sql-keyword">MAX</span>),
    reviewer_name <span class="sql-datatype">NVARCHAR</span>(<span class="sql-keyword">MAX</span>),
    comments <span class="sql-datatype">NVARCHAR</span>(<span class="sql-keyword">MAX</span>)
);

<span class="sql-comment">-- Dynamic file path (will be replaced by Python)</span>
<span class="sql-comment">-- Perform BULK INSERT directly using the provided file path</span>
<span class="sql-keyword">BULK</span> <span class="sql-keyword">INSERT</span> #temp_reviews
<span class="sql-keyword">FROM</span> <span class="sql-string">'{{REVIEWS_FILE_PATH}}'</span>
<span class="sql-keyword">WITH</span> (
    FIRSTROW = <span class="sql-number">2</span>,
    FIELDTERMINATOR = <span class="sql-string">'|'</span>,
    ROWTERMINATOR = <span class="sql-string">'0x0a'</span>,
    TABLOCK,
    CODEPAGE = <span class="sql-string">'65001'</span>
);

<span class="sql-comment">-- Table to hold the IDs of inserted rows</span>
<span class="sql-keyword">IF</span> <span class="sql-function">OBJECT_ID</span>(<span class="sql-string">'tempdb..#inserted_review_ids'</span>) <span class="sql-keyword">IS</span> <span class="sql-keyword">NOT</span> <span class="sql-keyword">NULL</span>
    <span class="sql-keyword">DROP</span> <span class="sql-keyword">TABLE</span> #inserted_review_ids;
<span class="sql-keyword">CREATE</span> <span class="sql-keyword">TABLE</span> #inserted_review_ids (review_id <span class="sql-datatype">BIGINT</span>);

<span class="sql-comment">-- Insert into fact table with proper data types and date mapping</span>
;WITH reviews_cte <span class="sql-keyword">AS</span> (
    <span class="sql-keyword">SELECT</span>
        <span class="sql-function">TRY_CAST</span>(r.id <span class="sql-keyword">AS</span> <span class="sql-datatype">BIGINT</span>) <span class="sql-keyword">AS</span> id,
        <span class="sql-function">TRY_CAST</span>(r.listing_id <span class="sql-keyword">AS</span> <span class="sql-datatype">BIGINT</span>) <span class="sql-keyword">AS</span> listing_id,
        d.date_id,
        <span class="sql-function">TRY_CAST</span>(r.reviewer_id <span class="sql-keyword">AS</span> <span class="sql-datatype">BIGINT</span>) <span class="sql-keyword">AS</span> reviewer_id,
        <span class="sql-function">LEFT</span>(r.reviewer_name, <span class="sql-number">255</span>) <span class="sql-keyword">AS</span> reviewer_name,
        r.comments,
        <span class="sql-function">ROW_NUMBER</span>() <span class="sql-keyword">OVER</span>(<span class="sql-keyword">PARTITION</span> <span class="sql-keyword">BY</span> r.id <span class="sql-keyword">ORDER</span> <span class="sql-keyword">BY</span> (<span class="sql-keyword">SELECT</span> <span class="sql-keyword">NULL</span>)) <span class="sql-keyword">as</span> rn
    <span class="sql-keyword">FROM</span> #temp_reviews r
    <span class="sql-keyword">INNER</span> <span class="sql-keyword">JOIN</span> dim_dates d <span class="sql-keyword">ON</span> <span class="sql-function">TRY_CONVERT</span>(<span class="sql-datatype">DATE</span>, r.date) = d.full_date
    <span class="sql-keyword">INNER</span> <span class="sql-keyword">JOIN</span> dim_listings l <span class="sql-keyword">ON</span> <span class="sql-function">TRY_CAST</span>(r.listing_id <span class="sql-keyword">AS</span> <span class="sql-datatype">BIGINT</span>) = l.listing_id
)
<span class="sql-keyword">INSERT</span> <span class="sql-keyword">INTO</span> fact_reviews (review_id, listing_id, date_id, reviewer_id, reviewer_name, comments)
<span class="sql-keyword">OUTPUT</span> inserted.review_id <span class="sql-keyword">INTO</span> #inserted_review_ids
<span class="sql-keyword">SELECT</span>
    cte.id,
    cte.listing_id,
    cte.date_id,
    cte.reviewer_id,
    cte.reviewer_name,
    cte.comments
<span class="sql-keyword">FROM</span> reviews_cte cte
<span class="sql-keyword">LEFT</span> <span class="sql-keyword">JOIN</span> fact_reviews fr <span class="sql-keyword">ON</span> cte.id = fr.review_id
<span class="sql-keyword">WHERE</span> cte.rn = <span class="sql-number">1</span> <span class="sql-keyword">AND</span> fr.review_id <span class="sql-keyword">IS</span> <span class="sql-keyword">NULL</span>;

<span class="sql-comment">-- Return the count of inserted rows</span>
<span class="sql-keyword">SELECT</span> <span class="sql-function">COUNT</span>(*) <span class="sql-keyword">AS</span> inserted_review_rows <span class="sql-keyword">FROM</span> #inserted_review_ids;

<span class="sql-comment">-- Drop temporary table</span>
<span class="sql-keyword">DROP</span> <span class="sql-keyword">TABLE</span> #temp_reviews;

<span class="sql-comment">-- Re-enable foreign key constraints</span>
<span class="sql-keyword">ALTER</span> <span class="sql-keyword">TABLE</span> fact_reviews <span class="sql-keyword">WITH</span> <span class="sql-keyword">CHECK</span> <span class="sql-keyword">CHECK</span> <span class="sql-keyword">CONSTRAINT</span> <span class="sql-keyword">ALL</span>;
</code></pre>
    </div>
  </div>
</div>
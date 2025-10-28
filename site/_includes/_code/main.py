# main.py
from modules.web_scraper import fetch_page_content, extract_city_sections
from modules.file_manager import create_download_folder, get_existing_files_info
from modules.download_manager import check_for_updates, download_file
from utils.helpers import extract_country_from_url, show_version_summary

def main():
    print("🚀 Starting Airbnb Data Downloader...")

    # Interactive folder setup
    print("\n📁 Please specify where to save the data:")
    download_folder = create_download_folder()

    # Scrape website
    print("\n🌐 Fetching data from Inside Airbnb...")
    soup = fetch_page_content()
    city_sections = extract_city_sections(soup)

    if not city_sections:
        print("❌ No city sections found")
        return

    print(f"📋 Found {len(city_sections)} city sections on the website")

    # Check existing files
    existing_files_info = get_existing_files_info(download_folder)
    print(f"📁 Found {len(existing_files_info)} existing files in folder")

    # Check for updates
    files_to_download = check_for_updates(city_sections, existing_files_info, extract_country_from_url)

    if not files_to_download:
        print("✅ All files are up to date - no downloads needed")
        return

    print(f"\n📥 Found {len(files_to_download)} files to download/update")

    # Download files
    success_count = 0
    for i, (file_url, city_name, file_type, website_date, reason) in enumerate(files_to_download, 1):
        try:
            country = extract_country_from_url(file_url)  # ← This will work now
            print(f"[{i}/{len(files_to_download)}] Downloading: {reason}")
            filename, file_size_mb = download_file(
                file_url,
                download_folder,
                country,
                city_name,
                file_type,
                website_date
            )
            print(f" ✓ Downloaded: {filename} ({file_size_mb:.2f} MB)")
            success_count += 1
        except Exception as e:
            print(f" ✗ Error: {str(e)}")

    # Summary
    print(f"\n✅ Download completed!")
    print(f"📊 Successfully downloaded: {success_count}/{len(files_to_download)} files")
    show_version_summary(download_folder)

if __name__ == "__main__":
    main()

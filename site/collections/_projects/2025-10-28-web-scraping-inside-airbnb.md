---
title: I.    Web-scraping-and-dataset-download-Inside-Airbnb
date: 2025-10-28
github_url: https://github.com/YassineEng/Web-scraping-and-dataset-download-Inside-Airbnb
---

<!-- Badges (must be outside YAML front matter) -->
<div style="margin-left: 20px;">
  <img src="https://img.shields.io/badge/Python-3.x-blue?logo=python">
  <img src="https://img.shields.io/badge/BeautifulSoup-HTML%20Parser-green?logo=html5">
  <img src="https://img.shields.io/badge/Requests-HTTP%20Client-blue">
  <img src="https://img.shields.io/badge/UV-Package%20Manager-purple">
</div>


<p style="margin-left: 20px;">Web Scraping and Data download from Airbnb Inside Website. A comprehensive web scraping project for Airbnb insights website "csv.gz" data collection. This modular Python application automatically checks for data updates and maintains versioned datasets each time you run it.</p>
<ul style="margin-left: 60px;">
  <li>🤖 Automated Data Collection: Scrapes Airbnb "csv.gz" data files from InsideAirbnb.com</li>
  <li>🔍 Smart Update Detection: Compares website dates with local files downloaded previously to download only new data</li>
  <li>📊 Version Control: Maintains multiple versions of datasets with dates in filenames</li>
  <li>🏗️ Modular Architecture: Clean, maintainable code structure with concerns separated</li>
</ul>

<div class="code-window single-code-window">
  <div class="code-header">
    <span class="red"></span>
    <span class="yellow"></span>
    <span class="green"></span>
  </div>
  <div class="code-body">
<pre><code>
<span class="comment"># main.py</span>
<span class="imports">from</span> modules.web_scraper <span class="imports">import</span> fetch_page_content, extract_city_sections
<span class="imports">from</span> modules.file_manager <span class="imports">import</span> create_download_folder, get_existing_files_info
<span class="imports">from</span> modules.download_manager <span class="imports">import</span> check_for_updates, download_file
<span class="imports">from</span> utils.helpers <span class="imports">import</span> extract_country_from_url, show_version_summary

<span class="keyword">def</span> <span class="function">main</span>():
    <span class="function">print</span>(<span class="string">"🚀 Starting Airbnb Data Downloader..."</span>)

    <span class="comment"># Interactive folder setup</span>
    <span class="function">print</span>(<span class="string">"\n📁 Please specify where to save the data:"</span>)
    download_folder = <span class="function">create_download_folder</span>()

    <span class="comment"># Scrape website</span>
    <span class="function">print</span>(<span class="string">"\n🌐 Fetching data from Inside Airbnb..."</span>)
    soup = <span class="function">fetch_page_content</span>()
    city_sections = <span class="function">extract_city_sections</span>(soup)
    <span class="keyword">if</span> <span class="function">not</span> city_sections:
        <span class="function">print</span>(<span class="string">"❌ No city sections found"</span>)
        <span class="keyword">return</span>

    <span class="function">print</span>(<span class="string">f"📋 Found {len(city_sections)} city sections on the website"</span>)

    <span class="comment"># Check existing files</span>
    existing_files_info = <span class="function">get_existing_files_info</span>(download_folder)
    <span class="function">print</span>(<span class="string">f"📁 Found {len(existing_files_info)} existing files in folder"</span>)

    <span class="comment"># Check for updates</span>
    files_to_download = <span class="function">check_for_updates</span>(
        city_sections, existing_files_info, extract_country_from_url
    )

    <span class="keyword">if</span> <span class="function">not</span> files_to_download:
        <span class="function">print</span>(<span class="string">"✅ All files are up to date - no downloads needed"</span>)
        <span class="keyword">return</span>

    <span class="function">print</span>(<span class="string">f"\n📥 Found {len(files_to_download)} files to download/update"</span>)

    <span class="comment"># Download files</span>
    success_count = <span class="number">0</span>
    <span class="keyword">for</span> i, (file_url, city_name, file_type, website_date, reason) <span class="imports">in</span> <span class="function">enumerate</span>(files_to_download, <span class="number">1</span>):
        <span class="keyword">try</span>:
            country = <span class="function">extract_country_from_url</span>(file_url)
            <span class="function">print</span>(<span class="string">f"[{i}/{len(files_to_download)}] Downloading: {reason}"</span>)
            filename, file_size_mb = <span class="function">download_file</span>(
                file_url, download_folder, country, city_name, file_type, website_date
            )
            <span class="function">print</span>(<span class="string">f" ✓ Downloaded: {filename} ({file_size_mb:.2f} MB)"</span>)
            success_count += <span class="number">1</span>
        <span class="keyword">except</span> Exception <span class="imports">as</span> e:
            <span class="function">print</span>(<span class="string">f" ✗ Error: {str(e)}"</span>)

    <span class="comment"># Summary</span>
    <span class="function">print</span>(<span class="string">f"\n✅ Download completed!"</span>)
    <span class="function">print</span>(<span class="string">f"📊 Successfully downloaded: {success_count}/{len(files_to_download)} files"</span>)
    <span class="function">show_version_summary</span>(download_folder)

<span class="keyword">if</span> __name__ == <span class="string">"__main__"</span>:
    <span class="function">main</span>()
</code></pre>
  </div>
</div>
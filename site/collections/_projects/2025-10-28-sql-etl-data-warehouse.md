---
title: SQL-ETL-data-warehouse-Inside-Airbnb
date: 2025-10-28
github_url: https://github.com/YassineEng/SQL-ETL-data-warehouse-Inside-Airbnb
order: 2
---

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
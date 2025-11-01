---
title: V. EDA-XGBoost-price-predection-Airbnb-Data
date: 2025-11-01
github_url: https://github.com/YassineEng/EDA-XGBoost--price-predection-Airbnb-Data
order: 5
---

<!-- Badges (must be outside YAML front matter) -->
<div style="margin-left: 20px;">
  <img src="https://img.shields.io/badge/Python-3.x-blue?logo=python">
  <img src="https://img.shields.io/badge/Pandas-Library-150458?logo=pandas">
  <img src="https://img.shields.io/badge/XGBoost-Model-820000?logo=xgboost">
  <img src="https://img.shields.io/badge/Matplotlib-Library-blue?logo=matplotlib">
  <img src="https://img.shields.io/badge/Seaborn-Library-blue?logo=seaborn">
  <img src="https://img.shields.io/badge/Scikit--learn-ML-orange?logo=scikit-learn">
</div>

<p style="margin-left: 20px;">The project is structured as a data pipeline that processes raw Airbnb data into a format suitable for analysis and modeling. The pipeline consists of the following steps:</p>

<ul style="margin-left: 60px;">
  <li>⚙️ Data Extraction and Feature Engineering: Data is extracted from a SQL Server database. A SQL view (vw_listing_features) is created to join calendar and listing data and to engineer new features. This view is then materialized into a table (listing_features) for performance.</li>
  <li>📊 Exploratory Data Analysis (EDA): The 01_EDA.ipynb notebook performs a thorough exploratory data analysis of the dataset. This includes visualizing data distributions, identifying correlations, and gaining insights into the factors that influence Airbnb prices.</li>
  <li>📈 Price Prediction Modeling: The 02_price_pred.ipynb notebook builds a regression model to predict Airbnb listing prices. This involves feature selection, model training, and evaluation.</li>
</ul>

  <!-- Binder Launch Button -->
  <a href="https://mybinder.org/v2/gh/YassineEng/EDA-XGBoost--price-predection-Airbnb-Data/main?filepath=notebooks/02_price_pred.ipynb" target="_blank" style="display:inline-block; margin: 15px 0;">
    <img src="https://mybinder.org/badge_logo.svg" alt="Launch Binder" style="height: 20px;">
  </a>

  <!-- Static Notebook Preview -->
  <div class="code-window-container">
    <div class="code-window">
      <div class="code-header">
        <span class="red"></span>
        <span class="yellow"></span>
        <span class="green"></span>
      </div>
      <div class="code-body">
        <iframe
          src="https://nbviewer.org/github/YassineEng/EDA-XGBoost--price-predection-Airbnb-Data/blob/main/notebooks/02_price_pred.ipynb"
          width="100%"
          height="800"
          frameborder="0"
          style="border-radius: 12px; box-shadow: 0 0 10px rgba(0,0,0,0.1); margin-top: 20px;">
        </iframe>
      </div>
    </div>
  </div>
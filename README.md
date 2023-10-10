# Bundled-Up Bike Trips in Montreal

This repository contains data and Python scripts for capturing information on Montreal cyclist counts and weather. 

The following are the files contained in this repository:
- Source data files
  - comptage_velo_2022.csv (Count of bike passages in Montreal, 2022)
  - localisation_des_compteurs_velo.csv (Information on bike sensors in Montreal)
  - en_climate_daily_QC_7025251_2022_P1D.csv (Canada Weather, 2022)
- Python scripts
  - load_passage.py (Load passage data)
  - load_sensor.py (Load bike counter data)
  - load_weather.py (Load weather data)
  - montreal_cyclist_transform.py (Transform data - part 1)
  - analytics_layer.py (Transform data - part 2 - aggregated view)
  - export_to_bigquery.py (Export part 1 data to BigQuery)
  - export_analytics_to_bigquery.py (Export part 2 data to BigQuery)
 - montreal_cyclists.drawio (Data model diagram)
 - virtual machine installations.txt (Commands to use in the Google VM instance)

For more information on this project, please check out the following Medium article: https://medium.com/@patrick.ml.walsh/mage-bigquery-and-bundled-up-bike-trips-672c041f808a

For questions or ideas on this project and its Mage AI, Google Cloud, Python, or Power BI components, feel free to reach out at patrick.ml.walsh@gmail.com. Thanks!

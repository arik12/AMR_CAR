README - Navigating Global Uncertainty

This folder contains the scripts and code used for generating and preparing the dataset, as well as creating figures. Below is an overview of each step and the corresponding files:

Generate the Main Data Files
Compustat: Run the Stata script (Data Cleanup Compustat.do file) to generate the primary data from Compustat.
BoardEx: Use Boardex CAR.py to process BoardEx data.
FactSet: Use FACTSET CAR.py to process FactSet data.
EPU: Use EPU CAR.py to include Economic Policy Uncertainty data.

Merge the Data Files
Use Merge_factset_boardex_epu_compustat_CAR.py to combine all datasets into a single comprehensive file.
Final Adjustments and Calculations

Use FInal Adjustment CAR.do (a Stata script) to perform the final data cleaning and calculations.
Figure Generation

Use Trade Country and EPU Shocks CAR.py to generate the figures and visualizations from the cleaned and merged dataset.

Each file is designed to work in sequence, so please follow the steps above in order. If you encounter any issues or have further questions, feel free to reach out to the repository’s maintainer.

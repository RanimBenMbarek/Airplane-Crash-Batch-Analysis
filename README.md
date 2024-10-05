# Airplane Crash Data Analysis & Visualization

This repository presents a pipeline for analyzing and visualizing airplane crash data. Using Apache Spark for batch processing and HBase for data storage, this project aims to uncover insights from historical airplane crash incidents.

## About the Dataset

The **Airplane Crashes and Fatalities Since 1908** dataset provides a comprehensive history of airplane crashes worldwide from 1908 to 2009. It includes data on civil, commercial, and military transport accidents. The dataset contains important fields such as the year of the crash, the number of people on board, survivors, fatalities, and a summary detailing the crash circumstances. Although the original dataset was hosted by Open Data by Socrata, it is no longer available at that link.

This dataset serves as a valuable resource for analyzing trends in airplane safety and the factors contributing to crashes.

## Dataset Link
You can download the dataset from Kaggle:
- [Airplane Crash Data on Kaggle](https://www.kaggle.com/datasets/saurograndi/airplane-crashes-since-1908/data?fbclid=IwY2xjawFuBw5leHRuA2FlbQIxMAABHT-1JuJhduEwm-uBDjrOdXKmRGHEN1m9ny4CNriPmyB_0aXjVbv6cQ6PvQ_aem_4nKJwPKDKvEQoti0J03J-g)

## Project Overview

1. **Data Ingestion**:
   - Load the CSV dataset into Spark for processing.

2. **Data Storage**:
   - Store processed data in HBase for efficient querying.

3. **Data Analysis**:
   - Analyze crash data to answer key questions, such as:
     - Yearly number of plane crashes.
     - Number of accidents by flight type.

4. **Data Visualization**:
   - Visualize insights through graphs and charts, illustrating trends and patterns in airplane crashes.

## Architecture

![](![image](https://github.com/user-attachments/assets/826f51ac-68e3-4fe2-9413-7f016782f38a)

## Sample Visualizations

Here are some sample visualizations produced from the analysis:

1. **Yearly Plane Crashes**:
   ![Yearly Plane Crashes](![image](https://github.com/user-attachments/assets/17ad3cb1-0402-48d3-8af4-7b3f533ae8c0)

2. **Fatalities by Aircraft Type**:
   ![Fatalities by flight Type](![image](https://github.com/user-attachments/assets/9c6ecc21-0088-422e-b14f-7831ed8f5943)


---

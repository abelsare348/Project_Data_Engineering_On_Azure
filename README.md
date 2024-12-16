# Azure Healthcare Data Engineering Project

## Project Overview

This project demonstrates a complete **data engineering pipeline** for a healthcare organization using **Azure cloud technologies**. The project processes and stores healthcare data, including patient records, doctor assignments, and ICU device metrics. The architecture leverages **Azure Data Factory** for data integration, **Azure Databricks** for data transformation (ETL), **Azure Synapse Analytics** for data storage and analytics, and **Azure Event Hub** for real-time data processing.

## Architecture Diagram

![Architecture Diagram](ArchitecturalDiagram/FinalprojectEngineeringOnAzure.drawio.png)

## Technologies Used

- **Azure Data Factory**: For data integration and orchestration.
- **Azure Databricks**: For ETL (Extract, Transform, Load) data transformation.
- **Azure Synapse Analytics**: For ELT (Extract, Load, Transform) with SQL pools and Spark pools.
- **Azure Event Hub**: For ingesting real-time streaming data from IoT devices in ICU wards.
- **Azure Data Lake**: To store raw data in a scalable manner.
- **Azure Logic Apps**: To build custom notification alerts without programming.
- **Azure Functions**: To build custom notification alerts with code.
- **Azure Active Directory**: For identity and access management.
- **Azure Key Vault**: For securely storing secrets and credentials.
- **Azure Synapse Analytics job**: For consuming the data that was stored at streaming rate like a batch.
- **Azure Purview**: For Data Governance,Data lineage and Data Security.

## Data Flow Steps

1. **Data Ingestion**: Data is ingested using **Azure Event Hub** for real-time streams (e.g., ICU device metrics) and **Azure Data Factory** for batch processing (e.g., patient records, doctor data).
2. **Data Storage**: Raw data is stored in **Azure Data Lake**.
3. **Data Transformation**: **Azure Databricks** processes and transforms the raw data for ETL pipelines, preparing it for analytics.
4. **Data Storage in Synapse**: Processed data is loaded into **Azure Synapse Analytics** for analytics using both SQL pools and Spark pools for large-scale analysis.
5. **Notification Systems**: Custom notifications are sent via **Azure Logic Apps** and **Azure Functions** when certain thresholds are reached (e.g., ICU device failures).


## Setup Instructions

1. Clone the repository:
    ```bash
    git clone https://github.com/abelsare348/Project_Data_Engineering_On_Azure.git
    ```

2. Set up Azure resources:
   - Create an **Azure Data Lake** storage account.
   - Create an **Azure Event Hub** for real-time data ingestion.
   - Create an **Azure Data Factory** instance for orchestration.
   - Set up **Azure Synapse Analytics** for storage and analytics.
   - Deploy **Azure Logic Apps** and **Azure Functions** for notifications.
   - Set up **Azure Purview** for Data Governance.

3. Upload the provided datasets into **Azure Data Lake** storage.

4. Run the **Azure Data Factory** pipelines to start the data flow.

5. Analyze the data in **Azure Synapse Analytics** using SQL pools or Spark pools.


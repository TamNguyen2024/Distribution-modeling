# Data Modeling - Distribution Model

# OBJECTIVE:

This is the combination between 3NF and Entity Centric Data Modeling (ECM) technique to organize data into the data warehouse. This modeling technique 
is an normalization step, helps us have a clear conceptual knowlegde about the business data before converting to Star Schema. 

# DESCRIPTION
Although this technique is not Star Schema, but I use dim table to describe the entity table and fact table to describe the transaction table. 
Moreover, I use brigde table to store the relationship between Vendor, Customer, and Route as these 3 dim tables is SCD type 2 (Slowly Changing Dimension).
All tables in folder **Customer_Performance_Snapshot** is dimensional snapshot (Entity Centric Data Modeling), it used to assess the performance of customer 
with Time-series or Point-in-time analysis.

1. Dim table : dim_customer (SCD type2), dim_vendor (SCD type 2), dim_route (SCD type 2), dim_sales_rep (SCD type 2), dim_tsm (SCD type 2), dim_asm (SCD type 2), dim_ranking, dim_channel,...

2. Fact table: Visit plan (factless fact), Sales_settled (transactional fact), Sales_settled_cumulative (cumulative fact)

3. Bridge table : Relationship, Relationship_Customer_Route, Relationship_Customer_Vendor

4. Daily Dimensional Snapshot: Daily_Customer_Performance

# Technology Used:
Data Modeling: 3NF, Entity Centric Data Modeling, Star Schema   

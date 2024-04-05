# e-commerce-data-pipeline
## Project Overview
This is an advanced data pipeline for a large e-commerce platform. The goal is to build a scalable, fault-tolerant data pipeline that efficiently processes the provided data for analytics purposes.

## Dataset Description
The provided dataset consists of three types of data from two distinct markets:

### Customer Data - Market 1 & 2 (customers.json)
Contains information about user transactions, including Customer ID, Last Used Platform, Is Blocked, Created At, Language, Outstanding Amount, Loyalty Points, Number of employees

### Orders Data - Market 1 & 2 (order.csv)
Contains orders data, including Order ID, Order Status, Category Name, SKU, Customization Group, Customization Option, Quantity, Unit Price, Cost Price, Total Cost Price, Total Price, Order Total, Sub Total, Tax, Delivery, Charge, Tip, Discount, Remaining Balance, Payment Method, Additional Charge, Taxable Amount, Transaction ID, Currency Symbol, Transaction Status, Promo Code, Customer ID, Merchant ID, Description, Distance (in km), Order Time, Pickup Time, Delivery Time, Ratings, Reviews, Merchant Earning, Commission Amount, Commission Payout Status, Order Preparation Time, Debt Amount, Redeemed Loyalty Points, Consumed Loyalty Points, Cancellation Reason, Flat Discount, Checkout Template Name, Checkout Template Value

### Deliveries Data - Market 1 & 2 (deliveries.csv)
Contains deliveries data, including Task_ID, Order_ID, Relationship, Team_Name, Task_Type, Notes, Agent_ID, Agent_Name, Distance(m), Total_Time_Taken(min), Task_Status, Ref_Images, Rating, Review, Latitude, Longitude, Tags, Promo_Applied, Custom_Template_ID, Task_Details_QTY, Task_Details_AMOUNT, Special_Instructions, Tip, Delivery_Charges, Discount, Subtotal, Payment_Type, Task_Category, Earning, Pricing